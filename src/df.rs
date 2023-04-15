#[cfg(feature = "arrow2_ih")]
extern crate arrow2_ih as arrow2;

use crate::{Error, Time, TimeZone};
use arrow2::array::{Array, Int64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow2::error::Error as ArrowError;
use arrow2::io::ipc::read::{StreamReader, StreamState};
use arrow2::io::ipc::write::{StreamWriter, WriteOptions};
use chrono::{DateTime, Local, NaiveDateTime, SecondsFormat, Utc};

/// Series type, alias for boxed arrow2 array
///
/// The series can contain a single array only. If more arrays required in a column, consider
/// creating a new dataframe
pub type Series = Box<(dyn Array + 'static)>;

/// Base data frame class
///
/// The data frame can be automatically converted into:
///
/// IPC chunk (Chunk::from)
/// Ready-to-send IPC block (Vec<u8>::from)
/// Polars data frame (polars::frame::DateFrame::from, "polars" feature required)
#[derive(Default, Clone)]
pub struct DataFrame {
    fields: Vec<Field>,
    data: Vec<Series>,
    rows: usize,
}

impl DataFrame {
    /// Create a new data frame with fixed number of rows and no columns
    #[inline]
    pub fn new0(rows: usize) -> Self {
        Self::new(rows, None)
    }
    /// Create a new data frame with fixed number of rows and allocate columns
    #[inline]
    pub fn new(rows: usize, cols: Option<usize>) -> Self {
        Self {
            data: Vec::with_capacity(cols.unwrap_or_default()),
            rows,
            fields: Vec::with_capacity(cols.unwrap_or_default()),
        }
    }
    /// Create a new time-series data frame from f64 timestamps
    ///
    /// # Panics
    ///
    /// should not panic
    pub fn new_timeseries_from_float(
        time_series: Vec<f64>,
        cols: Option<usize>,
        tz: TimeZone,
        time_unit: TimeUnit,
    ) -> Self {
        let mut df = Self::new(time_series.len(), cols.map(|c| c + 1));
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        let ts = Int64Array::from(
            time_series
                .into_iter()
                .map(|v| {
                    Some({
                        match time_unit {
                            TimeUnit::Second => v.trunc() as i64,
                            TimeUnit::Millisecond => {
                                let t = Time::from_timestamp(v);
                                t.timestamp_ms() as i64
                            }
                            TimeUnit::Microsecond => {
                                let t = Time::from_timestamp(v);
                                t.timestamp_us() as i64
                            }
                            TimeUnit::Nanosecond => {
                                let t = Time::from_timestamp(v);
                                t.timestamp_ns() as i64
                            }
                        }
                    })
                })
                .collect::<Vec<Option<i64>>>(),
        )
        .boxed();
        df.add_series("time", ts, DataType::Timestamp(time_unit, tz.into()))
            .unwrap();
        df
    }
    /// Create a new time-series data frame from f64 timestamps and convert them to rfc3339 strings
    ///
    /// # Panics
    ///
    /// should not panic
    pub fn new_timeseries_from_float_rfc3339(time_series: Vec<f64>, cols: Option<usize>) -> Self {
        let mut df = Self::new(time_series.len(), cols.map(|c| c + 1));
        let ts: Vec<Option<String>> = time_series
            .iter()
            .map(|v| {
                #[allow(clippy::cast_possible_truncation)]
                #[allow(clippy::cast_sign_loss)]
                let dt_utc = DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp_opt(
                        v.trunc() as i64,
                        (v.fract() * 1_000_000_000.0) as u32,
                    )
                    .unwrap_or_default(),
                    Utc,
                );
                let dt: DateTime<Local> = DateTime::from(dt_utc);
                Some(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
            })
            .collect();
        df.add_series0("time", Utf8Array::<i32>::from(ts).boxed())
            .unwrap();
        df
    }
    /// Create a data frame from IPC chunk and schema
    pub fn from_chunk(chunk: Chunk<Box<dyn Array + 'static>>, schema: &Schema) -> Self {
        let data = chunk.into_arrays();
        let rows = data.first().map_or(0, |v| v.len());
        Self {
            fields: schema.fields.clone(),
            data,
            rows,
        }
    }
    /// Create a data frame from vector of fields and vector of series
    pub fn from_parts(fields: Vec<Field>, data: Vec<Series>) -> Result<Self, Error> {
        let rows = if let Some(x) = data.first() {
            let rows = x.len();
            for s in data.iter().skip(1) {
                if s.len() != rows {
                    return Err(Error::RowsNotMatch);
                }
            }
            rows
        } else {
            0
        };
        Ok(Self { fields, data, rows })
    }
    /// Split the data frame into vector of fields and vector of series
    pub fn into_parts(self) -> (Vec<Field>, Vec<Series>) {
        (self.fields, self.data)
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    /// Column names
    #[inline]
    pub fn names(&self) -> Vec<&str> {
        self.fields.iter().map(|col| col.name.as_str()).collect()
    }
    /// Column field objects
    #[inline]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
    /// Columns (data)
    #[inline]
    pub fn data(&self) -> &[Series] {
        &self.data
    }
    /// Add series to the data frame as a new column and specify its type
    pub fn add_series(
        &mut self,
        name: &str,
        series: Series,
        data_type: DataType,
    ) -> Result<(), Error> {
        if series.len() == self.rows {
            self.fields.push(Field::new(name, data_type, true));
            self.data.push(series);
            Ok(())
        } else {
            Err(Error::RowsNotMatch)
        }
    }
    /// Add series to the data frame as a new column and use the same type as the series
    #[inline]
    pub fn add_series0(&mut self, name: &str, series: Series) -> Result<(), Error> {
        let dt = series.data_type().clone();
        self.add_series(name, series, dt)
    }
    /// Insert series to the data frame as a new column and specify its type
    pub fn insert_series(
        &mut self,
        name: &str,
        series: Series,
        index: usize,
        data_type: DataType,
    ) -> Result<(), Error> {
        if index <= self.data.len() {
            if series.len() == self.rows {
                self.fields.insert(index, Field::new(name, data_type, true));
                self.data.insert(index, series);
                Ok(())
            } else {
                Err(Error::RowsNotMatch)
            }
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Add series to the data frame as a new column and use the same type as the series
    #[inline]
    pub fn insert_series0(
        &mut self,
        name: &str,
        series: Series,
        index: usize,
    ) -> Result<(), Error> {
        let dt = series.data_type().clone();
        self.insert_series(name, series, index, dt)
    }
    /// Create a vector of sliced series
    pub fn try_series_sliced(&self, offset: usize, length: usize) -> Result<Vec<Series>, Error> {
        if offset + length <= self.rows {
            Ok(self.data.iter().map(|d| d.sliced(offset, length)).collect())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Create IPC chunk of sliced series
    #[inline]
    pub fn try_chunk_sliced(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<Chunk<Box<dyn Array>>, Error> {
        let series = self.try_series_sliced(offset, length)?;
        Ok(Chunk::new(series))
    }
    /// Create a new data frame of sliced series
    pub fn try_sliced(&self, offset: usize, length: usize) -> Result<Self, Error> {
        if offset + length <= self.rows {
            Ok(Self {
                data: self.data.iter().map(|d| d.sliced(offset, length)).collect(),
                rows: length,
                fields: self.fields.clone(),
            })
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Generate schema object
    #[inline]
    pub fn schema(&self) -> Schema {
        Schema::from(self.fields.clone())
    }
    #[inline]
    pub fn rows(&self) -> usize {
        self.rows
    }
    /// calculate approx data frame size
    ///
    /// (does not work properly for strings)
    pub fn size(&self) -> usize {
        let mut size = 0;
        for d in &self.data {
            let m = match d.data_type() {
                DataType::Boolean => 1,
                DataType::Int16 => 2,
                DataType::Int32 | DataType::Float32 => 4,
                _ => 8,
            };
            size += d.len() * m;
        }
        size
    }
    /// Set column ordering
    pub fn set_ordering(&mut self, cols: &[&str]) {
        for (i, col) in cols.iter().enumerate() {
            if let Some(pos) = self.fields.iter().position(|r| &r.name == col) {
                if pos != i {
                    self.fields.swap(i, pos);
                    self.data.swap(i, pos);
                }
            }
        }
    }
    /// Convert into IPC parts: schema + chunk
    pub fn into_ipc_parts(self) -> (Schema, Chunk<Box<dyn Array + 'static>>) {
        let schema = Schema::from(self.fields);
        let chunk = Chunk::new(self.data);
        (schema, chunk)
    }
    /// Convert into IPC ready-to-send block
    pub fn into_ipc_block(self) -> Result<Vec<u8>, ArrowError> {
        let mut buf = Vec::new();
        let schema = self.schema();
        let chunk = Chunk::new(self.data);
        let mut writer = StreamWriter::new(&mut buf, WriteOptions::default());
        writer.start(&schema, None)?;
        writer.write(&chunk, None)?;
        writer.finish()?;
        Ok(buf)
    }
    /// Create a data frame from a complete IPC block
    pub fn from_ipc_block(block: &[u8]) -> Result<Self, ArrowError> {
        let mut buf = std::io::Cursor::new(block);
        let meta = arrow2::io::ipc::read::read_stream_metadata(&mut buf)?;
        let reader = StreamReader::new(buf, meta, None);
        let fields = reader.metadata().schema.fields.clone();
        for state in reader {
            match state? {
                StreamState::Waiting => continue,
                StreamState::Some(chunk) => {
                    let data = chunk.into_arrays();
                    let rows = data.first().map_or(0, |v| v.len());
                    return Ok(Self { fields, data, rows });
                }
            }
        }
        Ok(DataFrame::new0(0))
    }
    /// Pop series by name
    pub fn pop_series(&mut self, name: &str) -> Result<(Series, DataType), Error> {
        if let Some((pos, _)) = self
            .fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == name)
        {
            let field = self.fields.remove(pos);
            Ok((self.data.remove(pos), field.data_type))
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    /// Pop series by index
    pub fn pop_series_at(&mut self, index: usize) -> Result<(Series, String, DataType), Error> {
        if index < self.fields.len() {
            let field = self.fields.remove(index);
            Ok((self.data.remove(index), field.name, field.data_type))
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Override field name
    pub fn set_name(&mut self, name: &str, new_name: &str) -> Result<(), Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            field.name = new_name.to_owned();
            Ok(())
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    /// Override field data type by index
    pub fn set_name_at(&mut self, index: usize, new_name: &str) -> Result<(), Error> {
        if let Some(field) = self.fields.get_mut(index) {
            field.name = new_name.to_owned();
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Override field data type
    pub fn set_data_type(&mut self, name: &str, data_type: DataType) -> Result<(), Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            field.data_type = data_type;
            Ok(())
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    /// Override field data type by index
    pub fn set_data_type_at(&mut self, index: usize, data_type: DataType) -> Result<(), Error> {
        if let Some(field) = self.fields.get_mut(index) {
            field.data_type = data_type;
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
}

impl From<DataFrame> for Chunk<Box<dyn Array>> {
    #[inline]
    fn from(df: DataFrame) -> Self {
        Chunk::new(df.data)
    }
}

impl TryFrom<DataFrame> for Vec<u8> {
    type Error = ArrowError;
    #[inline]
    fn try_from(df: DataFrame) -> Result<Self, Self::Error> {
        df.into_ipc_block()
    }
}

#[cfg(feature = "polars")]
impl From<DataFrame> for polars::frame::DataFrame {
    fn from(df: DataFrame) -> polars::frame::DataFrame {
        let (fields, data) = df.into_parts();
        let polars_series = unsafe {
            data.into_iter()
                .zip(fields)
                .map(|(d, f)| {
                    polars::series::Series::from_chunks_and_dtype_unchecked(
                        &f.name,
                        vec![d],
                        &f.data_type().into(),
                    )
                })
                .collect::<Vec<polars::series::Series>>()
        };
        polars::frame::DataFrame::new_no_checks(polars_series)
    }
}
