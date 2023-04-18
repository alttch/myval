#[cfg(feature = "arrow2_ih")]
extern crate arrow2_ih as arrow2;

use crate::{Error, Time, TimeZone};
use arrow2::array::{Array, Int64Array, PrimitiveArray, Utf8Array};
pub use arrow2::chunk::Chunk;
use arrow2::datatypes::Field;
pub use arrow2::datatypes::{DataType, Metadata, Schema, TimeUnit};
use arrow2::error::Error as ArrowError;
use arrow2::io::ipc::read::{StreamReader, StreamState};
use arrow2::io::ipc::write::{StreamWriter, WriteOptions};
use arrow2::types::NativeType;
use chrono::{DateTime, Local, NaiveDateTime, SecondsFormat, Utc};
use std::ops::{Add, Div, Mul, Sub};
use std::str::FromStr;

/// Series type, alias for boxed arrow2 array
///
/// The series can contain a single array only. If more arrays required in a column, consider
/// creating a new dataframe
pub type Series = Box<(dyn Array + 'static)>;

/// Base data frame class
#[derive(Default, Clone)]
pub struct DataFrame {
    fields: Vec<Field>,
    data: Vec<Series>,
    metadata: Metadata,
}

impl DataFrame {
    /// Create a new data frame with no columns
    #[inline]
    pub fn new0() -> Self {
        Self::new(None)
    }
    /// Create a new data frame and allocate columns
    #[inline]
    pub fn new(cols: Option<usize>) -> Self {
        Self {
            data: Vec::with_capacity(cols.unwrap_or_default()),
            fields: Vec::with_capacity(cols.unwrap_or_default()),
            metadata: <_>::default(),
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
        let mut df = Self::new(cols.map(|c| c + 1));
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
        df.add_series(
            "time",
            ts,
            Some(DataType::Timestamp(time_unit, tz.into())),
            None,
        )
        .unwrap();
        df
    }
    /// Create a new time-series data frame from f64 timestamps and convert them to rfc3339 strings
    ///
    /// # Panics
    ///
    /// should not panic
    pub fn new_timeseries_from_float_rfc3339(time_series: Vec<f64>, cols: Option<usize>) -> Self {
        let mut df = Self::new(cols.map(|c| c + 1));
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
        Self {
            fields: schema.fields.clone(),
            data,
            metadata: schema.metadata.clone(),
        }
    }
    /// Create a data frame from vector of fields and vector of series
    pub fn from_parts(
        fields: Vec<Field>,
        data: Vec<Series>,
        metadata: Option<Metadata>,
    ) -> Result<Self, Error> {
        if let Some(x) = data.first() {
            let rows = x.len();
            for s in data.iter().skip(1) {
                if s.len() != rows {
                    return Err(Error::RowsNotMatch);
                }
            }
        }
        Ok(Self {
            fields,
            data,
            metadata: metadata.unwrap_or_default(),
        })
    }
    /// Split the data frame into vector of fields, vector of series and metadata
    pub fn into_parts(self) -> (Vec<Field>, Vec<Series>, Metadata) {
        (self.fields, self.data, self.metadata)
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    /// metadata
    #[inline]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    /// metadata mutable
    #[inline]
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
    /// set metadata
    #[inline]
    pub fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// set metadata field
    #[inline]
    pub fn set_metadata_field(&mut self, metadata_field: &str, value: &str) {
        self.metadata
            .insert(metadata_field.to_owned(), value.to_owned());
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
        data_type: Option<DataType>,
        metadata: Option<Metadata>,
    ) -> Result<(), Error> {
        if self.data.is_empty() || series.len() == self.data[0].len() {
            let mut field = Field::new(name, data_type.unwrap_or(series.data_type().clone()), true);
            if let Some(meta) = metadata {
                field = field.with_metadata(meta);
            }
            self.fields.push(field);
            self.data.push(series);
            Ok(())
        } else {
            Err(Error::RowsNotMatch)
        }
    }
    /// Add series to the data frame as a new column and use the same type as the series
    #[inline]
    pub fn add_series0(&mut self, name: &str, series: Series) -> Result<(), Error> {
        self.add_series(name, series, None, None)
    }
    /// Insert series to the data frame as a new column and specify its type
    pub fn insert_series(
        &mut self,
        name: &str,
        series: Series,
        index: usize,
        data_type: Option<DataType>,
        metadata: Option<Metadata>,
    ) -> Result<(), Error> {
        if index <= self.data.len() {
            if self.data.is_empty() || series.len() == self.data[0].len() {
                let mut field =
                    Field::new(name, data_type.unwrap_or(series.data_type().clone()), true);
                if let Some(meta) = metadata {
                    field = field.with_metadata(meta);
                }
                self.fields.insert(index, field);
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
        self.insert_series(name, series, index, None, None)
    }
    /// Create a vector of sliced series
    pub fn try_series_sliced(&self, offset: usize, length: usize) -> Result<Vec<Series>, Error> {
        if self.data.is_empty() {
            Ok(vec![])
        } else if offset + length <= self.data[0].len() {
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
        if self.data.is_empty() {
            Ok(Self::new0())
        } else if offset + length <= self.data[0].len() {
            Ok(Self {
                data: self.data.iter().map(|d| d.sliced(offset, length)).collect(),
                fields: self.fields.clone(),
                metadata: self.metadata.clone(),
            })
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Generate schema object
    #[inline]
    pub fn schema(&self) -> Schema {
        Schema::from(self.fields.clone()).with_metadata(self.metadata.clone())
    }
    #[inline]
    pub fn rows(&self) -> Option<usize> {
        self.data.first().map(|v| v.len())
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
    /// Get column index
    #[inline]
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|v| v.name == name)
    }
    /// Set column ordering
    pub fn set_ordering(&mut self, names: &[&str]) {
        for (i, name) in names.iter().enumerate() {
            if let Some(pos) = self.get_column_index(name) {
                if pos != i {
                    self.fields.swap(i, pos);
                    self.data.swap(i, pos);
                }
            }
        }
    }
    /// Sort columns alphabetically
    pub fn sort_columns(&mut self) {
        let mut names = self
            .fields
            .iter()
            .map(|v| v.name.clone())
            .collect::<Vec<String>>();
        names.sort();
        self.set_ordering(&names.iter().map(String::as_str).collect::<Vec<&str>>());
    }
    /// Convert into IPC parts: schema + chunk
    pub fn into_ipc_parts(self) -> (Schema, Chunk<Box<dyn Array + 'static>>) {
        let schema = Schema::from(self.fields).with_metadata(self.metadata);
        let chunk = Chunk::new(self.data);
        (schema, chunk)
    }
    /// Convert into IPC ready-to-send block
    pub fn into_ipc_block(self) -> Result<Vec<u8>, ArrowError> {
        let mut buf = Vec::new();
        let schema = Schema::from(self.fields).with_metadata(self.metadata);
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
        let metadata = reader.metadata().schema.metadata.clone();
        for state in reader {
            match state? {
                StreamState::Waiting => continue,
                StreamState::Some(chunk) => {
                    let data = chunk.into_arrays();
                    return Ok(Self {
                        fields,
                        data,
                        metadata,
                    });
                }
            }
        }
        let mut df = DataFrame::new0();
        df.metadata = metadata;
        Ok(df)
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
    /// Rename column
    pub fn rename(&mut self, name: &str, new_name: &str) -> Result<(), Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            field.name = new_name.to_owned();
            Ok(())
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    /// Parse string column values
    pub fn parse<T>(&mut self, name: &str) -> Result<(), Error>
    where
        T: NativeType + FromStr,
    {
        if let Some(pos) = self.get_column_index(name) {
            self.parse_at::<T>(pos)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn parse_at<T>(&mut self, index: usize) -> Result<(), Error>
    where
        T: NativeType + FromStr,
    {
        if let Some(series) = self.data.get(index) {
            let values: &Utf8Array<i64> =
                series.as_any().downcast_ref().ok_or(Error::TypeMismatch)?;
            let mut dt: Vec<Option<_>> = Vec::with_capacity(values.len());
            for val in values {
                dt.push(if let Some(s) = val {
                    s.parse::<T>().ok()
                } else {
                    None
                });
            }
            let arr = PrimitiveArray::<T>::from(dt);
            let dtype = arr.data_type().clone();
            self.data[index] = arr.boxed();
            self.fields[index].data_type = dtype;
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    /// Set field name by index
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
    /// Override field meta data
    pub fn set_col_metadata(&mut self, name: &str, metadata: Metadata) -> Result<(), Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            field.metadata = metadata;
            Ok(())
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    /// Override field meta data by index
    pub fn set_col_metadata_at(&mut self, index: usize, metadata: Metadata) -> Result<(), Error> {
        if let Some(field) = self.fields.get_mut(index) {
            field.metadata = metadata;
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn col_metadata(&self, name: &str) -> Result<&Metadata, Error> {
        if let Some(field) = self.fields.iter().find(|field| field.name == name) {
            Ok(&field.metadata)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn col_metadata_mut(&mut self, name: &str) -> Result<&mut Metadata, Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            Ok(&mut field.metadata)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn col_metadata_at(&self, index: usize) -> Result<&Metadata, Error> {
        if let Some(field) = self.fields.get(index) {
            Ok(&field.metadata)
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn col_metadata_mut_at(&mut self, index: usize) -> Result<&mut Metadata, Error> {
        if let Some(field) = self.fields.get_mut(index) {
            Ok(&mut field.metadata)
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn set_col_metadata_field(
        &mut self,
        name: &str,
        metadata_field: &str,
        value: &str,
    ) -> Result<(), Error> {
        if let Some(field) = self.fields.iter_mut().find(|field| field.name == name) {
            field
                .metadata
                .insert(metadata_field.to_owned(), value.to_owned());
            Ok(())
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn set_col_metadata_field_at(
        &mut self,
        index: usize,
        metadata_field: &str,
        value: &str,
    ) -> Result<(), Error> {
        if let Some(field) = self.fields.get_mut(index) {
            field
                .metadata
                .insert(metadata_field.to_owned(), value.to_owned());
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn add<T>(&mut self, name: &str, value: T) -> Result<(), Error>
    where
        T: NativeType + Add,
        Vec<Option<<T as Add>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(pos) = self.get_column_index(name) {
            self.add_at(pos, value)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn add_at<T>(&mut self, index: usize, value: T) -> Result<(), Error>
    where
        T: NativeType + Add,
        Vec<Option<<T as Add>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(series) = self.data.get(index) {
            let values: &PrimitiveArray<T> =
                series.as_any().downcast_ref().ok_or(Error::TypeMismatch)?;
            let dt: Vec<Option<_>> = values.into_iter().map(|v| v.map(|n| *n + value)).collect();
            self.data[index] = PrimitiveArray::<T>::from(dt).boxed();
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn sub<T>(&mut self, name: &str, value: T) -> Result<(), Error>
    where
        T: NativeType + Sub,
        Vec<Option<<T as Sub>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(pos) = self.get_column_index(name) {
            self.sub_at(pos, value)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn sub_at<T>(&mut self, index: usize, value: T) -> Result<(), Error>
    where
        T: NativeType + Sub,
        Vec<Option<<T as Sub>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(series) = self.data.get(index) {
            let values: &PrimitiveArray<T> =
                series.as_any().downcast_ref().ok_or(Error::TypeMismatch)?;
            let dt: Vec<Option<_>> = values.into_iter().map(|v| v.map(|n| *n - value)).collect();
            self.data[index] = PrimitiveArray::<T>::from(dt).boxed();
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn mul<T>(&mut self, name: &str, value: T) -> Result<(), Error>
    where
        T: NativeType + Mul,
        Vec<Option<<T as Mul>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(pos) = self.get_column_index(name) {
            self.mul_at(pos, value)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn mul_at<T>(&mut self, index: usize, value: T) -> Result<(), Error>
    where
        T: NativeType + Mul,
        Vec<Option<<T as Mul>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(series) = self.data.get(index) {
            let values: &PrimitiveArray<T> =
                series.as_any().downcast_ref().ok_or(Error::TypeMismatch)?;
            let dt: Vec<Option<_>> = values.into_iter().map(|v| v.map(|n| *n * value)).collect();
            self.data[index] = PrimitiveArray::<T>::from(dt).boxed();
            Ok(())
        } else {
            Err(Error::OutOfBounds)
        }
    }
    pub fn div<T>(&mut self, name: &str, value: T) -> Result<(), Error>
    where
        T: NativeType + Div,
        Vec<Option<<T as Div>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(pos) = self.get_column_index(name) {
            self.div_at(pos, value)
        } else {
            Err(Error::NotFound(name.to_owned()))
        }
    }
    pub fn div_at<T>(&mut self, index: usize, value: T) -> Result<(), Error>
    where
        T: NativeType + Div,
        Vec<Option<<T as Div>::Output>>: AsRef<[Option<T>]>,
    {
        if let Some(series) = self.data.get(index) {
            let values: &PrimitiveArray<T> =
                series.as_any().downcast_ref().ok_or(Error::TypeMismatch)?;
            let dt: Vec<Option<_>> = values.into_iter().map(|v| v.map(|n| *n / value)).collect();
            self.data[index] = PrimitiveArray::<T>::from(dt).boxed();
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
    // The conversion is done in an unsafe way and has got the following (or more) limitations:
    //
    // If a data frame field has got data type Timestamp with TimeUnit::Second, the time is
    // converted incorrectly (Polars has no TimeUnit::Second support)
    //
    // If a data frame contains Utf8<i32> (Utf8 data type), the conversion may crash the program.
    // The solution for now is to avoid using Utf8 and use LargeUtf8 instead.
    fn from(df: DataFrame) -> polars::frame::DataFrame {
        let (fields, data, _) = df.into_parts();
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

#[cfg(feature = "polars")]
impl From<polars::frame::DataFrame> for DataFrame {
    fn from(mut polars_df: polars::frame::DataFrame) -> DataFrame {
        match polars_df.n_chunks() {
            0 => return DataFrame::new0(),
            2.. => polars_df = polars_df.agg_chunks(),
            _ => {}
        }
        let pl_series: Vec<polars::series::Series> = polars_df.into();
        let names: Vec<String> = pl_series.iter().map(|s| s.name().to_owned()).collect();
        let series: Vec<Series> = pl_series.into_iter().map(|v| v.to_arrow(0)).collect();
        let mut df = DataFrame::new(Some(series.len()));
        for (s, name) in series.into_iter().zip(names) {
            df.add_series0(&name, s).unwrap();
        }
        df
    }
}
