use crate::df::{DataFrame, Series};
use crate::Error;
use arrow2::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Utf8Array,
};
use arrow2::datatypes::{DataType, TimeUnit};
use async_stream::try_stream;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::{Column, PgPool, Postgres, Row, TypeInfo};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::pin::Pin;
use std::sync::Arc;

enum Data {
    Bool(Vec<Option<bool>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Timestamp(Vec<Option<i64>>),
    TimestampTz(Vec<Option<i64>>),
    Char(Vec<Option<String>>),
    Json(Vec<Option<String>>),
}

struct Col {
    index: usize,
    data: Data,
    size: usize,
}

impl Col {
    fn create(index: usize, type_id: &str) -> Result<Self, Error> {
        let data = match type_id {
            "BOOL" => Data::Bool(<_>::default()),
            "INT2" => Data::Int16(<_>::default()),
            "INT4" => Data::Int32(<_>::default()),
            "INT8" => Data::Int64(<_>::default()),
            "TIMESTAMP" => Data::Timestamp(<_>::default()),
            "TIMESTAMPTZ" => Data::TimestampTz(<_>::default()),
            "FLOAT4" => Data::Float32(<_>::default()),
            "FLOAT8" => Data::Float64(<_>::default()),
            "VARCHAR" | "CHAR" => Data::Char(<_>::default()),
            "JSON" | "JSONB" => Data::Json(<_>::default()),
            v => return Err(Error::Unimplemented(v.to_owned())),
        };
        Ok(Self {
            index,
            data,
            size: 0,
        })
    }
    #[allow(dead_code)]
    fn len(&self) -> usize {
        match &self.data {
            Data::Bool(v) => v.len(),
            Data::Int16(v) => v.len(),
            Data::Int32(v) => v.len(),
            Data::Int64(v) | Data::Timestamp(v) | Data::TimestampTz(v) => v.len(),
            Data::Float32(v) => v.len(),
            Data::Float64(v) => v.len(),
            Data::Char(v) | Data::Json(v) => v.len(),
        }
    }
    fn size(&self) -> usize {
        self.size
    }
    fn push(&mut self, row: &PgRow) -> Result<(), sqlx::Error> {
        match self.data {
            Data::Bool(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 1;
            }
            Data::Int16(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 2;
            }
            Data::Int32(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 4;
            }
            Data::Int64(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 8;
            }
            Data::Float32(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 4;
            }
            Data::Float64(ref mut v) => {
                v.push(row.try_get(self.index)?);
                self.size += 8;
            }
            Data::Timestamp(ref mut v) => {
                let t: Option<NaiveDateTime> = row.try_get(self.index)?;
                v.push(t.map(|x| x.timestamp_nanos()));
                self.size += 8;
            }
            Data::TimestampTz(ref mut v) => {
                let t: Option<DateTime<Utc>> = row.try_get(self.index)?;
                v.push(t.map(|x| x.timestamp_nanos()));
                self.size += 8;
            }
            Data::Char(ref mut v) => {
                let s: Option<String> = row.try_get(self.index)?;
                let len = s.as_ref().map_or(1, String::len);
                v.push(s);
                self.size += len;
            }
            Data::Json(ref mut v) => {
                let val: Option<Value> = row.try_get(self.index)?;
                if let Some(d) = val {
                    let s = serde_json::to_string(&d).ok();
                    let len = s.as_ref().map_or(1, String::len);
                    v.push(s);
                    self.size += len;
                } else {
                    v.push(None);
                    self.size += 1;
                }
            }
        }
        Ok(())
    }
    fn into_series_type(self) -> (Series, DataType) {
        match self.data {
            Data::Bool(v) => (BooleanArray::from(v).boxed(), DataType::Boolean),
            Data::Int16(v) => (Int16Array::from(v).boxed(), DataType::Int16),
            Data::Int32(v) => (Int32Array::from(v).boxed(), DataType::Int32),
            Data::Int64(v) => (Int64Array::from(v).boxed(), DataType::Int64),
            Data::Float32(v) => (Float32Array::from(v).boxed(), DataType::Float32),
            Data::Float64(v) => (Float64Array::from(v).boxed(), DataType::Float64),
            Data::Timestamp(v) | Data::TimestampTz(v) => (
                Int64Array::from(v).boxed(),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            Data::Char(v) | Data::Json(v) => {
                (Utf8Array::<i64>::from(v).boxed(), DataType::LargeUtf8)
            }
        }
    }
}

fn create_df(cols: Vec<(String, Col)>) -> Result<DataFrame, Error> {
    let mut df = DataFrame::new(Some(cols.len()));
    for (name, col) in cols {
        let (serie, data_type) = col.into_series_type();
        df.add_series(&name, serie, Some(data_type), None)?;
    }
    Ok(df)
}

fn pg_join(vals: &[&str]) -> Result<String, Error> {
    let mut s = String::new();
    for val in vals {
        if !s.is_empty() {
            write!(s, ",")?;
        }
        write!(s, "\"{}\"", val)?;
    }
    Ok(s)
}

fn pg_vals(len: usize) -> Result<String, Error> {
    let mut s = String::with_capacity(len * 3);
    for i in 1..=len {
        if !s.is_empty() {
            write!(s, ",")?;
        }
        write!(s, "${}", i)?;
    }
    Ok(s)
}

fn pg_excluded(vals: &[&str]) -> Result<String, Error> {
    let mut s = String::new();
    for val in vals {
        if !s.is_empty() {
            write!(s, ",")?;
        }
        write!(s, "\"{}\"=EXCLUDED.\"{}\"", val, val)?;
    }
    Ok(s)
}

const DB_NAME_FORBIDDEN_SYMBOLS: &str = "\"'`";

type PgQuery<'a> = Query<'a, Postgres, <Postgres as sqlx::database::HasArguments<'a>>::Arguments>;

fn pg_bind(q: PgQuery<'_>, arr: Series, is_json: bool) -> Result<PgQuery<'_>, Error> {
    macro_rules! bind_str {
        ($tsize: ty) => {{
            let val: Option<String> = arr
                .as_any()
                .downcast_ref::<Utf8Array<$tsize>>()
                .ok_or(Error::TypeMismatch)?
                .get(0)
                .map(ToOwned::to_owned);
            if is_json {
                if let Some(ref v) = val {
                    q.bind(serde_json::from_str::<Value>(v)?)
                } else {
                    q.bind(None::<Value>)
                }
            } else {
                q.bind(val)
            }
        }};
    }
    let q = match arr.data_type() {
        DataType::Boolean => q.bind(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Int16 => q.bind(
            arr.as_any()
                .downcast_ref::<Int16Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Int32 => q.bind(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Int64 => q.bind(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Float32 => q.bind(
            arr.as_any()
                .downcast_ref::<Float32Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Float64 => q.bind(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0),
        ),
        DataType::Utf8 => {
            bind_str!(i32)
        }
        DataType::LargeUtf8 => {
            bind_str!(i64)
        }
        DataType::Timestamp(time_unit, _) => {
            if let Some(ts) = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(Error::TypeMismatch)?
                .get(0)
            {
                #[allow(clippy::cast_sign_loss)]
                let t = match time_unit {
                    TimeUnit::Second => NaiveDateTime::from_timestamp_opt(ts, 0),
                    TimeUnit::Millisecond => NaiveDateTime::from_timestamp_millis(ts),
                    TimeUnit::Microsecond => NaiveDateTime::from_timestamp_micros(ts),
                    TimeUnit::Nanosecond => NaiveDateTime::from_timestamp_opt(
                        ts / 1_000_000_000,
                        (ts % 1_000_000_000) as u32,
                    ),
                };
                q.bind(t)
            } else {
                q.bind(None::<NaiveDateTime>)
            }
        }
        v => {
            return Err(Error::Unimplemented(format!("{:?}", v)));
        }
    };
    Ok(q)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Params<'a> {
    pub table: &'a str,
    pub postgres: Option<PgParams<'a>>,
    #[serde(default)]
    pub keys: BTreeSet<&'a str>,
    pub fields: Option<BTreeMap<&'a str, FieldParams>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FieldParams {
    #[serde(default)]
    pub key: bool,
    #[serde(default)]
    pub json: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PgParams<'a> {
    pub schema: Option<&'a str>,
}

macro_rules! check_forbidden_symbols {
    ($src: expr, $kind: expr) => {
        for c in $src.chars() {
            if DB_NAME_FORBIDDEN_SYMBOLS.contains(c) {
                return Err(Error::Other(format!(
                    "{} name {} contains invalid symbols",
                    $kind, $src
                )));
            }
        }
    };
}

pub async fn push<'a>(df: &DataFrame, params: &Params<'a>, pool: &PgPool) -> Result<usize, Error> {
    check_forbidden_symbols!(params.table, "table");
    let pg_schema = if let Some(ref pg_params) = params.postgres {
        pg_params.schema
    } else {
        None
    };
    let mut count = 0;
    if df.is_empty() {
        return Ok(count);
    }
    let mut conn = pool.begin().await?;
    let cols = df.names();
    if cols.is_empty() {
        return Ok(count);
    }
    for col in &cols {
        check_forbidden_symbols!(col, "column");
    }
    let mut keys = params.keys.clone();
    let mut json_fields: BTreeSet<&str> = <_>::default();
    if let Some(ref fields) = params.fields {
        for (field, val) in fields {
            if val.key {
                keys.insert(field);
            }
            if val.json {
                json_fields.insert(field);
            }
        }
    }
    let mut q: String = "INSERT INTO ".to_owned();
    if let Some(s) = pg_schema {
        check_forbidden_symbols!(s, "schema");
        write!(q, "\"{}\".", s)?;
    }
    write!(
        q,
        "\"{}\"({}) VALUES ({})",
        params.table,
        pg_join(&cols)?,
        pg_vals(cols.len())?
    )?;
    if !keys.is_empty() {
        let data_cols: Vec<&str> = cols
            .iter()
            .filter(|v| !keys.contains(*v))
            .copied()
            .collect();
        write!(
            q,
            " ON CONFLICT ({}) DO UPDATE SET {}",
            pg_join(&keys.iter().copied().collect::<Vec<&str>>())?,
            pg_excluded(&data_cols)?
        )?;
    }
    for i in 0..df.rows().unwrap_or_default() {
        let mut query = sqlx::query(&q);
        for (arr, col) in df.try_series_sliced(i, 1)?.into_iter().zip(&cols) {
            query = pg_bind(query, arr, json_fields.contains(col))?;
        }
        query.execute(&mut conn).await?;
        count += 1;
    }
    conn.commit().await?;
    Ok(count)
}

pub fn fetch(
    q: String,
    chunk_size: Option<usize>,
    pool: Arc<PgPool>,
) -> Pin<Box<impl Stream<Item = Result<DataFrame, Error>> + Send + ?Sized>> {
    let stream = try_stream! {
        let mut conn = pool.acquire().await?;
        let mut result = sqlx::query(&q).fetch(&mut conn);
        let mut cols: Vec<(String, Col)> = Vec::new();
        while let Some(row) = result.try_next().await? {
            if cols.is_empty() {
                for column in row.columns() {
                    cols.push((
                        column.name().to_owned(),
                        Col::create(cols.len(), column.type_info().name())?,
                    ));
                }
            }
            for (_, col) in &mut cols {
                col.push(&row)?;
            }
            let current_size: usize = cols.iter().map(|c| c.1.size()).sum();
            if let Some(s) = chunk_size {
                if current_size >= s {
                    let df = create_df(cols)?;
                    yield df;
                    cols = Vec::new();
                }
            }
        }
        if !cols.is_empty() {
            let df = create_df(cols)?;
            yield df;
        }
    };
    stream.boxed()
}
