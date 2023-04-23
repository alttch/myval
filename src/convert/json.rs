#[cfg(feature = "arrow2_ih")]
extern crate arrow2_ih as arrow2;

use crate::df::DataFrame;
use crate::Error;
use arrow2::array::{BooleanArray, PrimitiveArray, Utf8Array};
use arrow2::datatypes::DataType;
use serde::Deserialize;
use serde_json::Value;

impl TryFrom<DataFrame> for Value {
    type Error = Error;
    fn try_from(df: DataFrame) -> Result<Self, Self::Error> {
        Ok(Value::Object(df.to_json_map()?))
    }
}

impl TryFrom<&DataFrame> for Value {
    type Error = Error;
    fn try_from(df: &DataFrame) -> Result<Self, Self::Error> {
        Ok(Value::Object(df.to_json_map()?))
    }
}

#[derive(Default)]
pub struct Parser {
    type_map: Vec<(String, DataType)>,
}

impl Parser {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_type_mapping(mut self, name: &str, data_type: DataType) -> Self {
        self.type_map.push((name.to_owned(), data_type));
        self
    }
    pub fn parse_value(&self, value: serde_json::Value) -> Result<DataFrame, Error> {
        match value {
            serde_json::Value::Object(map) => self.parse_map(map),
            _ => Err(Error::Unimplemented(
                "unsupported json value type".to_owned(),
            )),
        }
    }
    pub fn parse_map(
        &self,
        mut map: serde_json::Map<String, serde_json::Value>,
    ) -> Result<DataFrame, Error> {
        let mut df = DataFrame::new(Some(map.len()));
        let mut missing = Vec::new();
        for (col, tp) in &self.type_map {
            if let Some(data) = map.remove(col) {
                macro_rules! v2p {
                    ($arr_kind: ty, $src_kind: ty) => {{
                        let d: Vec<Option<$src_kind>> = Vec::deserialize(data)?;
                        df.add_series0(col, <$arr_kind>::from(d).boxed())?
                    }};
                }
                macro_rules! prim_v2p {
                    ($src_kind: ty) => {
                        v2p!(PrimitiveArray<$src_kind>, $src_kind)
                    };
                }
                match tp {
                    DataType::Boolean => v2p!(BooleanArray, bool),
                    DataType::Float32 => {
                        prim_v2p!(f32);
                    }
                    DataType::Float64 => {
                        prim_v2p!(f64);
                    }
                    DataType::Int8 => {
                        prim_v2p!(i8);
                    }
                    DataType::Int16 => {
                        prim_v2p!(i16);
                    }
                    DataType::Int32 => {
                        prim_v2p!(i32);
                    }
                    DataType::Int64 => {
                        prim_v2p!(i64);
                    }
                    DataType::UInt8 => {
                        prim_v2p!(u8);
                    }
                    DataType::UInt16 => {
                        prim_v2p!(u16);
                    }
                    DataType::UInt32 => {
                        prim_v2p!(u32);
                    }
                    DataType::UInt64 => {
                        prim_v2p!(u64);
                    }
                    DataType::Utf8 => v2p!(Utf8Array<i32>, String),
                    DataType::LargeUtf8 => v2p!(Utf8Array<i64>, String),
                    v => {
                        return Err(Error::Unimplemented(format!("{:?}", v)));
                    }
                }
            } else {
                missing.push((col, tp));
            }
        }
        let rows = df.rows().unwrap_or_default();
        for (col, tp) in missing {
            let arr = arrow2::array::new_null_array(tp.clone(), rows);
            df.add_series0(col, arr)?;
        }
        Ok(df)
    }
}
