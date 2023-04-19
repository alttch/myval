#[cfg(feature = "arrow2_ih")]
extern crate arrow2_ih as arrow2;

use crate::df::{DataFrame, Series};
use crate::Error;
use arrow2::array::Array;
use arrow2::datatypes::DataType;
use std::collections::BTreeMap;

enum El<'a> {
    Array(&'a dyn Array),
    Null(Option<Box<dyn Array>>),
}

struct ArrInfo {
    dtype: DataType,
    rows: usize,
}

/// concat multiple data frames
///
/// # Panics
///
/// Should not panic
pub fn concat(data_frames: &[&DataFrame]) -> Result<DataFrame, Error> {
    if data_frames.is_empty() {
        Ok(DataFrame::new0())
    } else {
        let mut fields: Vec<arrow2::datatypes::Field> = Vec::new();
        let mut meta: BTreeMap<String, String> = BTreeMap::new();
        // collect all possible fields
        for df in data_frames {
            meta.extend(df.metadata().clone());
            if !df.is_empty() {
                for field in df.fields() {
                    if !fields.iter().any(|f| f.name == field.name) {
                        fields.push(field.clone());
                    }
                }
            }
        }
        let mut data: Vec<Series> = Vec::with_capacity(fields.len());
        // walk thru all fields in all data frames
        for field in &fields {
            let mut serie: Vec<El> = Vec::with_capacity(data_frames.len());
            for df in data_frames {
                if !df.is_empty() {
                    if let Some((s, _)) = df.get_series(&field.name) {
                        serie.push(El::Array(s.as_ref()));
                    } else {
                        serie.push(El::Null(None));
                    }
                }
            }
            let mut info = None;
            for s in &serie {
                if let El::Array(x) = s {
                    info.replace(ArrInfo {
                        dtype: x.data_type().clone(),
                        rows: x.len(),
                    });
                    break;
                }
            }
            if let Some(i) = info {
                let mut serie_data = Vec::with_capacity(serie.len());
                for s in &mut serie {
                    match s {
                        El::Array(a) => serie_data.push(*a),
                        El::Null(h) => {
                            let arr =
                                arrow2::array::new_null_array(i.dtype.clone(), i.rows).to_boxed();
                            h.replace(arr);
                            serie_data.push(h.as_ref().unwrap().as_ref());
                        }
                    }
                }
                let c_data = arrow2::compute::concatenate::concatenate(&serie_data)?;
                data.push(c_data);
            }
        }
        DataFrame::from_parts(fields, data, Some(meta))
    }
}
