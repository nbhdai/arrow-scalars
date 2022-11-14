use std::sync::Arc;

use arrow::{array::*, datatypes::*};
use half::f16;

use crate::{TableList, table_list, ArrowScalarError};


pub(crate) fn dict_builder<T: ArrowDictionaryKeyType>(list: &TableList) -> Result<ArrayRef, ArrowScalarError> {

    match &list.values {
        Some(table_list::Values::Int8(table_list::Int8List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Int8Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as i8).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Int16(table_list::Int16List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Int16Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as i16).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Int32(table_list::Int32List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Int32Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as i32).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Int64(table_list::Int64List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Int64Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as i64).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Uint8(table_list::UInt8List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, UInt8Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as u8).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Uint16(table_list::UInt16List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, UInt16Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as u16).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Uint32(table_list::UInt32List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, UInt32Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as u32).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Uint64(table_list::UInt64List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, UInt64Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as u64).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Float16(table_list::Float16List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Float16Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(f16::from_f32(*v)).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Float32(table_list::Float32List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Float32Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as f32).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Float64(table_list::Float64List {values, set})) => {
            let mut builder = PrimitiveDictionaryBuilder::<T, Float64Type>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(*v as f64).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Some(table_list::Values::Utf8(table_list::Utf8List {values, set})) => {
            let mut builder = StringDictionaryBuilder::<T>::new();
            for (v, s) in values.iter().zip(set.iter()) {
                if *s {
                    builder.append(v).map_err(|e| ArrowScalarError::ArrowError(e))?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ArrowScalarError::Unimplemented("TableList::to_array", "DataType::Dictionary(unknown, unknown)")),
    }
}