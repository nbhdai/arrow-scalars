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
                    builder.append(*v as i8).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v as i16).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v as u8).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v as u16).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(f16::from_f32(*v)).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(*v).map_err(ArrowScalarError::ArrowError)?;
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
                    builder.append(v).map_err(ArrowScalarError::ArrowError)?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ArrowScalarError::Unimplemented("TableList::to_array", "DataType::Dictionary(unknown, unknown)")),
    }
}

pub(crate) fn list_dict_builder<T: ArrowDictionaryKeyType>(data_type: DataType, list: &table_list::ListList) -> Result<ArrayRef, ArrowScalarError> {
    match data_type {
        DataType::Utf8 => {
            let mut list_builder = ListBuilder::new(StringDictionaryBuilder::<T>::new());
            for values in list.values.iter() {
                let values = match values.values.as_ref() {
                    Some(table_list::Values::Dictionary(dict)) => {
                        if let Some(vals) =  dict.values.as_ref() {
                            vals
                        } else {
                            list_builder.append(false);
                            continue;
                        }
                    }
                    None => {
                        list_builder.append(false);
                        continue;
                    }
                    _ => {
                        return Err(ArrowScalarError::InvalidProtobuf);
                    }
                };

                match values.values.as_ref() {
                    Some(table_list::Values::Utf8(table_list::Utf8List {values, set})) => {
                        let values_builder = list_builder.values();
                        for (v, s) in values.iter().zip(set.iter()) {
                            if *s {
                                values_builder.append(v).map_err(ArrowScalarError::ArrowError)?;
                            } else {
                                values_builder.append_null();
                            }
                        }
                        list_builder.append(true);
                    }
                    None => {
                        list_builder.append(false);
                    }
                    _ => {
                        return Err(ArrowScalarError::InvalidProtobuf);
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        _ => Err(ArrowScalarError::Unimplemented("TableList::to_array", "DataType::List(DataType::Dictionary(unknown, unknown))")),
    }
}

pub(crate) fn dict_array_to_proto<T:ArrowDictionaryKeyType>(array: &DictionaryArray<T>, values_type: &DataType) -> Result<table_list::DictionaryList, ArrowScalarError> {
    match values_type {
        DataType::Utf8 => {
            let dict_values = array.values().as_any().downcast_ref::<StringArray>().unwrap();
            let mut values = Vec::with_capacity(array.len());
            let mut set = Vec::with_capacity(array.len());
            let keys = array.keys();
            for key in keys.iter() {
                if let Some(key) = key {
                    set.push(true);
                    values.push(dict_values.value(key.to_usize().unwrap()).to_string());
                } else {
                    set.push(false);
                }
            }
            let string_list = TableList {
                values: Some(table_list::Values::Utf8(table_list::Utf8List {
                    values,
                    set,
                })),
            };
            Ok(table_list::DictionaryList {
                values: Some(Box::new(string_list)),
                index_type: None,
            })
        }
        _ => Err(ArrowScalarError::Unimplemented("TableList::from_array", "DataType::Dictionary(unknown, unknown)")),
    }
}