use std::sync::Arc;

use crate::table_scalar::Time;
use crate::{PrimitiveDataType, ScalarValuable};
use crate::{table_scalar, table_list, TableList, TableScalar};
use arrow::array::*;
use arrow::datatypes::*;
use half::f16;

fn arrow_data_type_to_proto(data_type: &DataType) -> Option<PrimitiveDataType> {
    match data_type {
        DataType::Int8 => Some(PrimitiveDataType::Int8),
        DataType::Int16 => Some(PrimitiveDataType::Int16),
        DataType::Int32 => Some(PrimitiveDataType::Int32),
        DataType::Int64 => Some(PrimitiveDataType::Int64),
        DataType::UInt8 => Some(PrimitiveDataType::Uint8),
        DataType::UInt16 => Some(PrimitiveDataType::Uint16),
        DataType::UInt32 => Some(PrimitiveDataType::Uint32),
        DataType::UInt64 => Some(PrimitiveDataType::Uint64),
        DataType::Float16 => Some(PrimitiveDataType::Float16),
        DataType::Float32 => Some(PrimitiveDataType::Float32),
        DataType::Float64 => Some(PrimitiveDataType::Float64),
        DataType::Date32 => Some(PrimitiveDataType::Date32),
        DataType::Date64 => Some(PrimitiveDataType::Date64),
        DataType::Boolean => Some(PrimitiveDataType::Boolean),
        DataType::Utf8 => Some(PrimitiveDataType::Utf8),
        DataType::LargeUtf8 => Some(PrimitiveDataType::LargeUtf8),
        _ => None,
    }
}

pub trait ListValuable {
    fn clone_as_list(&self) -> TableList;
}

impl<T: Array> ListValuable for T {
    fn clone_as_list(&self) -> TableList {
        let values = match self.data_type() {
            // Hard to do with a macro.
            DataType::Int8 => {
                let array = as_primitive_array::<Int8Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).into());
                    } else {
                        values.push(Int8Type::default_value().into());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int8List { values, set };
                Some(table_list::Values::Int8(list))
            }
            DataType::Int16 => {
                let array = as_primitive_array::<Int16Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).into());
                    } else {
                        values.push(Int16Type::default_value().into());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int16List { values, set };
                Some(table_list::Values::Int16(list))
            }
            DataType::Int32 => {
                let array = as_primitive_array::<Int32Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Int32Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int32List { values, set };
                Some(table_list::Values::Int32(list))
            }
            DataType::Int64 => {
                let array = as_primitive_array::<Int64Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Int64Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int64List { values, set };
                Some(table_list::Values::Int64(list))
            }
            DataType::UInt8 => {
                let array = as_primitive_array::<UInt8Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).into());
                    } else {
                        values.push(UInt8Type::default_value().into());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::UInt8List { values, set };
                Some(table_list::Values::Uint8(list))
            }
            DataType::UInt16 => {
                let array = as_primitive_array::<UInt16Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).into());
                    } else {
                        values.push(UInt16Type::default_value().into());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::UInt16List { values, set };
                Some(table_list::Values::Uint16(list))
            }
            DataType::UInt32 => {
                let array = as_primitive_array::<UInt32Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(UInt32Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::UInt32List { values, set };
                Some(table_list::Values::Uint32(list))
            }
            DataType::UInt64 => {
                let array = as_primitive_array::<UInt64Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(UInt64Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::UInt64List { values, set };
                Some(table_list::Values::Uint64(list))
            }
            DataType::Float16 => {
                let array = as_primitive_array::<Float16Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).into());
                    } else {
                        values.push(Float16Type::default_value().into());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Float16List { values, set };
                Some(table_list::Values::Float16(list))
            }
            DataType::Float32 => {
                let array = as_primitive_array::<Float32Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Float32Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Float32List { values, set };
                Some(table_list::Values::Float32(list))
            }
            DataType::Float64 => {
                let array = as_primitive_array::<Float64Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Float64Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Float64List { values, set };
                Some(table_list::Values::Float64(list))
            }
            DataType::Date32 => {
                let array = as_primitive_array::<Date32Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Date32Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int32List { values, set };
                Some(table_list::Values::Date32(list))
            }
            DataType::Date64 => {
                let array = as_primitive_array::<Date64Type>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(Date64Type::default_value());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Int64List { values, set };
                Some(table_list::Values::Date64(list))
            }
            DataType::Boolean => {
                let array = as_boolean_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i));
                    } else {
                        values.push(false);
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::BooleanList { values, set };
                Some(table_list::Values::Boolean(list))
            }
            DataType::Utf8 => {
                let array = as_string_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).to_string());
                    } else {
                        values.push(String::new());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Utf8List { values, set };
                Some(table_list::Values::Utf8(list))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).to_string());
                    } else {
                        values.push(String::new());
                    }
                    set.push(!array.is_null(i));
                }
                let list = table_list::Utf8List { values, set };
                Some(table_list::Values::LargeUtf8(list))
            }
            DataType::List(list_type) => {
                let list_type = arrow_data_type_to_proto(list_type.data_type())
                    .expect("Can't convert a compound arrow array list to a vector list");
                let array = as_list_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for value in array.iter() {
                    if let Some(value) = value {
                        values.push(value.clone_as_list());
                        set.push(true);
                    } else {
                        values.push(TableList::default());
                        set.push(false);
                    }
                }
                Some(table_list::Values::List(table_list::ListList {
                    values,
                    set,
                    list_type: list_type.into(),
                }))
            }
            DataType::LargeList(list_type) => {
                let list_type = arrow_data_type_to_proto(list_type.data_type())
                    .expect("Can't convert a compound arrow array list to a vector list");
                let array = as_list_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for value in array.iter() {
                    if let Some(value) = value {
                        values.push(value.clone_as_list());
                        set.push(true);
                    } else {
                        values.push(TableList::default());
                        set.push(false);
                    }
                }
                Some(table_list::Values::LargeList(table_list::ListList {
                    values,
                    set,
                    list_type: list_type.into(),
                }))
            }
            DataType::Null => {
                unreachable!();
            }
            _ => unimplemented!(),
        };
        TableList { values }
    }
}

impl ScalarValuable for TableList {
    fn scalar(&self, i: usize) -> TableScalar {
        match self.values.as_ref() {
            Some(table_list::Values::Boolean(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Boolean(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int8(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Int8(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int16(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Int16(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Int32(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Int64(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint8(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Uint8(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint16(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Uint16(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Uint32(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Uint64(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Float32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Float32(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Float64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Float64(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Utf8(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Utf8(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeUtf8(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::LargeUtf8(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Binary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Binary(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeBinary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Binary(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::List(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::List(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeList(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::List(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Timestamp(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time {
                    unit,
                    time,
                    tz,
                };
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Timestamp(time)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Date32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Date32(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Date64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Date64(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time32(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time {
                    unit,
                    time,
                    tz,
                };
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Time32(time)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time64(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time {
                    unit,
                    time,
                    tz,
                };
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Time64(time)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Struct(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Struct(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Dictionary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Dictionary(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Union(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::Union(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::FixedSizeBinary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::FixedSizeBinary(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::FixedSizeList(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar { value: Some(table_scalar::Value::FixedSizeList(value)) }
                } else {
                    TableScalar { value: None }
                }
            }
        }
    }
}


struct TableListIter<'a, T: Iterator> {
    values: T,
    set: std::slice::Iter<'a, bool>,
}

impl<'a, T: Iterator> Iterator for TableListIter<'a, T> {
    type Item = Option<T::Item>;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.values.next(), self.set.next()) {
            (Some(val), Some(true)) => Some(Some(val)),
            (Some(_), Some(false)) => Some(None),
            _ => None,
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.set.size_hint()
    }
}

impl TableList {
    pub fn push(&mut self, scalar: TableScalar) -> Result<(), TableScalar> {
        if self.values.is_none() {
            return Err(scalar);
        }
        if scalar.value.is_none() {
            self.push_null();
            return Ok(());
        }
        match (self.values.as_mut().unwrap(), scalar.value.unwrap()) {
            (
                table_list::Values::Boolean(table_list::BooleanList { values, set }),
                table_scalar::Value::Boolean(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Int8(table_list::Int8List { values, set }),
                table_scalar::Value::Int8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Int16(table_list::Int16List { values, set }),
                table_scalar::Value::Int16(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Int32(table_list::Int32List { values, set }),
                table_scalar::Value::Int32(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Int64(table_list::Int64List { values, set }),
                table_scalar::Value::Int64(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Uint8(table_list::UInt8List { values, set }),
                table_scalar::Value::Uint8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Uint16(table_list::UInt16List { values, set }),
                table_scalar::Value::Uint16(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Uint32(table_list::UInt32List { values, set }),
                table_scalar::Value::Uint32(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Uint64(table_list::UInt64List { values, set }),
                table_scalar::Value::Uint64(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Float16(table_list::Float16List { values, set }),
                table_scalar::Value::Float16(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Float32(table_list::Float32List { values, set }),
                table_scalar::Value::Float32(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Float64(table_list::Float64List { values, set }),
                table_scalar::Value::Float64(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Utf8(table_list::Utf8List { values, set }),
                table_scalar::Value::Utf8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Utf8(table_list::Utf8List { values, set }),
                table_scalar::Value::LargeUtf8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::LargeUtf8(table_list::Utf8List { values, set }),
                table_scalar::Value::Utf8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::LargeUtf8(table_list::Utf8List { values, set }),
                table_scalar::Value::LargeUtf8(b),
            ) => {
                values.push(b);
                set.push(true);
            }
            (
                table_list::Values::Timestamp(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time32(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time64(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                }),
                table_scalar::Value::Timestamp(b),
            ) => {
                if b.unit != *unit || b.tz != *tz {
                    return Err(TableScalar { value: Some(table_scalar::Value::Timestamp(b)) });
                }
                times.push(b.time);
                set.push(true);
            }
            (
                table_list::Values::Timestamp(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time32(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time64(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                }),
                table_scalar::Value::Time32(b),
            ) => {
                if b.unit != *unit || b.tz != *tz {
                    return Err(TableScalar { value: Some(table_scalar::Value::Time32(b)) });
                }
                times.push(b.time);
                set.push(true);
            }
            (
                table_list::Values::Timestamp(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time32(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                })
                | table_list::Values::Time64(table_list::TimeList {
                    unit,
                    times,
                    tz,
                    set,
                }),
                table_scalar::Value::Time64(b),
            ) => {
                if b.unit != *unit || b.tz != *tz {
                    return Err(TableScalar { value: Some(table_scalar::Value::Time64(b)) });
                }
                times.push(b.time);
                set.push(true);
            }
            (table_list::Values::List(values), table_scalar::Value::List(list)) => {
                if list.values.is_none() {
                    values.values.push(TableList::default());
                    values.set.push(false);
                }
                match (values.list_type(), list.values.unwrap()) {
                    (PrimitiveDataType::Boolean, table_list::Values::Boolean(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Boolean(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Binary, table_list::Values::Binary(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Binary(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Int8, table_list::Values::Int8(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Int8(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Int16, table_list::Values::Int16(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Int16(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Int32, table_list::Values::Int32(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Int32(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Int64, table_list::Values::Int64(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Int64(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Uint8, table_list::Values::Uint8(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Uint8(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Uint16, table_list::Values::Uint16(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Uint16(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Uint32, table_list::Values::Uint32(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Uint32(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Uint64, table_list::Values::Uint64(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Uint64(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Float16, table_list::Values::Float16(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Float16(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Float32, table_list::Values::Float32(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Float32(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Float64, table_list::Values::Float64(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Float64(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::Utf8, table_list::Values::Utf8(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::Utf8(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (PrimitiveDataType::LargeUtf8, table_list::Values::LargeUtf8(value)) => {
                        let rebuilt_list = TableList {
                            values: Some(table_list::Values::LargeUtf8(value)),
                        };
                        values.values.push(rebuilt_list);
                        values.set.push(true);
                    }
                    (_, list_vals) => {
                        return Err(TableScalar {
                            value: Some(table_scalar::Value::List(TableList {
                                values: Some(list_vals),
                            })),
                        });
                    }
                }
            }
            (_, val) => {
                return Err(TableScalar { value: Some(val) });
            }
        }
        Ok(())
    }

    pub fn push_null(&mut self) {
        if let Some(values) = self.values.as_mut() {
            match values {
                table_list::Values::Boolean(table_list::BooleanList { values, set }) => {
                    values.push(false);
                    set.push(false);
                }
                table_list::Values::Int8(table_list::Int8List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Int16(table_list::Int16List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Int32(table_list::Int32List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Int64(table_list::Int64List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Uint8(table_list::UInt8List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Uint16(table_list::UInt16List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Uint32(table_list::UInt32List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Uint64(table_list::UInt64List { values, set }) => {
                    values.push(0);
                    set.push(false);
                }
                table_list::Values::Float16(table_list::Float16List { values, set }) => {
                    values.push(0.0);
                    set.push(false);
                }
                table_list::Values::Float32(table_list::Float32List { values, set }) => {
                    values.push(0.0);
                    set.push(false);
                }
                table_list::Values::Float64(table_list::Float64List { values, set }) => {
                    values.push(0.0);
                    set.push(false);
                }
                table_list::Values::Utf8(table_list::Utf8List { values, set }) => {
                    values.push(String::new());
                    set.push(false);
                }
                table_list::Values::LargeUtf8(table_list::Utf8List { values, set }) => {
                    values.push(String::new());
                    set.push(false);
                }
                table_list::Values::List(table_list::ListList {
                    values,
                    set,
                    list_type: _,
                }) => {
                    values.push(TableList::default());
                    set.push(false);
                }
                _ => {}
            }
        }
    }

    pub fn to_array(&self) -> ArrayRef {
        if self.values.is_none() {
            panic!("Cannot make an null list into an array.");
        }
        match self.values.as_ref().unwrap() {
            table_list::Values::Boolean(table_list::BooleanList { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(BooleanArray::from(values.clone()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(BooleanArray::from_iter(iter))
                }
            }
            table_list::Values::Int8(table_list::Int8List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Int8Array::from_iter_values(values.iter().map(|f| *f as i8)))
                } else {
                    let iter = TableListIter {
                        values: values.iter().map(|f| *f as i8),
                        set: set.iter(),
                    };
                    Arc::new(Int8Array::from_iter(iter))
                }
            }
            table_list::Values::Int16(table_list::Int16List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Int16Array::from_iter_values(
                        values.iter().map(|f| *f as i16),
                    ))
                } else {
                    let iter = TableListIter {
                        values: values.iter().map(|f| *f as i16),
                        set: set.iter(),
                    };
                    Arc::new(Int16Array::from_iter(iter))
                }
            }
            table_list::Values::Int32(table_list::Int32List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Int32Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(Int32Array::from_iter(iter))
                }
            }
            table_list::Values::Int64(table_list::Int64List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Int64Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(Int64Array::from_iter(iter))
                }
            }
            table_list::Values::Uint8(table_list::UInt8List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(UInt8Array::from_iter_values(
                        values.iter().map(|f| *f as u8),
                    ))
                } else {
                    let iter = TableListIter {
                        values: values.iter().map(|f| *f as u8),
                        set: set.iter(),
                    };
                    Arc::new(UInt8Array::from_iter(iter))
                }
            }
            table_list::Values::Uint16(table_list::UInt16List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(UInt16Array::from_iter_values(
                        values.iter().map(|f| *f as u16),
                    ))
                } else {
                    let iter = TableListIter {
                        values: values.iter().map(|f| *f as u16),
                        set: set.iter(),
                    };
                    Arc::new(UInt16Array::from_iter(iter))
                }
            }
            table_list::Values::Uint32(table_list::UInt32List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(UInt32Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(UInt32Array::from_iter(iter))
                }
            }
            table_list::Values::Uint64(table_list::UInt64List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(UInt64Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(UInt64Array::from_iter(iter))
                }
            }
            table_list::Values::Float16(table_list::Float16List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Float16Array::from_iter_values(
                        values.iter().map(|f| f16::from_f32(*f)),
                    ))
                } else {
                    let iter = TableListIter {
                        values: values.iter().map(|f| f16::from_f32(*f)),
                        set: set.iter(),
                    };
                    Arc::new(iter.collect::<Float16Array>())
                }
            }
            table_list::Values::Float32(table_list::Float32List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Float32Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(Float32Array::from_iter(iter))
                }
            }
            table_list::Values::Float64(table_list::Float64List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(Float64Array::from_iter_values(values.iter().cloned()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(Float64Array::from_iter(iter))
                }
            }
            table_list::Values::Utf8(table_list::Utf8List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(StringArray::from_iter_values(values.iter()))
                } else {
                    let iter = TableListIter {
                        values: values.iter(),
                        set: set.iter(),
                    };
                    Arc::new(StringArray::from_iter(iter))
                }
            }
            table_list::Values::LargeUtf8(table_list::Utf8List { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(LargeStringArray::from_iter_values(values.iter()))
                } else {
                    let iter = TableListIter {
                        values: values.iter(),
                        set: set.iter(),
                    };
                    Arc::new(LargeStringArray::from_iter(iter))
                }
            }
            table_list::Values::List(list_list) => match list_list.list_type() {
                PrimitiveDataType::Int8 => primitive_list_list_builder_int8(list_list),
                PrimitiveDataType::Int16 => primitive_list_list_builder_int16(list_list),
                PrimitiveDataType::Int32 => primitive_list_list_builder_int32(list_list),
                PrimitiveDataType::Int64 => primitive_list_list_builder_int64(list_list),
                PrimitiveDataType::Uint8 => primitive_list_list_builder_uint8(list_list),
                PrimitiveDataType::Uint16 => primitive_list_list_builder_uint16(list_list),
                PrimitiveDataType::Uint32 => primitive_list_list_builder_uint32(list_list),
                PrimitiveDataType::Uint64 => primitive_list_list_builder_uint64(list_list),
                PrimitiveDataType::Float16 => primitive_list_list_builder_float16(list_list),
                PrimitiveDataType::Float32 => primitive_list_list_builder_float32(list_list),
                PrimitiveDataType::Float64 => primitive_list_list_builder_float64(list_list),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        if self.values.is_none() {
            return 0;
        }
        match self.values.as_ref().unwrap() {
            table_list::Values::Boolean(table_list::BooleanList { values, set: _ }) => values.len(),
            table_list::Values::Int8(table_list::Int8List { values, set: _ }) => values.len(),
            table_list::Values::Int16(table_list::Int16List { values, set: _ }) => values.len(),
            table_list::Values::Int32(table_list::Int32List { values, set: _ }) => values.len(),
            table_list::Values::Int64(table_list::Int64List { values, set: _ }) => values.len(),
            table_list::Values::Uint8(table_list::UInt8List { values, set: _ }) => values.len(),
            table_list::Values::Uint16(table_list::UInt16List { values, set: _ }) => values.len(),
            table_list::Values::Uint32(table_list::UInt32List { values, set: _ }) => values.len(),
            table_list::Values::Uint64(table_list::UInt64List { values, set: _ }) => values.len(),
            table_list::Values::Float16(table_list::Float16List { values, set: _ }) => values.len(),
            table_list::Values::Float32(table_list::Float32List { values, set: _ }) => values.len(),
            table_list::Values::Float64(table_list::Float64List { values, set: _ }) => values.len(),
            table_list::Values::Utf8(table_list::Utf8List { values, set: _ }) => values.len(),
            table_list::Values::LargeUtf8(table_list::Utf8List { values, set: _ }) => values.len(),
            table_list::Values::List(table_list::ListList {
                values,
                set: _,
                list_type: _,
            }) => values.len(),
            table_list::Values::LargeList(table_list::ListList {
                values,
                set: _,
                list_type: _,
            }) => values.len(),
            _ => unimplemented!(),
        }
    }
}
macro_rules! small_primitive_list_ingestor {
    ($func_name:ident, $primitive_type:ty, $values_type:ident, $list_type:ident) => {
        fn $func_name(list: &table_list::ListList) -> ArrayRef {
            let primitive_list_builder = PrimitiveBuilder::<$primitive_type>::with_capacity(
                list.values.iter().map(|l| l.len()).sum(),
            );
            let mut list_builder =
                ListBuilder::with_capacity(primitive_list_builder, list.values.len());

            for list in list.values.iter() {
                if let Some(table_list::Values::$values_type(table_list::$list_type {
                    values,
                    set,
                })) = &list.values
                {
                    let primitive_list_builder = list_builder.values();
                    values.iter().zip(set.iter()).for_each(|(v, s)| {
                        if *s {
                            primitive_list_builder.append_value((*v).try_into().unwrap());
                        } else {
                            primitive_list_builder.append_null();
                        }
                    });
                    list_builder.append(true);
                } else {
                    list_builder.append(false);
                }
            }
            Arc::new(list_builder.finish())
        }
    };
}
macro_rules! primitive_list_ingestor {
    ($func_name:ident, $primitive_type:ty, $values_type:ident, $list_type:ident) => {
        fn $func_name(list: &table_list::ListList) -> ArrayRef {
            let primitive_list_builder = PrimitiveBuilder::<$primitive_type>::with_capacity(
                list.values.iter().map(|l| l.len()).sum(),
            );
            let mut list_builder =
                ListBuilder::with_capacity(primitive_list_builder, list.values.len());

            for list in list.values.iter() {
                if let Some(table_list::Values::$values_type(table_list::$list_type {
                    values,
                    set,
                })) = &list.values
                {
                    let primitive_list_builder = list_builder.values();
                    primitive_list_builder.append_values(values.as_slice(), set.as_slice());
                    list_builder.append(true);
                } else {
                    list_builder.append(false);
                }
            }
            Arc::new(list_builder.finish())
        }
    };
}

fn primitive_list_list_builder_float16(list: &table_list::ListList) -> ArrayRef {
    let primitive_list_builder =
        PrimitiveBuilder::<Float16Type>::with_capacity(list.values.iter().map(|l| l.len()).sum());
    let mut list_builder = ListBuilder::with_capacity(primitive_list_builder, list.values.len());

    for list in list.values.iter() {
        if let Some(table_list::Values::Float16(table_list::Float16List { values, set })) =
            &list.values
        {
            let primitive_list_builder = list_builder.values();
            values.iter().zip(set.iter()).for_each(|(v, s)| {
                if *s {
                    primitive_list_builder.append_value(f16::from_f32(*v));
                } else {
                    primitive_list_builder.append_null();
                }
            });
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }
    Arc::new(list_builder.finish())
}

small_primitive_list_ingestor!(primitive_list_list_builder_int8, Int8Type, Int8, Int8List);
small_primitive_list_ingestor!(
    primitive_list_list_builder_int16,
    Int16Type,
    Int16,
    Int16List
);
primitive_list_ingestor!(
    primitive_list_list_builder_int32,
    Int32Type,
    Int32,
    Int32List
);
primitive_list_ingestor!(
    primitive_list_list_builder_int64,
    Int64Type,
    Int64,
    Int64List
);
small_primitive_list_ingestor!(
    primitive_list_list_builder_uint8,
    UInt8Type,
    Uint8,
    UInt8List
);
small_primitive_list_ingestor!(
    primitive_list_list_builder_uint16,
    UInt16Type,
    Uint16,
    UInt16List
);
primitive_list_ingestor!(
    primitive_list_list_builder_uint32,
    UInt32Type,
    Uint32,
    UInt32List
);
primitive_list_ingestor!(
    primitive_list_list_builder_uint64,
    UInt64Type,
    Uint64,
    UInt64List
);
primitive_list_ingestor!(
    primitive_list_list_builder_float32,
    Float32Type,
    Float32,
    Float32List
);
primitive_list_ingestor!(
    primitive_list_list_builder_float64,
    Float64Type,
    Float64,
    Float64List
);

#[cfg(test)]
pub mod tests {
    use std::ops::Deref;

    use super::*;
    use crate::table_list::Float16List;

    macro_rules! primitive_list_test {
        ($func_name:ident, $prim_type:expr, $array_type:ty, $values_type:ident, $list_type:ident, $values:expr, $intended_values:expr, $set:expr) => {
            #[test]
            fn $func_name() {
                let values = $values;
                let array = Arc::new(<$array_type>::from(values));
                let list = array.clone_as_list();
                let intended_list = TableList {
                    values: Some(table_list::Values::$values_type(table_list::$list_type {
                        values: $intended_values,
                        set: $set,
                    })),
                };
                assert_eq!(intended_list, list);
                assert_eq!($prim_type(&list.to_array()), array.deref());
            }
        };
    }
    macro_rules! primitive_list_push_test {
        ($func_name:ident, $prim_type:ident, $array_type:ty, $values_type:ident, $list_type:ident, $values:expr, $intended_values:expr, $set:expr, $value:expr) => {
            #[test]
            fn $func_name() {
                let values = $values;
                let array = Arc::new(<$array_type>::from(values));
                let mut list = TableList {
                    values: Some(table_list::Values::$values_type(table_list::$list_type {
                        values: $intended_values,
                        set: $set,
                    })),
                };
                list.push(TableScalar { value: None }).unwrap();
                list.push_null();
                list.push(TableScalar {
                    value: Some(table_scalar::Value::$values_type($value)),
                })
                .unwrap();

                assert_eq!(
                    as_primitive_array::<$prim_type>(&list.to_array()),
                    array.deref()
                );
            }
        };
    }
    primitive_list_test!(
        test_int8_list_nulless,
        as_primitive_array::<Int8Type>,
        Int8Array,
        Int8,
        Int8List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_int8_list,
        as_primitive_array::<Int8Type>,
        Int8Array,
        Int8,
        Int8List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_int16_list_nulless,
        as_primitive_array::<Int16Type>,
        Int16Array,
        Int16,
        Int16List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_int16_list,
        as_primitive_array::<Int16Type>,
        Int16Array,
        Int16,
        Int16List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_int32_list_nulless,
        as_primitive_array::<Int32Type>,
        Int32Array,
        Int32,
        Int32List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_int32_list,
        as_primitive_array::<Int32Type>,
        Int32Array,
        Int32,
        Int32List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_int64_list_nulless,
        as_primitive_array::<Int64Type>,
        Int64Array,
        Int64,
        Int64List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_int64_list,
        as_primitive_array::<Int64Type>,
        Int64Array,
        Int64,
        Int64List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_uint8_list_nulless,
        as_primitive_array::<UInt8Type>,
        UInt8Array,
        Uint8,
        UInt8List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_uint8_list,
        as_primitive_array::<UInt8Type>,
        UInt8Array,
        Uint8,
        UInt8List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_uint16_list_nulless,
        as_primitive_array::<UInt16Type>,
        UInt16Array,
        Uint16,
        UInt16List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_uint16_list,
        as_primitive_array::<UInt16Type>,
        UInt16Array,
        Uint16,
        UInt16List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_uint32_list_nulless,
        as_primitive_array::<UInt32Type>,
        UInt32Array,
        Uint32,
        UInt32List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_uint32_list,
        as_primitive_array::<UInt32Type>,
        UInt32Array,
        Uint32,
        UInt32List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_uint64_list_nulless,
        as_primitive_array::<UInt64Type>,
        UInt64Array,
        Uint64,
        UInt64List,
        vec![1, 2, 5, 3, 4],
        vec![1, 2, 5, 3, 4],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_uint64_list,
        as_primitive_array::<UInt64Type>,
        UInt64Array,
        Uint64,
        UInt64List,
        vec![Some(1), Some(2), None, Some(3), Some(4)],
        vec![1, 2, 0, 3, 4],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_float32_list_nulless,
        as_primitive_array::<Float32Type>,
        Float32Array,
        Float32,
        Float32List,
        vec![1.0, 2.0, 5.0, 3.0, 4.0],
        vec![1.0, 2.0, 5.0, 3.0, 4.0],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_float32_list,
        as_primitive_array::<Float32Type>,
        Float32Array,
        Float32,
        Float32List,
        vec![Some(1.0), Some(2.0), None, Some(3.0), Some(4.0)],
        vec![1.0, 2.0, 0.0, 3.0, 4.0],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_float64_list_nulless,
        as_primitive_array::<Float64Type>,
        Float64Array,
        Float64,
        Float64List,
        vec![1.0, 2.0, 5.0, 3.0, 4.0],
        vec![1.0, 2.0, 5.0, 3.0, 4.0],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_float64_list,
        as_primitive_array::<Float64Type>,
        Float64Array,
        Float64,
        Float64List,
        vec![Some(1.0), Some(2.0), None, Some(3.0), Some(4.0)],
        vec![1.0, 2.0, 0.0, 3.0, 4.0],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_utf8_list_nulless,
        as_string_array,
        StringArray,
        Utf8,
        Utf8List,
        vec!["1.0", "2.0", "5.0", "3.0", "4.0"],
        vec![
            "1.0".to_string(),
            "2.0".to_string(),
            "5.0".to_string(),
            "3.0".to_string(),
            "4.0".to_string(),
        ],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_utf8_list,
        as_string_array,
        StringArray,
        Utf8,
        Utf8List,
        vec![Some("1.0"), Some("2.0"), None, Some("3.0"), Some("4.0")],
        vec![
            "1.0".to_string(),
            "2.0".to_string(),
            String::new(),
            "3.0".to_string(),
            "4.0".to_string(),
        ],
        vec![true, true, false, true, true]
    );
    primitive_list_test!(
        test_large_utf8_list_nulless,
        as_largestring_array,
        LargeStringArray,
        LargeUtf8,
        Utf8List,
        vec!["1.0", "2.0", "5.0", "3.0", "4.0"],
        vec![
            "1.0".to_string(),
            "2.0".to_string(),
            "5.0".to_string(),
            "3.0".to_string(),
            "4.0".to_string(),
        ],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_large_utf8_list,
        as_largestring_array,
        LargeStringArray,
        LargeUtf8,
        Utf8List,
        vec![Some("1.0"), Some("2.0"), None, Some("3.0"), Some("4.0")],
        vec![
            "1.0".to_string(),
            "2.0".to_string(),
            String::new(),
            "3.0".to_string(),
            "4.0".to_string(),
        ],
        vec![true, true, false, true, true]
    );

    primitive_list_test!(
        test_bool_list_nulless,
        as_boolean_array,
        BooleanArray,
        Boolean,
        BooleanList,
        vec![true, false, true, true, false],
        vec![true, false, true, true, false],
        vec![true, true, true, true, true]
    );
    primitive_list_test!(
        test_bool_list,
        as_boolean_array,
        BooleanArray,
        Boolean,
        BooleanList,
        vec![Some(true), Some(false), None, Some(true), Some(false)],
        vec![true, false, false, true, false],
        vec![true, true, false, true, true]
    );
    primitive_list_push_test!(
        test_int8_list_push,
        Int8Type,
        Int8Array,
        Int8,
        Int8List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_int16_list_push,
        Int16Type,
        Int16Array,
        Int16,
        Int16List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_int32_list_push,
        Int32Type,
        Int32Array,
        Int32,
        Int32List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_int64_list_push,
        Int64Type,
        Int64Array,
        Int64,
        Int64List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_uint8_list_push,
        UInt8Type,
        UInt8Array,
        Uint8,
        UInt8List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_uint16_list_push,
        UInt16Type,
        UInt16Array,
        Uint16,
        UInt16List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_uint32_list_push,
        UInt32Type,
        UInt32Array,
        Uint32,
        UInt32List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_uint64_list_push,
        UInt64Type,
        UInt64Array,
        Uint64,
        UInt64List,
        vec![Some(1), Some(2), None, None, Some(4)],
        vec![1, 2],
        vec![true, true],
        4
    );
    primitive_list_push_test!(
        test_float32_list_push,
        Float32Type,
        Float32Array,
        Float32,
        Float32List,
        vec![Some(1.0), Some(2.0), None, None, Some(4.0)],
        vec![1.0, 2.0],
        vec![true, true],
        4.0
    );
    primitive_list_push_test!(
        test_float64_list_push,
        Float64Type,
        Float64Array,
        Float64,
        Float64List,
        vec![Some(1.0), Some(2.0), None, None, Some(4.0)],
        vec![1.0, 2.0],
        vec![true, true],
        4.0
    );

    #[test]
    fn test_float_16_list() {
        let array: Float16Array = vec![
            Some(f16::from_f32(1.0)),
            Some(f16::from_f32(2.0)),
            None,
            Some(f16::from_f32(3.0)),
            Some(f16::from_f32(4.0)),
        ]
        .into_iter()
        .collect();
        let list = array.clone_as_list();
        let intended_list = TableList {
            values: Some(table_list::Values::Float16(Float16List {
                values: vec![1.0, 2.0, 0.0, 3.0, 4.0],
                set: vec![true, true, false, true, true],
            })),
        };
        assert_eq!(intended_list, list);
        assert_eq!(as_primitive_array::<Float16Type>(&list.to_array()), &array);
    }
    #[test]
    fn test_float_16_list_nulless() {
        let array: Float16Array = [
            f16::from_f32(1.0),
            f16::from_f32(2.0),
            f16::from_f32(5.0),
            f16::from_f32(3.0),
            f16::from_f32(4.0),
        ]
        .into_iter()
        .collect();
        let list = array.clone_as_list();
        let intended_list = TableList {
            values: Some(table_list::Values::Float16(Float16List {
                values: vec![1.0, 2.0, 5.0, 3.0, 4.0],
                set: vec![true, true, true, true, true],
            })),
        };
        assert_eq!(intended_list, list);
        assert_eq!(as_primitive_array::<Float16Type>(&list.to_array()), &array);
    }
}
