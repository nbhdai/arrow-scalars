use std::collections::HashMap;
use std::sync::Arc;

use crate::ScalarValuable;
use crate::dict_array_builder::dict_builder;
use crate::{
    data_type_proto, table_list, table_scalar, ArrowScalarError, DataTypeProto, FieldProto,
    TableList, TableScalar,
};
use arrow::array::*;
use arrow::datatypes::*;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use half::f16;

pub trait ListValuable {
    fn clone_as_list(&self) -> Result<TableList, ArrowScalarError>;
}

impl<T: Array> ListValuable for T {
    fn clone_as_list(&self) -> Result<TableList, ArrowScalarError> {
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
                let list_type = Some(FieldProto::from_arrow(list_type));
                let array = as_list_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for value in array.iter() {
                    if let Some(value) = value {
                        values.push(value.clone_as_list()?);
                        set.push(true);
                    } else {
                        values.push(TableList::default());
                        set.push(false);
                    }
                }
                Some(table_list::Values::List(table_list::ListList {
                    values,
                    set,
                    list_type,
                    size: None,
                }))
            }
            DataType::LargeList(list_type) => {
                let list_type = Some(FieldProto::from_arrow(list_type));
                let array = as_large_list_array(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for value in array.iter() {
                    if let Some(value) = value {
                        values.push(value.clone_as_list()?);
                        set.push(true);
                    } else {
                        values.push(TableList::default());
                        set.push(false);
                    }
                }
                Some(table_list::Values::LargeList(table_list::ListList {
                    values,
                    set,
                    list_type,
                    size: None,
                }))
            }
            DataType::FixedSizeList(list_type, len) => {
                let list_type = Some(FieldProto::from_arrow(list_type));
                let array = self.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if array.is_valid(i) {
                        let value = array.value(i);
                        values.push(value.clone_as_list()?);
                        set.push(true);
                    } else {
                        values.push(TableList::default());
                        set.push(false);
                    }
                }
                Some(table_list::Values::FixedSizeList(table_list::ListList {
                    values,
                    set,
                    list_type,
                    size: Some(*len),
                }))
            }
            DataType::Timestamp(time_unit, tz) => match time_unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<TimestampSecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(TimestampSecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let tz = tz.as_ref().map(|tz| tz.to_string());
                    let time_list = table_list::TimeList { tz, times, set };
                    Some(table_list::Values::TimestampSecond(time_list))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<TimestampMillisecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(TimestampMillisecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let tz = tz.as_ref().map(|tz| tz.to_string());
                    let time_list = table_list::TimeList { tz, times, set };
                    Some(table_list::Values::TimestampMillisecond(time_list))
                }
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<TimestampMicrosecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(TimestampMicrosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let tz = tz.as_ref().map(|tz| tz.to_string());
                    let time_list = table_list::TimeList { tz, times, set };
                    Some(table_list::Values::TimestampMicrosecond(time_list))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<TimestampNanosecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(TimestampNanosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let tz = tz.as_ref().map(|tz| tz.to_string());
                    let time_list = table_list::TimeList { tz, times, set };
                    Some(table_list::Values::TimestampNanosecond(time_list))
                }
            },
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<Time32SecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(Time32SecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int32List { values: times, set };
                    Some(table_list::Values::Time32Second(time_list))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<Time32MillisecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(Time32MillisecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int32List { values: times, set };
                    Some(table_list::Values::Time32Millisecond(time_list))
                }
                _ => None,
            },
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<Time64MicrosecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(Time64MicrosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List { values: times, set };
                    Some(table_list::Values::Time64Microsecond(time_list))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<Time64NanosecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i));
                        } else {
                            times.push(Time64NanosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List { values: times, set };
                    Some(table_list::Values::Time64Nanosecond(time_list))
                }
                _ => None,
            },
            DataType::Binary => {
                let array = as_generic_binary_array::<i32>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).to_vec());
                    } else {
                        values.push(Vec::new());
                    }
                    set.push(!array.is_null(i));
                }
                let binary_list = table_list::BinaryList {
                    values,
                    set,
                    size: None,
                };
                Some(table_list::Values::Binary(binary_list))
            }
            DataType::LargeBinary => {
                let array = as_generic_binary_array::<i64>(self);
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).to_vec());
                    } else {
                        values.push(Vec::new());
                    }
                    set.push(!array.is_null(i));
                }
                let binary_list = table_list::BinaryList {
                    values,
                    set,
                    size: None,
                };
                Some(table_list::Values::Binary(binary_list))
            }
            DataType::FixedSizeBinary(size) => {
                let array = self
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("This is a bug");
                let mut values = Vec::with_capacity(array.len());
                let mut set = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        values.push(array.value(i).to_vec());
                    } else {
                        values.push(vec![0; *size as usize]);
                    }
                    set.push(!array.is_null(i));
                }
                let binary_list = table_list::BinaryList {
                    values,
                    set,
                    size: Some(*size),
                };
                Some(table_list::Values::Binary(binary_list))
            }
            DataType::Duration(time_unit) => match time_unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<DurationSecondType>(self);
                    let mut durations = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            durations.push(array.value(i));
                        } else {
                            durations.push(DurationSecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List {
                        values: durations,
                        set,
                    };
                    Some(table_list::Values::DurationSecond(time_list))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<DurationMillisecondType>(self);
                    let mut durations = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            durations.push(array.value(i));
                        } else {
                            durations.push(DurationMillisecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List {
                        values: durations,
                        set,
                    };
                    Some(table_list::Values::DurationMillisecond(time_list))
                }
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<DurationMicrosecondType>(self);
                    let mut durations = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            durations.push(array.value(i));
                        } else {
                            durations.push(DurationMicrosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List {
                        values: durations,
                        set,
                    };
                    Some(table_list::Values::DurationMicrosecond(time_list))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<DurationNanosecondType>(self);
                    let mut durations = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            durations.push(array.value(i));
                        } else {
                            durations.push(DurationNanosecondType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::Int64List {
                        values: durations,
                        set,
                    };
                    Some(table_list::Values::DurationNanosecond(time_list))
                }
            },
            DataType::Interval(interval_unit) => match interval_unit {
                IntervalUnit::YearMonth => {
                    let array = as_primitive_array::<IntervalYearMonthType>(self);
                    let mut intervals = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            intervals.push(array.value(i));
                        } else {
                            intervals.push(IntervalYearMonthType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let interval_list = table_list::Int32List {
                        values: intervals,
                        set,
                    };
                    Some(table_list::Values::IntervalYearMonth(interval_list))
                }
                IntervalUnit::DayTime => {
                    let array = as_primitive_array::<IntervalDayTimeType>(self);
                    let mut intervals = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            intervals.push(array.value(i));
                        } else {
                            intervals.push(IntervalDayTimeType::default_value());
                        }
                        set.push(!array.is_null(i));
                    }
                    let interval_list = table_list::Int64List {
                        values: intervals,
                        set,
                    };
                    Some(table_list::Values::IntervalDayTime(interval_list))
                }
                IntervalUnit::MonthDayNano => {
                    return Err(ArrowScalarError::Unimplemented(
                        "clone_as_list",
                        "IntervalUnit::MonthDayNano",
                    ));
                }
            },
            DataType::Struct(fields) => {
                let array = as_struct_array(self);
                let fields = fields
                    .iter()
                    .map(FieldProto::from_arrow)
                    .collect::<Vec<_>>();

                let set = (0..array.len()).map(|i| array.is_valid(i)).collect();
                let values = (0..fields.len())
                    .map(|i| {
                        let field_array = array.column(i);
                        field_array.clone_as_list()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let struct_list = table_list::StructList {
                    fields,
                    values,
                    set,
                };
                Some(table_list::Values::Struct(struct_list))
            }
            DataType::Union(_fields, _type_ids, _mode) => {
                return Err(ArrowScalarError::Unimplemented("clone_as_list", "Union"));
            }
            DataType::Dictionary(_key_type, _value_type) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list",
                    "Dictionary",
                ));
            }
            DataType::Decimal128(_precision, _scale) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list",
                    "Decimal128",
                ));
            }
            DataType::Decimal256(_precision, _scale) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list",
                    "Decimal256",
                ));
            }
            DataType::Map(_key_type, _value_type) => {
                return Err(ArrowScalarError::Unimplemented("clone_as_list", "Map"));
            }
            DataType::Null => {
                unreachable!();
            }
        };
        Ok(TableList { values })
    }
}

impl ScalarValuable for TableList {
    fn scalar(&self, i: usize) -> Result<TableScalar, ArrowScalarError> {
        let scalar = match self.values.as_ref() {
            Some(table_list::Values::Boolean(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Boolean(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int8(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Int8(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int16(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Int16(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Int32(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Int64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Int64(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint8(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Uint8(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint16(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Uint16(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Uint32(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Uint64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Uint64(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Float16(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Float16(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Float32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Float32(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Float64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Float64(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Utf8(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Utf8(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeUtf8(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::LargeUtf8(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Binary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Binary(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeBinary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Binary(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::List(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::List(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::LargeList(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::List(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Date32(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Date32(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Date64(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Date64(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time32Second(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time32Second(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time32Millisecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time32Millisecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time64Microsecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time64Microsecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time64Nanosecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time64Nanosecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::DurationSecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::DurationSecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::DurationMillisecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::DurationMillisecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::DurationMicrosecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::DurationMicrosecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::DurationNanosecond(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::DurationNanosecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::IntervalYearMonth(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::IntervalYearMonth(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::IntervalDayTime(list)) => {
                let value = list.values[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::IntervalDayTime(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::TimestampSecond(list)) => {
                let value = list.times[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::TimestampSecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::TimestampMillisecond(list)) => {
                let value = list.times[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::TimestampMillisecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::TimestampMicrosecond(list)) => {
                let value = list.times[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::TimestampMicrosecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::TimestampNanosecond(list)) => {
                let value = list.times[i];
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::TimestampNanosecond(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Struct(list)) => {
                if list.set[i] {
                    let elements: Result<HashMap<String, TableScalar>, ArrowScalarError> = list
                        .fields
                        .iter()
                        .zip(list.values.iter())
                        .map(|(field, value)| {
                            let value = value.scalar(i)?;
                            Ok((field.name.clone(), value))
                        })
                        .collect();
                    let value = table_scalar::Value::Struct(table_scalar::Struct {
                        elements: elements?,
                    });
                    TableScalar { value: Some(value) }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Dictionary(list)) => {
                if let Some(values) = list.values.as_ref() {
                    values.scalar(i)?
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Union(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Union(Box::new(value))),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::FixedSizeBinary(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::FixedSizeBinary(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::FixedSizeList(list)) => {
                let value = list.values[i].clone();
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::FixedSizeList(value)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            None => TableScalar { value: None },
        };
        Ok(scalar)
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

fn pop_value_ret(
    value: Option<table_scalar::Value>,
    set: Option<bool>,
) -> Option<table_scalar::Value> {
    match (value, set) {
        (Some(val), Some(true)) => Some(val),
        (Some(_val), Some(false)) => None,
        _ => None,
    }
}

impl TableList {
    pub fn new(data_type: &DataType) -> Result<Self, ArrowScalarError> {
        let values = match data_type {
            DataType::Boolean => table_list::Values::Boolean(table_list::BooleanList::default()),
            DataType::Int8 => table_list::Values::Int8(table_list::Int8List::default()),
            DataType::Int16 => table_list::Values::Int16(table_list::Int16List::default()),
            DataType::Int32 => table_list::Values::Int32(table_list::Int32List::default()),
            DataType::Int64 => table_list::Values::Int64(table_list::Int64List::default()),
            DataType::UInt8 => table_list::Values::Uint8(table_list::UInt8List::default()),
            DataType::UInt16 => table_list::Values::Uint16(table_list::UInt16List::default()),
            DataType::UInt32 => table_list::Values::Uint32(table_list::UInt32List::default()),
            DataType::UInt64 => table_list::Values::Uint64(table_list::UInt64List::default()),
            DataType::Float16 => table_list::Values::Float16(table_list::Float16List::default()),
            DataType::Float32 => table_list::Values::Float32(table_list::Float32List::default()),
            DataType::Float64 => table_list::Values::Float64(table_list::Float64List::default()),
            DataType::Date32 => table_list::Values::Date32(table_list::Int32List::default()),
            DataType::Date64 => table_list::Values::Date64(table_list::Int64List::default()),
            DataType::Binary => table_list::Values::Binary(table_list::BinaryList::default()),
            DataType::LargeBinary => {
                table_list::Values::LargeBinary(table_list::BinaryList::default())
            }
            DataType::FixedSizeBinary(size) => {
                table_list::Values::FixedSizeBinary(table_list::BinaryList {
                    size: Some(*size),
                    ..Default::default()
                })
            }
            DataType::Utf8 => table_list::Values::Utf8(table_list::Utf8List::default()),
            DataType::LargeUtf8 => table_list::Values::LargeUtf8(table_list::Utf8List::default()),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    table_list::Values::Time32Second(table_list::Int32List::default())
                }
                TimeUnit::Millisecond => {
                    table_list::Values::Time32Millisecond(table_list::Int32List::default())
                }
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    table_list::Values::Time64Microsecond(table_list::Int64List::default())
                }
                TimeUnit::Nanosecond => {
                    table_list::Values::Time64Nanosecond(table_list::Int64List::default())
                }
                _ => unreachable!(),
            },
            DataType::Timestamp(unit, tz) => match unit {
                TimeUnit::Second => table_list::Values::TimestampSecond(table_list::TimeList {
                    tz: tz.clone(),
                    ..Default::default()
                }),
                TimeUnit::Millisecond => {
                    table_list::Values::TimestampMillisecond(table_list::TimeList {
                        tz: tz.clone(),
                        ..Default::default()
                    })
                }
                TimeUnit::Microsecond => {
                    table_list::Values::TimestampMicrosecond(table_list::TimeList {
                        tz: tz.clone(),
                        ..Default::default()
                    })
                }
                TimeUnit::Nanosecond => {
                    table_list::Values::TimestampNanosecond(table_list::TimeList {
                        tz: tz.clone(),
                        ..Default::default()
                    })
                }
            },
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => {
                    table_list::Values::IntervalYearMonth(table_list::Int32List::default())
                }
                IntervalUnit::DayTime => {
                    table_list::Values::IntervalDayTime(table_list::Int64List::default())
                }
                IntervalUnit::MonthDayNano => {
                    return Err(ArrowScalarError::Unimplemented(
                        "TableList::new",
                        "IntervalUnit::MonthDayNano",
                    ))
                }
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => {
                    table_list::Values::DurationSecond(table_list::Int64List::default())
                }
                TimeUnit::Millisecond => {
                    table_list::Values::DurationMillisecond(table_list::Int64List::default())
                }
                TimeUnit::Microsecond => {
                    table_list::Values::DurationMicrosecond(table_list::Int64List::default())
                }
                TimeUnit::Nanosecond => {
                    table_list::Values::DurationNanosecond(table_list::Int64List::default())
                }
            },
            DataType::List(field) => table_list::Values::List(table_list::ListList {
                list_type: Some(FieldProto::from_arrow(field)),
                ..Default::default()
            }),
            DataType::LargeList(field) => table_list::Values::LargeList(table_list::ListList {
                list_type: Some(FieldProto::from_arrow(field)),
                ..Default::default()
            }),
            DataType::FixedSizeList(field, size) => {
                table_list::Values::LargeList(table_list::ListList {
                    size: Some(*size),
                    list_type: Some(FieldProto::from_arrow(field)),
                    ..Default::default()
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                table_list::Values::Dictionary(Box::new(table_list::DictionaryList {
                    index_type: Some(DataTypeProto::from_arrow(key_type)),
                    values: Some(Box::new(TableList::new(value_type)?)),
                    ..Default::default()
                }))
            }
            DataType::Struct(fields) => {
                let values = fields
                    .iter()
                    .map(|field| TableList::new(field.data_type()))
                    .collect::<Result<Vec<_>, _>>()?;
                let fields = fields
                    .iter()
                    .map(FieldProto::from_arrow)
                    .collect::<Vec<_>>();
                table_list::Values::Struct(table_list::StructList {
                    values,
                    fields,
                    ..Default::default()
                })
            }
            DataType::Decimal128(_, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::new",
                    "Decimal128",
                ));
            }
            DataType::Decimal256(_, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::new",
                    "Decimal256",
                ));
            }
            DataType::Map(_, _) => {
                return Err(ArrowScalarError::Unimplemented("TableList::new", "Map"));
            }
            DataType::Union(_, _, _) => table_list::Values::Union(table_list::UnionList::default()),
            DataType::Null => {
                return Ok(TableList { values: None });
            }
        };
        Ok(TableList {
            values: Some(values),
        })
    }

    pub fn push(&mut self, scalar: TableScalar) -> Result<(), ArrowScalarError> {
        if self.values.is_none() {
            return Err(ArrowScalarError::InvalidScalar(scalar));
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
            (table_list::Values::List(values), table_scalar::Value::List(list)) => {
                if let Err(list) = values.push(list) {
                    return Err(ArrowScalarError::InvalidScalar(TableScalar {
                        value: Some(table_scalar::Value::List(list)),
                    }));
                }
            }
            (table_list::Values::LargeList(values), table_scalar::Value::List(list)) => {
                if let Err(list) = values.push(list) {
                    return Err(ArrowScalarError::InvalidScalar(TableScalar {
                        value: Some(table_scalar::Value::List(list)),
                    }));
                }
            }
            (table_list::Values::FixedSizeList(values), table_scalar::Value::List(list)) => {
                if let Err(list) = values.push(list) {
                    return Err(ArrowScalarError::InvalidScalar(TableScalar {
                        value: Some(table_scalar::Value::List(list)),
                    }));
                }
            }
            (table_list::Values::Struct(struct_list), table_scalar::Value::Struct(mut values)) => {
                if struct_list.values.len() == values.elements.len()
                    && struct_list
                        .fields
                        .iter()
                        .all(|k| values.elements.contains_key(&k.name))
                {
                    for (field, value) in
                        struct_list.fields.iter().zip(struct_list.values.iter_mut())
                    {
                        value.push(values.elements.remove(&field.name).unwrap())?;
                    }
                } else {
                    return Err(ArrowScalarError::InvalidScalar(TableScalar {
                        value: Some(table_scalar::Value::Struct(values)),
                    }));
                }
            }
            (table_list::Values::Binary(values), table_scalar::Value::Binary(b)) => {
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::LargeBinary(values), table_scalar::Value::Binary(b)) => {
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::FixedSizeBinary(values), table_scalar::Value::Binary(b)) => {
                if values.size != Some(b.len() as i32) {
                    return Err(ArrowScalarError::InvalidScalar(TableScalar {
                        value: Some(table_scalar::Value::Binary(b)),
                    }));
                }
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::Time32Second(values), table_scalar::Value::Time32Second(b)) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::Time32Millisecond(values),
                table_scalar::Value::Time32Millisecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::Time64Microsecond(values),
                table_scalar::Value::Time64Microsecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::Time64Nanosecond(values),
                table_scalar::Value::Time64Nanosecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::Date32(values), table_scalar::Value::Date32(b)) => {
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::Date64(values), table_scalar::Value::Date64(b)) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::TimestampSecond(values),
                table_scalar::Value::TimestampSecond(b),
            ) => {
                values.times.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::TimestampMillisecond(values),
                table_scalar::Value::TimestampMillisecond(b),
            ) => {
                values.times.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::TimestampMicrosecond(values),
                table_scalar::Value::TimestampMicrosecond(b),
            ) => {
                values.times.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::TimestampNanosecond(values),
                table_scalar::Value::TimestampNanosecond(b),
            ) => {
                values.times.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::IntervalYearMonth(values),
                table_scalar::Value::IntervalYearMonth(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::IntervalDayTime(values),
                table_scalar::Value::IntervalDayTime(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::DurationSecond(values),
                table_scalar::Value::DurationSecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::DurationMillisecond(values),
                table_scalar::Value::DurationMillisecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::DurationMicrosecond(values),
                table_scalar::Value::DurationMicrosecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (
                table_list::Values::DurationNanosecond(values),
                table_scalar::Value::DurationNanosecond(b),
            ) => {
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::Union(values), table_scalar::Value::Union(union)) => {
                values.values.push(*union);
                values.set.push(true);
            }
            (table_list::Values::Dictionary(values), table_scalar::Value::Dictionary(dict)) => {
                values.values.as_mut().expect("valid proto").push(*dict)?;
                values.set.push(true);
            }
            (_, val) => {
                return Err(ArrowScalarError::InvalidScalar(TableScalar {
                    value: Some(val),
                }));
            }
        }
        Ok(())
    }

    //todo! test this
    pub fn pop(&mut self) -> Option<TableScalar> {
        self.values.as_ref()?;
        let value = match self.values.as_mut().unwrap() {
            table_list::Values::Boolean(table_list::BooleanList { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Boolean);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Int8(table_list::Int8List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Int8);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Int16(table_list::Int16List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Int16);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Int32(table_list::Int32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Int32);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Int64(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Int64);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Uint8(table_list::UInt8List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Uint8);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Uint16(table_list::UInt16List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Uint16);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Uint32(table_list::UInt32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Uint32);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Uint64(table_list::UInt64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Uint64);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Float16(table_list::Float16List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Float16);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Float32(table_list::Float32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Float32);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Float64(table_list::Float64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Float64);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Utf8(table_list::Utf8List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Utf8);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::LargeUtf8(table_list::Utf8List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::LargeUtf8);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::List(table_list::ListList {
                values,
                set,
                list_type: _,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::List);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::LargeList(table_list::ListList {
                values,
                set,
                list_type: _,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::LargeList);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::FixedSizeList(table_list::ListList {
                values,
                set,
                list_type: _,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::FixedSizeList);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Binary(table_list::BinaryList {
                values,
                set,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::Binary);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::LargeBinary(table_list::BinaryList {
                values,
                set,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::LargeBinary);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::FixedSizeBinary(table_list::BinaryList {
                values,
                set,
                size: _,
            }) => {
                let value = values.pop().map(table_scalar::Value::FixedSizeBinary);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Struct(table_list::StructList {
                fields,
                values,
                set,
            }) => {
                //todo!("Make this resilient")
                if Some(true) == set.pop() {
                    let elements: HashMap<String, TableScalar> = fields
                        .iter()
                        .zip(values.iter_mut())
                        .map(|(field, values)| (field.name.to_owned(), values.pop().unwrap()))
                        .collect();
                    Some(table_scalar::Value::Struct(table_scalar::Struct {
                        elements,
                    }))
                } else {
                    None
                }
            }
            table_list::Values::Union(table_list::UnionList { values, set }) => {
                let value = values
                    .pop()
                    .map(|val| table_scalar::Value::Union(Box::new(val)));
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Dictionary(dict) => {
                let table_list::DictionaryList {
                    values,
                    index_type: _,
                    set,
                } = dict.as_mut();
                let value = values.as_mut().and_then(|v| {
                    v.pop()
                        .map(|val| table_scalar::Value::Dictionary(Box::new(val)))
                });
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Time32Second(table_list::Int32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Time32Second);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Time32Millisecond(table_list::Int32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Time32Millisecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Time64Microsecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Time64Microsecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Time64Nanosecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Time64Nanosecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::TimestampSecond(table_list::TimeList { times, tz: _, set }) => {
                let value = times.pop().map(table_scalar::Value::TimestampSecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::TimestampMillisecond(table_list::TimeList {
                times,
                tz: _,
                set,
            }) => {
                let value = times.pop().map(table_scalar::Value::TimestampMillisecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::TimestampMicrosecond(table_list::TimeList {
                times,
                tz: _,
                set,
            }) => {
                let value = times.pop().map(table_scalar::Value::TimestampMicrosecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::TimestampNanosecond(table_list::TimeList { times, tz: _, set }) => {
                let value = times.pop().map(table_scalar::Value::TimestampNanosecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }

            table_list::Values::Date32(table_list::Int32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Date32);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::Date64(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::Date64);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::IntervalYearMonth(table_list::Int32List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::IntervalYearMonth);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::IntervalDayTime(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::IntervalDayTime);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::DurationSecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::DurationSecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::DurationMillisecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::DurationMillisecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::DurationMicrosecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::DurationMicrosecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
            table_list::Values::DurationNanosecond(table_list::Int64List { values, set }) => {
                let value = values.pop().map(table_scalar::Value::DurationNanosecond);
                let set = set.pop();
                pop_value_ret(value, set)
            }
        };

        Some(TableScalar { value })
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
                    size: _,
                }) => {
                    values.push(TableList::default());
                    set.push(false);
                }
                _ => {}
            }
        }
    }

    pub fn push_date_time(&mut self, time: NaiveDateTime) -> Result<(), NaiveDateTime> {
        if let Some(values) = self.values.as_mut() {
            match values {
                table_list::Values::Date32(table_list::Int32List { values, set }) => {
                    values.push(time.num_days_from_ce());
                    set.push(true);
                }
                table_list::Values::Date64(table_list::Int64List { values, set }) => {
                    values.push(time.timestamp_millis());
                    set.push(true);
                }
                table_list::Values::TimestampSecond(table_list::TimeList { times, tz: _, set }) => {
                    times.push(time.timestamp());
                    set.push(true);
                }
                table_list::Values::TimestampMillisecond(table_list::TimeList {
                    times,
                    tz: _,
                    set,
                }) => {
                    times.push(time.timestamp_millis());
                    set.push(true);
                }
                table_list::Values::TimestampMicrosecond(table_list::TimeList {
                    times,
                    tz: _,
                    set,
                }) => {
                    times.push(time.timestamp_nanos() * 1000);
                    set.push(true);
                }
                table_list::Values::TimestampNanosecond(table_list::TimeList {
                    times,
                    tz: _,
                    set,
                }) => {
                    times.push(time.timestamp_nanos());
                    set.push(true);
                }
                table_list::Values::Time32Second(table_list::Int32List { values, set }) => {
                    values.push(time.num_seconds_from_midnight() as i32);
                    set.push(true);
                }
                table_list::Values::Time32Millisecond(table_list::Int32List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let milli: i64 = time.nanosecond().into();
                    values.push((seconds * 1000 + milli / 1000000) as i32);
                    set.push(true);
                }
                table_list::Values::Time64Microsecond(table_list::Int64List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let micro: i64 = time.nanosecond().into();
                    values.push(seconds * 1000000 + micro / 1000);
                    set.push(true);
                }
                table_list::Values::Time64Nanosecond(table_list::Int64List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let nano: i64 = time.nanosecond().into();
                    values.push(seconds * 1000000000 + nano);
                    set.push(true);
                }
                _ => return Err(time),
            }
        }
        Ok(())
    }

    pub fn push_time(&mut self, time: NaiveTime) -> Result<(), NaiveTime> {
        if let Some(values) = self.values.as_mut() {
            match values {
                table_list::Values::Time32Second(table_list::Int32List { values, set }) => {
                    values.push(time.num_seconds_from_midnight() as i32);
                    set.push(true);
                }
                table_list::Values::Time32Millisecond(table_list::Int32List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let milli: i64 = time.nanosecond().into();
                    values.push((seconds * 1000 + milli / 1000000) as i32);
                    set.push(true);
                }
                table_list::Values::Time64Microsecond(table_list::Int64List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let micro: i64 = time.nanosecond().into();
                    values.push(seconds * 1000000 + micro / 1000);
                    set.push(true);
                }
                table_list::Values::Time64Nanosecond(table_list::Int64List { values, set }) => {
                    let seconds: i64 = time.num_seconds_from_midnight().into();
                    let nano: i64 = time.nanosecond().into();
                    values.push(seconds * 1000000000 + nano);
                    set.push(true);
                }
                _ => return Err(time),
            }
        }
        Ok(())
    }

    pub fn push_date(&mut self, date: NaiveDate) -> Result<(), NaiveDate> {
        if let Some(values) = self.values.as_mut() {
            match values {
                table_list::Values::Date32(table_list::Int32List { values, set }) => {
                    values.push(date.num_days_from_ce());
                    set.push(true);
                }
                table_list::Values::Date64(table_list::Int64List { values, set }) => {
                    values.push(date.and_hms(0, 0, 0).timestamp_millis());
                    set.push(true);
                }
                _ => return Err(date),
            }
        }
        Ok(())
    }

    pub fn to_array(&self) -> Result<ArrayRef, ArrowScalarError> {
        if self.values.is_none() {
            panic!("Cannot make an null list into an array.");
        }
        let array = match self.values.as_ref().unwrap() {
            table_list::Values::Boolean(table_list::BooleanList { values, set }) => {
                if set.iter().all(|f| !f) {
                    Arc::new(BooleanArray::from(values.clone()))
                } else {
                    let iter = TableListIter {
                        values: values.iter().cloned(),
                        set: set.iter(),
                    };
                    Arc::new(BooleanArray::from_iter(iter)) as ArrayRef
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
            table_list::Values::List(list_list) => {
                if list_list.list_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_type = list_list.list_type.as_ref().unwrap();
                if list_type.data_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_data_type = list_type.data_type.as_ref().unwrap();
                if list_data_type.data_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_data_type = list_data_type.data_type.as_ref().unwrap();
                match list_data_type {
                    data_type_proto::DataType::Int8(_) => {
                        primitive_list_list_builder_int8(list_list)
                    }
                    data_type_proto::DataType::Int16(_) => {
                        primitive_list_list_builder_int16(list_list)
                    }
                    data_type_proto::DataType::Int32(_) => {
                        primitive_list_list_builder_int32(list_list)
                    }
                    data_type_proto::DataType::Int64(_) => {
                        primitive_list_list_builder_int64(list_list)
                    }
                    data_type_proto::DataType::Uint8(_) => {
                        primitive_list_list_builder_uint8(list_list)
                    }
                    data_type_proto::DataType::Uint16(_) => {
                        primitive_list_list_builder_uint16(list_list)
                    }
                    data_type_proto::DataType::Uint32(_) => {
                        primitive_list_list_builder_uint32(list_list)
                    }
                    data_type_proto::DataType::Uint64(_) => {
                        primitive_list_list_builder_uint64(list_list)
                    }
                    data_type_proto::DataType::Float16(_) => {
                        primitive_list_list_builder_float16(list_list)
                    }
                    data_type_proto::DataType::Float32(_) => {
                        primitive_list_list_builder_float32(list_list)
                    }
                    data_type_proto::DataType::Float64(_) => {
                        primitive_list_list_builder_float64(list_list)
                    }
                    data_type_proto::DataType::Time32Second(_) => {
                        primitive_list_list_builder_time32_second(list_list)
                    }
                    data_type_proto::DataType::Time32Millisecond(_) => {
                        primitive_list_list_builder_time32_millisecond(list_list)
                    }
                    data_type_proto::DataType::Time64Microsecond(_) => {
                        primitive_list_list_builder_time64_microsecond(list_list)
                    }
                    data_type_proto::DataType::Time64Nanosecond(_) => {
                        primitive_list_list_builder_time64_nanosecond(list_list)
                    }
                    data_type_proto::DataType::DurationSecond(_) => {
                        primitive_list_list_builder_duration_second(list_list)
                    }
                    data_type_proto::DataType::DurationMillisecond(_) => {
                        primitive_list_list_builder_duration_millisecond(list_list)
                    }
                    data_type_proto::DataType::DurationMicrosecond(_) => {
                        primitive_list_list_builder_duration_microsecond(list_list)
                    }
                    data_type_proto::DataType::DurationNanosecond(_) => {
                        primitive_list_list_builder_duration_nanosecond(list_list)
                    }
                    data_type_proto::DataType::Date32(_) => {
                        primitive_list_list_builder_date32(list_list)
                    }
                    data_type_proto::DataType::Date64(_) => {
                        primitive_list_list_builder_date64(list_list)
                    }
                    data_type_proto::DataType::IntervalYearMonth(_) => {
                        primitive_list_list_builder_interval_year_month(list_list)
                    }
                    data_type_proto::DataType::IntervalDayTime(_) => {
                        primitive_list_list_builder_interval_day_time(list_list)
                    }
                    data_type_proto::DataType::Utf8(_) => {
                        string_list_list_builder(list_list)
                    }

                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array",
                            "DataType::List::Unknown",
                        ))
                    }
                }
            }
            table_list::Values::LargeList(list_list) => {
                if list_list.list_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_type = list_list.list_type.as_ref().unwrap();
                if list_type.data_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_data_type = list_type.data_type.as_ref().unwrap();
                if list_data_type.data_type.is_none() {
                    return Err(ArrowScalarError::InvalidProtobuf);
                }
                let list_data_type = list_data_type.data_type.as_ref().unwrap();
                match list_data_type {
                    data_type_proto::DataType::Int8(_) => {
                        primitive_large_list_list_builder_int8(list_list)
                    }
                    data_type_proto::DataType::Int16(_) => {
                        primitive_large_list_list_builder_int16(list_list)
                    }
                    data_type_proto::DataType::Int32(_) => {
                        primitive_large_list_list_builder_int32(list_list)
                    }
                    data_type_proto::DataType::Int64(_) => {
                        primitive_large_list_list_builder_int64(list_list)
                    }
                    data_type_proto::DataType::Uint8(_) => {
                        primitive_large_list_list_builder_uint8(list_list)
                    }
                    data_type_proto::DataType::Uint16(_) => {
                        primitive_large_list_list_builder_uint16(list_list)
                    }
                    data_type_proto::DataType::Uint32(_) => {
                        primitive_large_list_list_builder_uint32(list_list)
                    }
                    data_type_proto::DataType::Uint64(_) => {
                        primitive_large_list_list_builder_uint64(list_list)
                    }
                    data_type_proto::DataType::Float16(_) => {
                        primitive_large_list_list_builder_float16(list_list)
                    }
                    data_type_proto::DataType::Float32(_) => {
                        primitive_large_list_list_builder_float32(list_list)
                    }
                    data_type_proto::DataType::Float64(_) => {
                        primitive_large_list_list_builder_float64(list_list)
                    }
                    data_type_proto::DataType::Time32Second(_) => {
                        primitive_large_list_list_builder_time32_second(list_list)
                    }
                    data_type_proto::DataType::Time32Millisecond(_) => {
                        primitive_large_list_list_builder_time32_millisecond(list_list)
                    }
                    data_type_proto::DataType::Time64Microsecond(_) => {
                        primitive_large_list_list_builder_time64_microsecond(list_list)
                    }
                    data_type_proto::DataType::Time64Nanosecond(_) => {
                        primitive_large_list_list_builder_time64_nanosecond(list_list)
                    }
                    data_type_proto::DataType::DurationSecond(_) => {
                        primitive_large_list_list_builder_duration_second(list_list)
                    }
                    data_type_proto::DataType::DurationMillisecond(_) => {
                        primitive_large_list_list_builder_duration_millisecond(list_list)
                    }
                    data_type_proto::DataType::DurationMicrosecond(_) => {
                        primitive_large_list_list_builder_duration_microsecond(list_list)
                    }
                    data_type_proto::DataType::DurationNanosecond(_) => {
                        primitive_large_list_list_builder_duration_nanosecond(list_list)
                    }
                    data_type_proto::DataType::Date32(_) => {
                        primitive_large_list_list_builder_date32(list_list)
                    }
                    data_type_proto::DataType::Date64(_) => {
                        primitive_large_list_list_builder_date64(list_list)
                    }
                    data_type_proto::DataType::IntervalYearMonth(_) => {
                        primitive_large_list_list_builder_interval_year_month(list_list)
                    }
                    data_type_proto::DataType::IntervalDayTime(_) => {
                        primitive_large_list_list_builder_interval_day_time(list_list)
                    }
                    data_type_proto::DataType::Utf8(_) => {
                        string_large_list_list_builder(list_list)
                    }
                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array",
                            "DataType::List::Unknown",
                        ))
                    }
                }
            }
            table_list::Values::Binary(list) => {
                let mut builder = BinaryBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::LargeBinary(list) => {
                let mut builder = LargeBinaryBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Struct(struct_list) => {
                let arrays = struct_list
                    .fields
                    .iter()
                    .zip(struct_list.values.iter())
                    .map(|(field, list)| Ok((field.to_arrow()?, list.to_array()?)))
                    .collect::<Result<Vec<_>, ArrowScalarError>>()?;
                Arc::new(StructArray::from(arrays))
            }
            table_list::Values::TimestampSecond(list) => {
                let mut builder = TimestampSecondBuilder::new();
                for (i, value) in list.times.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::TimestampMillisecond(list) => {
                let mut builder = TimestampMillisecondBuilder::new();
                for (i, value) in list.times.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::TimestampMicrosecond(list) => {
                let mut builder = TimestampMicrosecondBuilder::new();
                for (i, value) in list.times.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::TimestampNanosecond(list) => {
                let mut builder = TimestampNanosecondBuilder::new();
                for (i, value) in list.times.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }

            table_list::Values::Time32Second(list) => {
                let mut builder = Time32SecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Time32Millisecond(list) => {
                let mut builder = Time32MillisecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Time64Microsecond(list) => {
                let mut builder = Time64MicrosecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Time64Nanosecond(list) => {
                let mut builder = Time64NanosecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }

            table_list::Values::Date32(list) => {
                let mut builder = Date32Builder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Date64(list) => {
                let mut builder = Date64Builder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::DurationSecond(list) => {
                let mut builder = DurationSecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::DurationMillisecond(list) => {
                let mut builder = DurationMillisecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::DurationMicrosecond(list) => {
                let mut builder = DurationMicrosecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::DurationNanosecond(list) => {
                let mut builder = DurationNanosecondBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::IntervalYearMonth(list) => {
                let mut builder = IntervalYearMonthBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::IntervalDayTime(list) => {
                let mut builder = IntervalDayTimeBuilder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::Dictionary(dictionary_list) => {
                let key_type = if let Some(dict) = &dictionary_list.index_type {
                    dict.to_arrow()?
                } else {
                    return Err(ArrowScalarError::InvalidProtobuf);
                };
                let values = if let Some(values) = &dictionary_list.values {
                    values
                } else {
                    return Err(ArrowScalarError::InvalidProtobuf);
                };
                match key_type {
                    DataType::Int8 => {
                        dict_builder::<Int8Type>(values)?
                    }
                    DataType::Int16 => {
                        dict_builder::<Int16Type>(values)?
                    }
                    DataType::Int32 => {
                        dict_builder::<Int32Type>(values)?
                    }
                    DataType::Int64 => {
                        dict_builder::<Int64Type>(values)?
                    }
                    DataType::UInt8 => {
                        dict_builder::<UInt8Type>(values)?
                    }
                    DataType::UInt16 => {
                        dict_builder::<UInt16Type>(values)?
                    }
                    DataType::UInt32 => {
                        dict_builder::<UInt32Type>(values)?
                    }
                    DataType::UInt64 => {
                        dict_builder::<UInt64Type>(values)?
                    }
                    _ => {
                        return Err(ArrowScalarError::InvalidProtobuf);
                    }
                }
            }
            table_list::Values::Union(_) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::to_array",
                    "Union",
                ));
            }
            table_list::Values::FixedSizeBinary(list) => {
                let mut builder = FixedSizeBinaryBuilder::new(list.size());
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(value).expect("We check this on push");
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            table_list::Values::FixedSizeList(_list) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::to_array",
                    "FixedSizeList",
                ));
            }
        };

        Ok(array)
    }

    pub fn data_type(&self) -> Result<DataType, ArrowScalarError> {
        match self.values.as_ref().unwrap() {
            table_list::Values::Boolean(_) => Ok(DataType::Boolean),
            table_list::Values::Int8(_) => Ok(DataType::Int8),
            table_list::Values::Int16(_) => Ok(DataType::Int16),
            table_list::Values::Int32(_) => Ok(DataType::Int32),
            table_list::Values::Int64(_) => Ok(DataType::Int64),
            table_list::Values::Uint8(_) => Ok(DataType::UInt8),
            table_list::Values::Uint16(_) => Ok(DataType::UInt16),
            table_list::Values::Uint32(_) => Ok(DataType::UInt32),
            table_list::Values::Uint64(_) => Ok(DataType::UInt64),
            table_list::Values::Float16(_) => Ok(DataType::Float16),
            table_list::Values::Float32(_) => Ok(DataType::Float32),
            table_list::Values::Float64(_) => Ok(DataType::Float64),
            table_list::Values::Utf8(_) => Ok(DataType::Utf8),
            table_list::Values::LargeUtf8(_) => Ok(DataType::LargeUtf8),
            table_list::Values::Date32(table_list::Int32List { values: _, set: _ }) => {
                Ok(DataType::Date32)
            }
            table_list::Values::Date64(table_list::Int64List { values: _, set: _ }) => {
                Ok(DataType::Date64)
            }
            table_list::Values::List(table_list::ListList {
                values: _,
                set: _,
                list_type,
                size: _,
            }) => {
                if let Some(list_type) = list_type {
                    Ok(DataType::List(Box::new(list_type.to_arrow()?)))
                } else {
                    Err(ArrowScalarError::InvalidProtobuf)
                }
            }
            table_list::Values::LargeList(table_list::ListList {
                values: _,
                set: _,
                list_type,
                size: _,
            }) => {
                if let Some(list_type) = list_type {
                    Ok(DataType::LargeList(Box::new(list_type.to_arrow()?)))
                } else {
                    Err(ArrowScalarError::InvalidProtobuf)
                }
            }
            table_list::Values::FixedSizeList(table_list::ListList {
                values: _,
                set: _,
                list_type,
                size,
            }) => {
                if let (Some(list_type), Some(size)) = (list_type, size) {
                    Ok(DataType::FixedSizeList(
                        Box::new(list_type.to_arrow()?),
                        *size,
                    ))
                } else {
                    Err(ArrowScalarError::InvalidProtobuf)
                }
            }
            table_list::Values::Binary(_) => Ok(DataType::Binary),
            table_list::Values::LargeBinary(_) => Ok(DataType::LargeBinary),
            table_list::Values::FixedSizeBinary(table_list::BinaryList {
                values: _,
                set: _,
                size,
            }) => {
                if let Some(size) = size {
                    Ok(DataType::FixedSizeBinary(*size))
                } else {
                    Err(ArrowScalarError::InvalidProtobuf)
                }
            }
            table_list::Values::Struct(table_list::StructList {
                fields,
                values: _,
                set: _,
            }) => {
                let fields = fields
                    .iter()
                    .map(|field| field.to_arrow())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Struct(fields))
            }
            table_list::Values::Union(table_list::UnionList { values: _, set: _ }) => Err(
                ArrowScalarError::Unimplemented("TableList::data_type", "Union"),
            ),
            table_list::Values::Dictionary(_dict) => Err(ArrowScalarError::Unimplemented(
                "TableList::data_type",
                "Union",
            )),
            table_list::Values::Time32Second(_) => Ok(DataType::Time32(TimeUnit::Second)),
            table_list::Values::Time32Millisecond(_) => Ok(DataType::Time32(TimeUnit::Millisecond)),
            table_list::Values::Time64Microsecond(_) => Ok(DataType::Time64(TimeUnit::Microsecond)),
            table_list::Values::Time64Nanosecond(_) => Ok(DataType::Time64(TimeUnit::Nanosecond)),
            table_list::Values::TimestampSecond(table_list::TimeList {
                times: _,
                tz,
                set: _,
            }) => Ok(DataType::Timestamp(TimeUnit::Second, tz.clone())),
            table_list::Values::TimestampMillisecond(table_list::TimeList {
                times: _,
                tz,
                set: _,
            }) => Ok(DataType::Timestamp(TimeUnit::Millisecond, tz.clone())),
            table_list::Values::TimestampMicrosecond(table_list::TimeList {
                times: _,
                tz,
                set: _,
            }) => Ok(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            table_list::Values::TimestampNanosecond(table_list::TimeList {
                times: _,
                tz,
                set: _,
            }) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
            table_list::Values::IntervalYearMonth(_) => {
                Ok(DataType::Interval(IntervalUnit::YearMonth))
            }
            table_list::Values::IntervalDayTime(_) => Ok(DataType::Interval(IntervalUnit::DayTime)),
            table_list::Values::DurationSecond(_) => Ok(DataType::Duration(TimeUnit::Second)),
            table_list::Values::DurationMillisecond(_) => {
                Ok(DataType::Duration(TimeUnit::Millisecond))
            }
            table_list::Values::DurationMicrosecond(_) => {
                Ok(DataType::Duration(TimeUnit::Microsecond))
            }
            table_list::Values::DurationNanosecond(_) => {
                Ok(DataType::Duration(TimeUnit::Nanosecond))
            }
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
                size: _,
            }) => values.len(),
            table_list::Values::LargeList(table_list::ListList {
                values,
                set: _,
                list_type: _,
                size: _,
            }) => values.len(),
            table_list::Values::FixedSizeList(table_list::ListList {
                values,
                set: _,
                list_type: _,
                size: _,
            }) => values.len(),
            table_list::Values::Binary(table_list::BinaryList {
                values,
                set: _,
                size: _,
            }) => values.len(),
            table_list::Values::LargeBinary(table_list::BinaryList {
                values,
                set: _,
                size: _,
            }) => values.len(),
            table_list::Values::FixedSizeBinary(table_list::BinaryList {
                values,
                set: _,
                size: _,
            }) => values.len(),
            table_list::Values::Struct(table_list::StructList {
                fields: _,
                values,
                set: _,
            }) => values.first().map(|arr| arr.len()).unwrap_or(0),
            table_list::Values::Union(table_list::UnionList { values, set: _ }) => values.len(),
            table_list::Values::Dictionary(dict) => {
                let table_list::DictionaryList {
                    values,
                    index_type: _,
                    set: _,
                } = dict.as_ref();
                values.as_ref().map(|a| a.len()).unwrap_or(0)
            }
            table_list::Values::Date32(table_list::Int32List { values, set: _ }) => values.len(),
            table_list::Values::Date64(table_list::Int64List { values, set: _ }) => values.len(),
            table_list::Values::Time32Second(table_list::Int32List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::Time32Millisecond(table_list::Int32List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::Time64Microsecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::Time64Nanosecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::TimestampSecond(table_list::TimeList {
                times,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::TimestampMillisecond(table_list::TimeList {
                times,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::TimestampMicrosecond(table_list::TimeList {
                times,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::TimestampNanosecond(table_list::TimeList {
                times,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::IntervalYearMonth(table_list::Int32List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::IntervalDayTime(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::DurationSecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::DurationMillisecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::DurationMicrosecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
            table_list::Values::DurationNanosecond(table_list::Int64List { values, set: _ }) => {
                values.len()
            }
        }
    }
}

impl table_list::ListList {
    pub fn push(&mut self, list: TableList) -> Result<(), TableList> {
        if list.values.is_none() {
            self.values.push(TableList::default());
            self.set.push(false);
        }
        if self.list_type.is_none() {
            return Err(list);
        }
        let list_type = self.list_type.as_ref().unwrap();
        if list_type.data_type.is_none() {
            return Err(list);
        }
        let list_data_type = list_type.to_arrow().expect("valid type");

        match (list_data_type.data_type(), list.values.unwrap()) {
            (DataType::Boolean, table_list::Values::Boolean(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Boolean(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Binary, table_list::Values::Binary(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Binary(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Int8, table_list::Values::Int8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Int16, table_list::Values::Int16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Int32, table_list::Values::Int32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Int64, table_list::Values::Int64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::UInt8, table_list::Values::Uint8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::UInt16, table_list::Values::Uint16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::UInt32, table_list::Values::Uint32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::UInt64, table_list::Values::Uint64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Float16, table_list::Values::Float16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Float32, table_list::Values::Float32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Float64, table_list::Values::Float64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Utf8, table_list::Values::Utf8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Utf8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::LargeUtf8, table_list::Values::LargeUtf8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::LargeUtf8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }

            (DataType::Date32, table_list::Values::Date32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Date32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Date64, table_list::Values::Date64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Date64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Time32(TimeUnit::Second), table_list::Values::Time32Second(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time32Second(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Time32(TimeUnit::Millisecond),
                table_list::Values::Time32Millisecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time32Second(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Time64(TimeUnit::Microsecond),
                table_list::Values::Time64Microsecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time64Microsecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Time64(TimeUnit::Nanosecond),
                table_list::Values::Time64Nanosecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time64Nanosecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Timestamp(TimeUnit::Second, _),
                table_list::Values::TimestampSecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::TimestampSecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Timestamp(TimeUnit::Millisecond, _),
                table_list::Values::TimestampMillisecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::TimestampMillisecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                table_list::Values::TimestampMicrosecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::TimestampMicrosecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Timestamp(TimeUnit::Nanosecond, _),
                table_list::Values::TimestampNanosecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::TimestampNanosecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Interval(IntervalUnit::YearMonth),
                table_list::Values::IntervalYearMonth(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::IntervalYearMonth(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Interval(IntervalUnit::DayTime),
                table_list::Values::IntervalDayTime(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::IntervalDayTime(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Duration(TimeUnit::Second), table_list::Values::DurationSecond(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::DurationSecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Duration(TimeUnit::Millisecond),
                table_list::Values::DurationMillisecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::DurationMillisecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Duration(TimeUnit::Microsecond),
                table_list::Values::DurationMicrosecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::DurationMicrosecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                DataType::Duration(TimeUnit::Nanosecond),
                table_list::Values::DurationNanosecond(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::DurationNanosecond(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::List(_), table_list::Values::List(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::List(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::LargeList(_), table_list::Values::LargeList(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::LargeList(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::FixedSizeList(_, _), table_list::Values::FixedSizeList(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::FixedSizeList(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Struct(_), table_list::Values::Struct(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Struct(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (DataType::Dictionary(_, _), table_list::Values::Dictionary(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Dictionary(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (_, list_vals) => {
                return Err(TableList {
                    values: Some(list_vals),
                });
            }
        }
        Ok(())
    }
}

macro_rules! small_primitive_list_ingestor {
    ($func_name:ident, $primitive_type:ty, $values_type:ident, $list_type:ident, $list_size: ident) => {
        fn $func_name(list: &table_list::ListList) -> ArrayRef {
            let primitive_list_builder = PrimitiveBuilder::<$primitive_type>::with_capacity(
                list.values.iter().map(|l| l.len()).sum(),
            );
            let mut list_builder =
                $list_size::with_capacity(primitive_list_builder, list.values.len());

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
    ($func_name:ident, $primitive_type:ty, $values_type:ident, $list_type:ident, $list_size: ident) => {
        fn $func_name(list: &table_list::ListList) -> ArrayRef {
            let primitive_list_builder = PrimitiveBuilder::<$primitive_type>::with_capacity(
                list.values.iter().map(|l| l.len()).sum(),
            );
            let mut list_builder =
                $list_size::with_capacity(primitive_list_builder, list.values.len());

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

fn string_list_list_builder(list: &table_list::ListList) -> ArrayRef {
    let mut list_builder = ListBuilder::new(StringBuilder::new());

    for list in list.values.iter() {
        if let Some(table_list::Values::Utf8(table_list::Utf8List { values, set })) =
            &list.values
        {
            let string_list_builder = list_builder.values();
            values.iter().zip(set.iter()).for_each(|(v, s)| {
                if *s {
                    string_list_builder.append_value(v);
                } else {
                    string_list_builder.append_null();
                }
            });
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }
    Arc::new(list_builder.finish())
}

fn string_large_list_list_builder(list: &table_list::ListList) -> ArrayRef {
    let mut list_builder = LargeListBuilder::new(StringBuilder::new());

    for list in list.values.iter() {
        if let Some(table_list::Values::Utf8(table_list::Utf8List { values, set })) =
            &list.values
        {
            let string_list_builder = list_builder.values();
            values.iter().zip(set.iter()).for_each(|(v, s)| {
                if *s {
                    string_list_builder.append_value(v);
                } else {
                    string_list_builder.append_null();
                }
            });
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }
    Arc::new(list_builder.finish())
}

fn primitive_large_list_list_builder_float16(list: &table_list::ListList) -> ArrayRef {
    let primitive_list_builder =
        PrimitiveBuilder::<Float16Type>::with_capacity(list.values.iter().map(|l| l.len()).sum());
    let mut list_builder =
        LargeListBuilder::with_capacity(primitive_list_builder, list.values.len());

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

small_primitive_list_ingestor!(
    primitive_list_list_builder_int8,
    Int8Type,
    Int8,
    Int8List,
    ListBuilder
);
small_primitive_list_ingestor!(
    primitive_list_list_builder_int16,
    Int16Type,
    Int16,
    Int16List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_int32,
    Int32Type,
    Int32,
    Int32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_int64,
    Int64Type,
    Int64,
    Int64List,
    ListBuilder
);
small_primitive_list_ingestor!(
    primitive_list_list_builder_uint8,
    UInt8Type,
    Uint8,
    UInt8List,
    ListBuilder
);
small_primitive_list_ingestor!(
    primitive_list_list_builder_uint16,
    UInt16Type,
    Uint16,
    UInt16List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_uint32,
    UInt32Type,
    Uint32,
    UInt32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_uint64,
    UInt64Type,
    Uint64,
    UInt64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_float32,
    Float32Type,
    Float32,
    Float32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_float64,
    Float64Type,
    Float64,
    Float64List,
    ListBuilder
);

primitive_list_ingestor!(
    primitive_list_list_builder_time32_second,
    Time32SecondType,
    Time32Second,
    Int32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_time32_millisecond,
    Time32MillisecondType,
    Time32Millisecond,
    Int32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_time64_microsecond,
    Time64MicrosecondType,
    Time64Microsecond,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_time64_nanosecond,
    Time64NanosecondType,
    Time64Nanosecond,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_date32,
    Date32Type,
    Date32,
    Int32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_date64,
    Date64Type,
    Date64,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_interval_year_month,
    IntervalYearMonthType,
    IntervalYearMonth,
    Int32List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_interval_day_time,
    IntervalDayTimeType,
    IntervalDayTime,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_duration_second,
    DurationSecondType,
    DurationSecond,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_duration_millisecond,
    DurationMillisecondType,
    DurationMillisecond,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_duration_microsecond,
    DurationMicrosecondType,
    DurationMicrosecond,
    Int64List,
    ListBuilder
);
primitive_list_ingestor!(
    primitive_list_list_builder_duration_nanosecond,
    DurationNanosecondType,
    DurationNanosecond,
    Int64List,
    ListBuilder
);

small_primitive_list_ingestor!(
    primitive_large_list_list_builder_int8,
    Int8Type,
    Int8,
    Int8List,
    LargeListBuilder
);
small_primitive_list_ingestor!(
    primitive_large_list_list_builder_int16,
    Int16Type,
    Int16,
    Int16List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_int32,
    Int32Type,
    Int32,
    Int32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_int64,
    Int64Type,
    Int64,
    Int64List,
    LargeListBuilder
);
small_primitive_list_ingestor!(
    primitive_large_list_list_builder_uint8,
    UInt8Type,
    Uint8,
    UInt8List,
    LargeListBuilder
);
small_primitive_list_ingestor!(
    primitive_large_list_list_builder_uint16,
    UInt16Type,
    Uint16,
    UInt16List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_uint32,
    UInt32Type,
    Uint32,
    UInt32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_uint64,
    UInt64Type,
    Uint64,
    UInt64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_float32,
    Float32Type,
    Float32,
    Float32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_float64,
    Float64Type,
    Float64,
    Float64List,
    LargeListBuilder
);

primitive_list_ingestor!(
    primitive_large_list_list_builder_time32_second,
    Time32SecondType,
    Time32Second,
    Int32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_time32_millisecond,
    Time32MillisecondType,
    Time32Millisecond,
    Int32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_time64_microsecond,
    Time64MicrosecondType,
    Time64Microsecond,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_time64_nanosecond,
    Time64NanosecondType,
    Time64Nanosecond,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_date32,
    Date32Type,
    Date32,
    Int32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_date64,
    Date64Type,
    Date64,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_interval_year_month,
    IntervalYearMonthType,
    IntervalYearMonth,
    Int32List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_interval_day_time,
    IntervalDayTimeType,
    IntervalDayTime,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_duration_second,
    DurationSecondType,
    DurationSecond,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_duration_millisecond,
    DurationMillisecondType,
    DurationMillisecond,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_duration_microsecond,
    DurationMicrosecondType,
    DurationMicrosecond,
    Int64List,
    LargeListBuilder
);
primitive_list_ingestor!(
    primitive_large_list_list_builder_duration_nanosecond,
    DurationNanosecondType,
    DurationNanosecond,
    Int64List,
    LargeListBuilder
);

#[cfg(test)]
pub mod tests {
    use std::ops::Deref;

    use super::*;
    use crate::table_list::{Float16List, ListList};

    macro_rules! primitive_list_test {
        ($func_name:ident, $prim_type:expr, $array_type:ty, $values_type:ident, $list_type:ident, $values:expr, $intended_values:expr, $set:expr) => {
            #[test]
            fn $func_name() {
                let values = $values;
                let array = Arc::new(<$array_type>::from(values));
                let list = array.clone_as_list().unwrap();
                let intended_list = TableList {
                    values: Some(table_list::Values::$values_type(table_list::$list_type {
                        values: $intended_values,
                        set: $set,
                    })),
                };
                assert_eq!(intended_list, list);
                assert_eq!($prim_type(&list.to_array().unwrap()), array.deref());
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
                    as_primitive_array::<$prim_type>(&list.to_array().unwrap()),
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
        let list = array.clone_as_list().unwrap();
        let intended_list = TableList {
            values: Some(table_list::Values::Float16(Float16List {
                values: vec![1.0, 2.0, 0.0, 3.0, 4.0],
                set: vec![true, true, false, true, true],
            })),
        };
        assert_eq!(intended_list, list);
        assert_eq!(
            as_primitive_array::<Float16Type>(&list.to_array().unwrap()),
            &array
        );
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
        let list = array.clone_as_list().unwrap();
        let intended_list = TableList {
            values: Some(table_list::Values::Float16(Float16List {
                values: vec![1.0, 2.0, 5.0, 3.0, 4.0],
                set: vec![true, true, true, true, true],
            })),
        };
        assert_eq!(intended_list, list);
        assert_eq!(
            as_primitive_array::<Float16Type>(&list.to_array().unwrap()),
            &array
        );
    }
    #[test]
    fn test_primitive_list_list_test() {
        let list = TableList {
            values: Some(table_list::Values::Float32(table_list::Float32List {
                values: vec![1.0, 2.0, 5.0, 3.0, 4.0],
                set: vec![true, true, true, true, true],
            })),
        };
        let intended_list = TableList {
            values: Some(table_list::Values::List(ListList {
                values: vec![
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list,
                ],
                set: vec![true, true, true, true, true],
                list_type: Some(FieldProto {
                    name: "item".to_string(),
                    data_type: Some(Box::new(DataTypeProto::from_arrow(&DataType::Float32))),
                    nullable: true,
                }),
                size: None,
            })),
        };
        let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Float32Type>::new());
        for _ in 0..5 {
            list_builder
                .values()
                .append_slice(&[1.0, 2.0, 5.0, 3.0, 4.0]);
            list_builder.append(true);
        }

        let array = list_builder.finish();
        let list = array.clone_as_list().unwrap();

        assert_eq!(intended_list, list);
        assert_eq!(as_list_array(&list.to_array().unwrap()), &array);
    }

    #[test]
    fn test_primitive_large_list_list_test() {
        let list = TableList {
            values: Some(table_list::Values::Float32(table_list::Float32List {
                values: vec![1.0, 2.0, 5.0, 3.0, 4.0],
                set: vec![true, true, true, true, true],
            })),
        };
        let intended_list = TableList {
            values: Some(table_list::Values::LargeList(ListList {
                values: vec![
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list,
                ],
                set: vec![true, true, true, true, true],
                list_type: Some(FieldProto {
                    name: "item".to_string(),
                    data_type: Some(Box::new(DataTypeProto::from_arrow(&DataType::Float32))),
                    nullable: true,
                }),
                size: None,
            })),
        };
        let mut list_builder = LargeListBuilder::new(PrimitiveBuilder::<Float32Type>::new());
        for _ in 0..5 {
            list_builder
                .values()
                .append_slice(&[1.0, 2.0, 5.0, 3.0, 4.0]);
            list_builder.append(true);
        }

        let array = list_builder.finish();
        let list = array.clone_as_list().unwrap();

        assert_eq!(intended_list, list);
        assert_eq!(as_large_list_array(&list.to_array().unwrap()), &array);
    }

    #[test]
    fn test_string_list_list_test() {
        let values = vec!["one".to_string(), "2.0".to_string(), "5.0".to_string(), "".to_string(), "redde".to_string()];
        let list = TableList {
            values: Some(table_list::Values::Utf8(table_list::Utf8List {
                values: values.clone(),
                set: vec![true, true, true, true, true],
            })),
        };
        let intended_list = TableList {
            values: Some(table_list::Values::List(ListList {
                values: vec![
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list.clone(),
                    list,
                ],
                set: vec![true, true, true, true, true],
                list_type: Some(FieldProto {
                    name: "item".to_string(),
                    data_type: Some(Box::new(DataTypeProto::from_arrow(&DataType::Utf8))),
                    nullable: true,
                }),
                size: None,
            })),
        };
        let mut list_builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..5 {

            let string_builder = list_builder
                .values();
            for value in values.iter() {
                string_builder.append_value(value);
            }
            list_builder.append(true);
        }

        let array = list_builder.finish();
        let list = array.clone_as_list().unwrap();

        assert_eq!(intended_list, list);
        assert_eq!(as_list_array(&list.to_array().unwrap()), &array);
    }
}
