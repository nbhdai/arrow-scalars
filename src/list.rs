use std::collections::HashMap;
use std::sync::Arc;

use crate::table_scalar::{Duration, Time};
use crate::{data_type_proto, table_list, table_scalar, ArrowScalarError, TableList, TableScalar};
use crate::{DataTypeProto, ScalarValuable};
use arrow::array::*;
use arrow::datatypes::*;
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
                let list_type = DataTypeProto::from_arrow(list_type.data_type());
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
                    list_type: list_type.into(),
                    size: None,
                }))
            }
            DataType::LargeList(list_type) => {
                let list_type = DataTypeProto::from_arrow(list_type.data_type());
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
                Some(table_list::Values::LargeList(table_list::ListList {
                    values,
                    set,
                    list_type: list_type.into(),
                    size: None,
                }))
            }
            DataType::FixedSizeList(list_type, len) => {
                let list_type = DataTypeProto::from_arrow(list_type.data_type());
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
                Some(table_list::Values::FixedSizeList(table_list::ListList {
                    values,
                    set,
                    list_type: list_type.into(),
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Second.into(),
                        tz,
                        times,
                        set,
                    };
                    Some(table_list::Values::Timestamp(time_list))
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Millisecond.into(),
                        tz,
                        times,
                        set,
                    };
                    Some(table_list::Values::Timestamp(time_list))
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Microsecond.into(),
                        tz,
                        times,
                        set,
                    };
                    Some(table_list::Values::Timestamp(time_list))
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Nanosecond.into(),
                        tz,
                        times,
                        set,
                    };
                    Some(table_list::Values::Timestamp(time_list))
                }
            },
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<Time32SecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i) as i64);
                        } else {
                            times.push(Time32SecondType::default_value() as i64);
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Second.into(),
                        tz: None,
                        times,
                        set,
                    };
                    Some(table_list::Values::Time32(time_list))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<Time32MillisecondType>(self);
                    let mut times = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            times.push(array.value(i) as i64);
                        } else {
                            times.push(Time32MillisecondType::default_value() as i64);
                        }
                        set.push(!array.is_null(i));
                    }
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Millisecond.into(),
                        tz: None,
                        times,
                        set,
                    };
                    Some(table_list::Values::Time32(time_list))
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Microsecond.into(),
                        tz: None,
                        times,
                        set,
                    };
                    Some(table_list::Values::Time64(time_list))
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
                    let time_list = table_list::TimeList {
                        unit: data_type_proto::TimeUnit::Nanosecond.into(),
                        tz: None,
                        times,
                        set,
                    };
                    Some(table_list::Values::Time64(time_list))
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
                    let time_list = table_list::DurationList {
                        unit: data_type_proto::TimeUnit::Second.into(),
                        durations,
                        set,
                    };
                    Some(table_list::Values::Duration(time_list))
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
                    let time_list = table_list::DurationList {
                        unit: data_type_proto::TimeUnit::Millisecond.into(),
                        durations,
                        set,
                    };
                    Some(table_list::Values::Duration(time_list))
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
                    let time_list = table_list::DurationList {
                        unit: data_type_proto::TimeUnit::Microsecond.into(),
                        durations,
                        set,
                    };
                    Some(table_list::Values::Duration(time_list))
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
                    let time_list = table_list::DurationList {
                        unit: data_type_proto::TimeUnit::Nanosecond.into(),
                        durations,
                        set,
                    };
                    Some(table_list::Values::Duration(time_list))
                }
            },
            DataType::Interval(interval_unit) => match interval_unit {
                IntervalUnit::YearMonth => {
                    let array = as_primitive_array::<IntervalYearMonthType>(self);
                    let mut intervals = Vec::with_capacity(array.len());
                    let mut set = Vec::with_capacity(array.len());
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            intervals.push(array.value(i) as i64);
                        } else {
                            intervals.push(IntervalYearMonthType::default_value() as i64);
                        }
                        set.push(!array.is_null(i));
                    }
                    let interval_list = table_list::IntervalList {
                        unit: data_type_proto::IntervalUnit::YearMonth.into(),
                        intervals,
                        set,
                    };
                    Some(table_list::Values::Interval(interval_list))
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
                    let interval_list = table_list::IntervalList {
                        unit: data_type_proto::IntervalUnit::DayTime.into(),
                        intervals,
                        set,
                    };
                    Some(table_list::Values::Interval(interval_list))
                }
                IntervalUnit::MonthDayNano => {
                    return Err(ArrowScalarError::Unimplemented(
                        "clone_as_list".to_string(),
                        "IntervalUnit::MonthDayNano".to_string(),
                    ));
                }
            },
            DataType::Struct(fields) => {
                let array = as_struct_array(self);
                let mut values = HashMap::with_capacity(array.len());
                let set = (0..array.len()).map(|i| array.is_valid(i)).collect();
                for (i, field) in fields.iter().enumerate() {
                    let field_name = field.name().to_string();
                    let field_array = array.column(i);
                    let field_values = field_array.clone_as_list()?;
                    values.insert(field_name, field_values);
                }
                let struct_list = table_list::StructList { values, set };
                Some(table_list::Values::Struct(struct_list))
            }
            DataType::Union(_fields, type_ids, mode) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list".to_string(),
                    format!("Union({:?}, {:?})", type_ids, mode),
                ));
            }
            DataType::Dictionary(key_type, value_type) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list".to_string(),
                    format!("Dictionary({:?}, {:?})", key_type, value_type),
                ));
            }
            DataType::Decimal128(precision, scale) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list".to_string(),
                    format!("Decimal128({:?}, {:?})", precision, scale),
                ));
            }
            DataType::Decimal256(precision, scale) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list".to_string(),
                    format!("Decimal256({:?}, {:?})", precision, scale),
                ));
            }
            DataType::Map(key_type, value_type) => {
                return Err(ArrowScalarError::Unimplemented(
                    "clone_as_list".to_string(),
                    format!("Map({:?}, {:?})", key_type, value_type),
                ));
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
            Some(table_list::Values::Timestamp(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time { unit, time, tz };
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Timestamp(time)),
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
            Some(table_list::Values::Time32(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time { unit, time, tz };
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time32(time)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Time64(list)) => {
                let unit = list.unit;
                let time = list.times[i];
                let tz = list.tz.clone();
                let time = Time { unit, time, tz };
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Time64(time)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Interval(list)) => {
                if list.set[i] {
                    let interval = list.intervals[i];
                    let interval = Some(match list.unit() {
                        data_type_proto::IntervalUnit::YearMonth => {
                            table_scalar::interval::Interval::YearMonth(interval as i32)
                        }
                        data_type_proto::IntervalUnit::DayTime => {
                            table_scalar::interval::Interval::DayTime(interval)
                        }
                        data_type_proto::IntervalUnit::MonthDayNano => {
                            return Err(ArrowScalarError::Unimplemented(
                                "TableList::scalar".to_string(),
                                "MonthDayNano".to_string(),
                            ))
                        }
                    });
                    let interval = table_scalar::Interval { interval };
                    TableScalar {
                        value: Some(table_scalar::Value::Interval(interval)),
                    }
                } else {
                    TableScalar { value: None }
                }
            }
            Some(table_list::Values::Struct(list)) => {
                if list.set[i] {
                    let elements: Result<HashMap<String, TableScalar>, ArrowScalarError> = list
                        .values
                        .iter()
                        .map(|(field, value)| {
                            let value = value.scalar(i)?;
                            Ok((field.clone(), value))
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
            Some(table_list::Values::Duration(list)) => {
                let unit = list.unit;
                let duration = list.durations[i];
                let duration = Duration { unit, duration };
                if list.set[i] {
                    TableScalar {
                        value: Some(table_scalar::Value::Duration(duration)),
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
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Timestamp(b)),
                    });
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
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Time32(b)),
                    });
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
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Time64(b)),
                    });
                }
                times.push(b.time);
                set.push(true);
            }
            (table_list::Values::List(values), table_scalar::Value::List(list)) => {
                return values.push(list);
            }
            (table_list::Values::LargeList(values), table_scalar::Value::List(list)) => {
                return values.push(list);
            }
            (table_list::Values::FixedSizeList(values), table_scalar::Value::List(list)) => {
                return values.push(list);
            }
            (table_list::Values::Struct(struct_list), table_scalar::Value::Struct(mut values)) => {
                if struct_list.values.len() == values.elements.len() && struct_list.values.keys().all(|k| values.elements.contains_key(k)) {
                    for (field, value) in struct_list.values.iter_mut() {
                        value.push(values.elements.remove(field).unwrap())?;
                    }
                } else {
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Struct(values)),
                    });
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
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Binary(b)),
                    });
                }
                values.values.push(b);
                values.set.push(true);
            }
            (table_list::Values::Duration(values), table_scalar::Value::Duration(b)) => {
                if values.unit != b.unit {
                    return Err(TableScalar {
                        value: Some(table_scalar::Value::Duration(b)),
                    });
                }
                values.durations.push(b.duration);
                values.set.push(true);
            }
            (table_list::Values::Interval(values), table_scalar::Value::Interval(b)) => {
                match (values.unit(), b.interval.as_ref().unwrap()) {
                    (data_type_proto::IntervalUnit::YearMonth, table_scalar::interval::Interval::YearMonth(i)) => {
                        values.intervals.push(*i as i64);
                        values.set.push(true);
                    }
                    (data_type_proto::IntervalUnit::DayTime, table_scalar::interval::Interval::DayTime(i)) => {
                        values.intervals.push(*i);
                        values.set.push(true);
                    }
                    _ => {
                        return Err(TableScalar {
                            value: Some(table_scalar::Value::Interval(b)),
                        });
                    }
                }
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
            (table_list::Values::Union(values), table_scalar::Value::Union(union)) => {
                values.values.push(*union);
                values.set.push(true);
            }
            (table_list::Values::Dictionary(_values), table_scalar::Value::Dictionary(dict)) => {
                return Err(TableScalar {
                    value: Some(table_scalar::Value::Dictionary(dict)),
                });
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
                    size: _,
                }) => {
                    values.push(TableList::default());
                    set.push(false);
                }
                _ => {}
            }
        }
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
                let list_type = list_list.list_type.as_ref().unwrap();
                let list_data_type = list_type.data_type.as_ref();
                match list_data_type {
                    Some(data_type_proto::DataType::Int8(_)) => {
                        primitive_list_list_builder_int8(list_list)
                    }
                    Some(data_type_proto::DataType::Int16(_)) => {
                        primitive_list_list_builder_int16(list_list)
                    }
                    Some(data_type_proto::DataType::Int32(_)) => {
                        primitive_list_list_builder_int32(list_list)
                    }
                    Some(data_type_proto::DataType::Int64(_)) => {
                        primitive_list_list_builder_int64(list_list)
                    }
                    Some(data_type_proto::DataType::Uint8(_)) => {
                        primitive_list_list_builder_uint8(list_list)
                    }
                    Some(data_type_proto::DataType::Uint16(_)) => {
                        primitive_list_list_builder_uint16(list_list)
                    }
                    Some(data_type_proto::DataType::Uint32(_)) => {
                        primitive_list_list_builder_uint32(list_list)
                    }
                    Some(data_type_proto::DataType::Uint64(_)) => {
                        primitive_list_list_builder_uint64(list_list)
                    }
                    Some(data_type_proto::DataType::Float16(_)) => {
                        primitive_list_list_builder_float16(list_list)
                    }
                    Some(data_type_proto::DataType::Float32(_)) => {
                        primitive_list_list_builder_float32(list_list)
                    }
                    Some(data_type_proto::DataType::Float64(_)) => {
                        primitive_list_list_builder_float64(list_list)
                    }
                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array".to_string(),
                            format!("{:?}", list_data_type),
                        ))
                    }
                }
            }
            table_list::Values::LargeList(list_list) => {
                let list_type = list_list.list_type.as_ref().unwrap();
                let list_data_type = list_type.data_type.as_ref();
                match list_data_type {
                    Some(data_type_proto::DataType::Int8(_)) => {
                        primitive_list_list_builder_int8(list_list)
                    }
                    Some(data_type_proto::DataType::Int16(_)) => {
                        primitive_list_list_builder_int16(list_list)
                    }
                    Some(data_type_proto::DataType::Int32(_)) => {
                        primitive_list_list_builder_int32(list_list)
                    }
                    Some(data_type_proto::DataType::Int64(_)) => {
                        primitive_list_list_builder_int64(list_list)
                    }
                    Some(data_type_proto::DataType::Uint8(_)) => {
                        primitive_list_list_builder_uint8(list_list)
                    }
                    Some(data_type_proto::DataType::Uint16(_)) => {
                        primitive_list_list_builder_uint16(list_list)
                    }
                    Some(data_type_proto::DataType::Uint32(_)) => {
                        primitive_list_list_builder_uint32(list_list)
                    }
                    Some(data_type_proto::DataType::Uint64(_)) => {
                        primitive_list_list_builder_uint64(list_list)
                    }
                    Some(data_type_proto::DataType::Float16(_)) => {
                        primitive_list_list_builder_float16(list_list)
                    }
                    Some(data_type_proto::DataType::Float32(_)) => {
                        primitive_list_list_builder_float32(list_list)
                    }
                    Some(data_type_proto::DataType::Float64(_)) => {
                        primitive_list_list_builder_float64(list_list)
                    }
                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array".to_string(),
                            format!("{:?}", list_data_type),
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
                    .values
                    .iter()
                    .map(|(name, list)| {
                        Ok((Field::new(name, list.data_type(), false), list.to_array()?))
                    })
                    .collect::<Result<Vec<_>, ArrowScalarError>>()?;
                Arc::new(StructArray::from(arrays))
            }
            table_list::Values::Timestamp(list) => {
                match list.unit() {
                    data_type_proto::TimeUnit::Second => {
                        let mut builder = TimestampSecondBuilder::new();
                        for (i, value) in list.times.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    data_type_proto::TimeUnit::Millisecond => {
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
                    data_type_proto::TimeUnit::Microsecond => {
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
                    data_type_proto::TimeUnit::Nanosecond => {
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
                }
            }
            table_list::Values::Time32(list) => {
                match list.unit() {
                    data_type_proto::TimeUnit::Second => {
                        let mut builder = Time32SecondBuilder::new();
                        for (i, value) in list.times.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i32);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    data_type_proto::TimeUnit::Millisecond => {
                        let mut builder = Time32MillisecondBuilder::new();
                        for (i, value) in list.times.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i32);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array".to_string(),
                            format!("{:?}", list.unit()),
                        ))
                    }
                }
            }
            table_list::Values::Time64(list) => {
                match list.unit() {
                    data_type_proto::TimeUnit::Microsecond => {
                        let mut builder = Time64MicrosecondBuilder::new();
                        for (i, value) in list.times.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    data_type_proto::TimeUnit::Nanosecond => {
                        let mut builder = Time64NanosecondBuilder::new();
                        for (i, value) in list.times.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array".to_string(),
                            format!("{:?}", list.unit()),
                        ))
                    }
                }
            }
            table_list::Values::Date32(list) => {
                let mut builder = Date32Builder::new();
                for (i, value) in list.values.iter().enumerate() {
                    if list.set[i] {
                        builder.append_value(*value as i32);
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
            table_list::Values::Duration(list) => {
                match list.unit() {
                    data_type_proto::TimeUnit::Second => {
                        let mut builder = DurationSecondBuilder::new();
                        for (i, value) in list.durations.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    data_type_proto::TimeUnit::Millisecond => {
                        let mut builder = DurationMillisecondBuilder::new();
                        for (i, value) in list.durations.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    data_type_proto::TimeUnit::Microsecond => {
                        let mut builder = DurationMicrosecondBuilder::new();
                        for (i, value) in list.durations.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    data_type_proto::TimeUnit::Nanosecond => {
                        let mut builder = DurationNanosecondBuilder::new();
                        for (i, value) in list.durations.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                }
            }
            table_list::Values::Interval(list) => {
                match list.unit() {
                    data_type_proto::IntervalUnit::YearMonth => {
                        let mut builder = IntervalYearMonthBuilder::new();
                        for (i, value) in list.intervals.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i32);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    data_type_proto::IntervalUnit::DayTime => {
                        let mut builder = IntervalDayTimeBuilder::new();
                        for (i, value) in list.intervals.iter().enumerate() {
                            if list.set[i] {
                                builder.append_value(*value as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    data_type_proto::IntervalUnit::MonthDayNano => {
                        return Err(ArrowScalarError::Unimplemented(
                            "TableList::to_array".to_string(),
                            format!("{:?}", list.unit()),
                        ));
                    }
                }
            }
            table_list::Values::Dictionary(_) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::to_array".to_string(),
                    "Dictionary".to_string(),
                ));
            }
            table_list::Values::Union(_) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableList::to_array".to_string(),
                    "Union".to_string(),
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
                    "TableList::to_array".to_string(),
                    "FixedSizeList".to_string(),
                ));
            }
        };

        Ok(array)
    }

    pub fn data_type(&self) -> DataType {
        todo!()
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
            table_list::Values::Struct(table_list::StructList { values, set: _ }) => {
                values.iter().next().map(|(_, arr)| arr.len()).unwrap_or(0)
            }
            table_list::Values::Union(table_list::UnionList { values, set: _ }) => values.len(),
            table_list::Values::Dictionary(dict) => {
                let table_list::DictionaryList {
                    values,
                    index_type: _,
                    set: _,
                } = dict.as_ref();
                values.as_ref().map(|a| a.len()).unwrap_or(0)
            }
            table_list::Values::Time32(table_list::TimeList {
                times,
                unit: _,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::Time64(table_list::TimeList {
                times,
                unit: _,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::Timestamp(table_list::TimeList {
                times,
                unit: _,
                tz: _,
                set: _,
            }) => times.len(),
            table_list::Values::Date32(table_list::Int32List { values, set: _ }) => values.len(),
            table_list::Values::Date64(table_list::Int64List { values, set: _ }) => values.len(),
            table_list::Values::Interval(table_list::IntervalList {
                intervals,
                unit: _,
                set: _,
            }) => intervals.len(),
            table_list::Values::Duration(table_list::DurationList {
                durations,
                unit: _,
                set: _,
            }) => durations.len(),
        }
    }
}

impl table_list::ListList {
    pub fn push(&mut self, list: TableList) -> Result<(), TableScalar> {
        if list.values.is_none() {
            self.values.push(TableList::default());
            self.set.push(false);
        }
        let list_type = self.list_type.as_ref().unwrap();
        let list_data_type = list_type.data_type.as_ref();
        match (list_data_type, list.values.unwrap()) {
            (Some(data_type_proto::DataType::Bool(_)), table_list::Values::Boolean(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Boolean(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Binary(_)), table_list::Values::Binary(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Binary(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Int8(_)), table_list::Values::Int8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Int16(_)), table_list::Values::Int16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Int32(_)), table_list::Values::Int32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Int64(_)), table_list::Values::Int64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Int64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Uint8(_)), table_list::Values::Uint8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Uint16(_)), table_list::Values::Uint16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Uint32(_)), table_list::Values::Uint32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Uint64(_)), table_list::Values::Uint64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Uint64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Float16(_)), table_list::Values::Float16(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float16(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Float32(_)), table_list::Values::Float32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Float64(_)), table_list::Values::Float64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Float64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Utf8(_)), table_list::Values::Utf8(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Utf8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                Some(data_type_proto::DataType::LargeUtf8(_)),
                table_list::Values::LargeUtf8(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::LargeUtf8(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (
                Some(data_type_proto::DataType::Timestamp(_)),
                table_list::Values::Timestamp(value),
            ) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Timestamp(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Date32(_)), table_list::Values::Date32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Date32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Date64(_)), table_list::Values::Date64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Date64(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Time32(_)), table_list::Values::Time32(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (Some(data_type_proto::DataType::Time64(_)), table_list::Values::Time64(value)) => {
                let rebuilt_list = TableList {
                    values: Some(table_list::Values::Time32(value)),
                };
                self.values.push(rebuilt_list);
                self.set.push(true);
            }
            (_, list_vals) => {
                return Err(TableScalar {
                    value: Some(table_scalar::Value::List(TableList {
                        values: Some(list_vals),
                    })),
                });
            }
        }
        Ok(())
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
}
