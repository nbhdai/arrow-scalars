use crate::list::ListValuable;
use crate::{table_scalar, ArrowScalarError, TableList, TableScalar};
use arrow::array::*;
use arrow::datatypes::*;
use half::f16;
use std::collections::HashMap;
use std::ops::Deref;

pub trait ScalarValuable {
    fn scalar(&self, i: usize) -> Result<TableScalar, ArrowScalarError>;
}

impl<T: Array> ScalarValuable for T {
    fn scalar(&self, i: usize) -> Result<TableScalar, ArrowScalarError> {
        if self.is_null(i) {
            return Ok(TableScalar { value: None });
        }
        let value = match self.data_type() {
            DataType::Null => unreachable!(),
            DataType::Int8 => {
                let array = as_primitive_array::<Int8Type>(self);
                Some(table_scalar::Value::Int8(array.value(i).into()))
            }
            DataType::Int16 => {
                let array = as_primitive_array::<Int16Type>(self);
                Some(table_scalar::Value::Int16(array.value(i).into()))
            }
            DataType::Int32 => {
                let array = as_primitive_array::<Int32Type>(self);
                Some(table_scalar::Value::Int32(array.value(i)))
            }
            DataType::Int64 => {
                let array = as_primitive_array::<Int64Type>(self);
                Some(table_scalar::Value::Int64(array.value(i)))
            }
            DataType::UInt8 => {
                let array = as_primitive_array::<UInt8Type>(self);
                Some(table_scalar::Value::Uint8(array.value(i).into()))
            }
            DataType::UInt16 => {
                let array = as_primitive_array::<UInt16Type>(self);
                Some(table_scalar::Value::Uint16(array.value(i).into()))
            }
            DataType::UInt32 => {
                let array = as_primitive_array::<UInt32Type>(self);
                Some(table_scalar::Value::Uint32(array.value(i)))
            }
            DataType::UInt64 => {
                let array = as_primitive_array::<UInt64Type>(self);
                Some(table_scalar::Value::Uint64(array.value(i)))
            }
            DataType::Float16 => {
                let array = as_primitive_array::<Float16Type>(self);
                Some(table_scalar::Value::Float16(array.value(i).into()))
            }
            DataType::Float32 => {
                let array = as_primitive_array::<Float32Type>(self);
                Some(table_scalar::Value::Float32(array.value(i)))
            }
            DataType::Float64 => {
                let array = as_primitive_array::<Float64Type>(self);
                Some(table_scalar::Value::Float64(array.value(i)))
            }
            DataType::Date32 => {
                let array = as_primitive_array::<Date32Type>(self);
                Some(table_scalar::Value::Date32(array.value(i)))
            }
            DataType::Date64 => {
                let array = as_primitive_array::<Date64Type>(self);
                Some(table_scalar::Value::Date64(array.value(i)))
            }
            // Simple non-primitive arrays
            DataType::Boolean => {
                let array = as_boolean_array(self);
                Some(table_scalar::Value::Boolean(array.value(i)))
            }
            DataType::Binary => {
                let array = as_generic_binary_array::<i32>(self);
                Some(table_scalar::Value::Binary(array.value(i).into()))
            }
            DataType::LargeBinary => {
                let array = as_generic_binary_array::<i64>(self);
                Some(table_scalar::Value::LargeBinary(array.value(i).into()))
            }
            DataType::FixedSizeBinary(_) => {
                let array = self
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("Just checked it has this type.");
                Some(table_scalar::Value::Binary(array.value(i).into()))
            }
            DataType::Utf8 => {
                let array = as_string_array(self);
                Some(table_scalar::Value::Utf8(array.value(i).into()))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(self);
                Some(table_scalar::Value::LargeUtf8(array.value(i).into()))
            }
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<Time32SecondType>(self);
                    Some(table_scalar::Value::Time32Second(array.value(i)))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<Time32MillisecondType>(self);
                    Some(table_scalar::Value::Time32Millisecond(
                        array.value(i),
                    ))
                }
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<Time64MicrosecondType>(self);
                    Some(table_scalar::Value::Time64Microsecond(
                        array.value(i),
                    ))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<Time64NanosecondType>(self);
                    Some(table_scalar::Value::Time64Nanosecond(array.value(i)))
                }
                _ => unreachable!(),
            },
            DataType::Timestamp(unit, _tz) => match unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<TimestampSecondType>(self);
                    Some(table_scalar::Value::TimestampSecond(array.value(i)))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<TimestampMillisecondType>(self);
                    Some(table_scalar::Value::TimestampMillisecond(
                        array.value(i),
                    ))
                }
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<TimestampMicrosecondType>(self);
                    Some(table_scalar::Value::TimestampMicrosecond(
                        array.value(i),
                    ))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<TimestampNanosecondType>(self);
                    Some(table_scalar::Value::TimestampNanosecond(
                        array.value(i),
                    ))
                }
            },
            DataType::Interval(interval) => {
                let value = match interval {
                    IntervalUnit::YearMonth => table_scalar::Value::IntervalYearMonth(
                        as_primitive_array::<IntervalYearMonthType>(self).value(i),
                    ),
                    IntervalUnit::DayTime => table_scalar::Value::IntervalDayTime(
                        as_primitive_array::<IntervalDayTimeType>(self).value(i),
                    ),
                    IntervalUnit::MonthDayNano => {
                        return Err(ArrowScalarError::Unimplemented(
                            "Array::scalar",
                            "IntervalMonthDayNano",
                        ))
                    }
                };
                Some(value)
            }
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => {
                    let array = as_primitive_array::<DurationSecondType>(self);
                    Some(table_scalar::Value::DurationSecond(array.value(i)))
                }
                TimeUnit::Millisecond => {
                    let array = as_primitive_array::<DurationMillisecondType>(self);
                    Some(table_scalar::Value::DurationMillisecond(array.value(i)))
                }
                TimeUnit::Microsecond => {
                    let array = as_primitive_array::<DurationMicrosecondType>(self);
                    Some(table_scalar::Value::DurationMicrosecond(array.value(i)))
                }
                TimeUnit::Nanosecond => {
                    let array = as_primitive_array::<DurationNanosecondType>(self);
                    Some(table_scalar::Value::DurationNanosecond(array.value(i)))
                }
            },
            DataType::Struct(_) => {
                let arrays = as_struct_array(self);
                let elements: Result<HashMap<String, TableScalar>, ArrowScalarError> = arrays
                    .columns()
                    .iter()
                    .zip(arrays.column_names())
                    .map(|(arr, str)| Ok((str.to_string(), arr.scalar(i)?)))
                    .collect();
                let value = table_scalar::Struct {
                    elements: elements?,
                };
                Some(table_scalar::Value::Struct(value))
            }
            DataType::Dictionary(key_type, _) => {
                let value = match key_type.deref() {
                    DataType::Int8 => {
                        let array = as_dictionary_array::<Int8Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::Int16 => {
                        let array = as_dictionary_array::<Int16Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::Int32 => {
                        let array = as_dictionary_array::<Int32Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::Int64 => {
                        let array = as_dictionary_array::<Int64Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::UInt8 => {
                        let array = as_dictionary_array::<UInt8Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::UInt16 => {
                        let array = as_dictionary_array::<UInt16Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::UInt32 => {
                        let array = as_dictionary_array::<UInt32Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    DataType::UInt64 => {
                        let array = as_dictionary_array::<UInt64Type>(self);
                        let index = array.keys().value(i).to_usize().unwrap();
                        array.values().scalar(index)
                    }
                    _ => unreachable!(),
                };

                Some(table_scalar::Value::Dictionary(Box::new(value?)))
            }
            DataType::List(_) => {
                let array = self
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Just checked it has this type.");
                let value = array.value(i).clone_as_list()?;
                Some(table_scalar::Value::List(value))
            }
            DataType::LargeList(_) => {
                let array = self
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .expect("Just checked it has this type.");
                let value = array.value(i).clone_as_list()?;
                Some(table_scalar::Value::LargeList(value))
            }
            DataType::FixedSizeList(_, _) => {
                let array = self
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("Just checked it has this type.");
                let value = array.value(i).clone_as_list()?;
                Some(table_scalar::Value::FixedSizeList(value))
            }
            // Unsupported types
            DataType::Union(_, _, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "Array::scalar",
                    "Union",
                ));
            }
            DataType::Decimal128(_, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "Array::scalar",
                    "Decimal128",
                ));
            }
            DataType::Decimal256(_, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "Array::scalar",
                    "Decimal256",
                ));
            }
            DataType::Map(_, _) => {
                return Err(ArrowScalarError::Unimplemented(
                    "Array::scalar",
                    "Map",
                ));
            }
        };
        Ok(TableScalar { value })
    }
}

impl TableScalar {
    pub fn data_type(&self) -> Result<DataType, ArrowScalarError> {
        let val = match &self.value {
            Some(table_scalar::Value::Boolean(_)) => DataType::Boolean,
            Some(table_scalar::Value::Int8(_)) => DataType::Int8,
            Some(table_scalar::Value::Int16(_)) => DataType::Int16,
            Some(table_scalar::Value::Int32(_)) => DataType::Int32,
            Some(table_scalar::Value::Int64(_)) => DataType::Int64,
            Some(table_scalar::Value::Uint8(_)) => DataType::UInt8,
            Some(table_scalar::Value::Uint16(_)) => DataType::UInt16,
            Some(table_scalar::Value::Uint32(_)) => DataType::UInt32,
            Some(table_scalar::Value::Uint64(_)) => DataType::UInt64,
            Some(table_scalar::Value::Float16(_)) => DataType::Float16,
            Some(table_scalar::Value::Float32(_)) => DataType::Float32,
            Some(table_scalar::Value::Float64(_)) => DataType::Float64,
            Some(table_scalar::Value::Utf8(_)) => DataType::Utf8,
            Some(table_scalar::Value::LargeUtf8(_)) => DataType::LargeUtf8,
            Some(table_scalar::Value::Binary(_)) => DataType::Binary,
            Some(table_scalar::Value::LargeBinary(_)) => DataType::LargeBinary,
            Some(table_scalar::Value::Date32(_)) => DataType::Date32,
            Some(table_scalar::Value::Date64(_)) => DataType::Date64,
            Some(table_scalar::Value::Time32Second(_)) => DataType::Time32(TimeUnit::Second),
            Some(table_scalar::Value::Time32Millisecond(_)) => {
                DataType::Time32(TimeUnit::Millisecond)
            }
            Some(table_scalar::Value::Time64Microsecond(_)) => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            Some(table_scalar::Value::Time64Nanosecond(_)) => {
                DataType::Time64(TimeUnit::Nanosecond)
            }
            Some(table_scalar::Value::TimestampSecond(_)) => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            Some(table_scalar::Value::TimestampMillisecond(_)) => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            Some(table_scalar::Value::TimestampMicrosecond(_)) => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            Some(table_scalar::Value::TimestampNanosecond(_)) => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            Some(table_scalar::Value::IntervalYearMonth(_)) => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            Some(table_scalar::Value::IntervalDayTime(_)) => {
                DataType::Interval(IntervalUnit::DayTime)
            }
            Some(table_scalar::Value::DurationSecond(_)) => DataType::Duration(TimeUnit::Second),
            Some(table_scalar::Value::DurationMillisecond(_)) => {
                DataType::Duration(TimeUnit::Millisecond)
            }
            Some(table_scalar::Value::DurationMicrosecond(_)) => {
                DataType::Duration(TimeUnit::Microsecond)
            }
            Some(table_scalar::Value::DurationNanosecond(_)) => {
                DataType::Duration(TimeUnit::Nanosecond)
            }
            Some(table_scalar::Value::List(list)) => {
                DataType::List(Box::new(Field::new("item", list.data_type()?, false)))
            }
            Some(table_scalar::Value::LargeList(list)) => {
                DataType::LargeList(Box::new(Field::new("item", list.data_type()?, false)))
            }
            Some(table_scalar::Value::FixedSizeList(list)) => DataType::FixedSizeList(
                Box::new(Field::new("item", list.data_type()?, false)),
                list.len() as i32,
            ),
            Some(table_scalar::Value::Struct(struct_)) => {
                let fields = struct_
                    .elements
                    .iter()
                    .map(|(name, field)| Ok(Field::new(name, field.data_type()?, true)))
                    .collect::<Result<Vec<_>, ArrowScalarError>>()?;
                DataType::Struct(fields)
            }
            Some(table_scalar::Value::FixedSizeBinary(fixed_size_binary)) => {
                DataType::FixedSizeBinary(fixed_size_binary.len() as i32)
            }
            Some(table_scalar::Value::Union(_union)) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableScalar::data_type",
                    "Union",
                ))
            }
            Some(table_scalar::Value::Dictionary(_dict)) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableScalar::data_type",
                    "Dictionary",
                ))
            }
            Some(table_scalar::Value::Map(_map)) => {
                return Err(ArrowScalarError::Unimplemented(
                    "TableScalar::data_type",
                    "Map",
                ))
            }
            Some(table_scalar::Value::Null(_)) => DataType::Null,
            None => return Err(ArrowScalarError::InvalidProtobuf),
        };
        Ok(val)
    }
    pub fn int8(value: i8) -> Self {
        Self {
            value: Some(table_scalar::Value::Int8(value as i32)),
        }
    }
    pub fn int16(value: i16) -> Self {
        Self {
            value: Some(table_scalar::Value::Int16(value as i32)),
        }
    }
    pub fn int32(value: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::Int32(value)),
        }
    }
    pub fn int64(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::Int64(value)),
        }
    }
    pub fn uint8(value: u8) -> Self {
        Self {
            value: Some(table_scalar::Value::Uint8(value as u32)),
        }
    }
    pub fn uint16(value: u16) -> Self {
        Self {
            value: Some(table_scalar::Value::Uint16(value as u32)),
        }
    }
    pub fn uint32(value: u32) -> Self {
        Self {
            value: Some(table_scalar::Value::Uint32(value)),
        }
    }
    pub fn uint64(value: u64) -> Self {
        Self {
            value: Some(table_scalar::Value::Uint64(value)),
        }
    }
    pub fn float16(value: f16) -> Self {
        Self {
            value: Some(table_scalar::Value::Float16(value.to_f32())),
        }
    }
    pub fn float32(value: f32) -> Self {
        Self {
            value: Some(table_scalar::Value::Float32(value)),
        }
    }
    pub fn float64(value: f64) -> Self {
        Self {
            value: Some(table_scalar::Value::Float64(value)),
        }
    }
    pub fn boolean(value: bool) -> Self {
        Self {
            value: Some(table_scalar::Value::Boolean(value)),
        }
    }
    pub fn utf8(value: String) -> Self {
        Self {
            value: Some(table_scalar::Value::Utf8(value)),
        }
    }
    pub fn date32(value: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::Date32(value)),
        }
    }
    pub fn date64(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::Date64(value)),
        }
    }
    pub fn time32_second(value: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::Time32Second(value)),
        }
    }
    pub fn time32_millisecond(value: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::Time32Millisecond(value)),
        }
    }
    pub fn time64_nanosecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::Time64Nanosecond(value)),
        }
    }
    pub fn time64_microsecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::Time64Microsecond(value)),
        }
    }
    pub fn timestamp_second(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::TimestampSecond(value)),
        }
    }
    pub fn timestamp_millisecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::TimestampMillisecond(value)),
        }
    }
    pub fn timestamp_microsecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::TimestampMicrosecond(value)),
        }
    }
    pub fn timestamp_nanosecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::TimestampNanosecond(value)),
        }
    }
    pub fn interval_day_time(days: i32, millis: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::IntervalDayTime(
                IntervalDayTimeType::make_value(days, millis),
            )),
        }
    }
    pub fn interval_year_month(years: i32, months: i32) -> Self {
        Self {
            value: Some(table_scalar::Value::IntervalYearMonth(
                IntervalYearMonthType::make_value(years, months),
            )),
        }
    }
    pub fn duration_second(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::DurationNanosecond(value)),
        }
    }
    pub fn duration_millisecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::DurationNanosecond(value)),
        }
    }
    pub fn duration_microsecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::DurationNanosecond(value)),
        }
    }
    pub fn duration_nanosecond(value: i64) -> Self {
        Self {
            value: Some(table_scalar::Value::DurationNanosecond(value)),
        }
    }
    pub fn binary(value: Vec<u8>) -> Self {
        Self {
            value: Some(table_scalar::Value::Binary(value)),
        }
    }
    pub fn large_binary(value: Vec<u8>) -> Self {
        Self {
            value: Some(table_scalar::Value::LargeBinary(value)),
        }
    }
    pub fn fixed_size_binary(value: Vec<u8>) -> Self {
        Self {
            value: Some(table_scalar::Value::FixedSizeBinary(value)),
        }
    }
    pub fn list(value: Vec<Self>) -> Result<Self, ArrowScalarError> {
        Ok(Self {
            value: Some(table_scalar::Value::List(value.try_into()?)),
        })
    }
    pub fn large_list(value: Vec<Self>) -> Result<Self, ArrowScalarError> {
        Ok(Self {
            value: Some(table_scalar::Value::LargeList(value.try_into()?)),
        })
    }
    pub fn fixed_size_list(value: Vec<Self>) -> Result<Self, ArrowScalarError> {
        Ok(Self {
            value: Some(table_scalar::Value::FixedSizeList(value.try_into()?)),
        })
    }
    pub fn struct_(value: HashMap<String, Self>) -> Self {
        Self {
            value: Some(table_scalar::Value::Struct(table_scalar::Struct {
                elements: value,
            })),
        }
    }
}

impl TryFrom<Vec<TableScalar>> for TableList {
    type Error = ArrowScalarError;
    fn try_from(v: Vec<TableScalar>) -> Result<Self, Self::Error> {
        let dtype = if let Some(dtype) = v.first().map(|v| v.data_type()) {
            dtype?
        } else {
            return Ok(TableList { values: None });
        };

        let mut list = TableList::new(&dtype)?;
        for v in v {
            list.push(v)?;
        }
        Ok(list)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{table_list, TableList};

    use super::*;

    #[test]
    fn test_bool_scalar() {
        let values = vec![Some(true), Some(false), None, Some(true), Some(false)];
        let array = BooleanArray::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Boolean(true))
            }
        );
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Boolean(false))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
    }

    #[test]
    fn test_primitive_int_scalar() {
        let values = vec![Some(1), Some(2), None, Some(3), Some(4)];
        let array = Int8Array::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Int8(1))
            }
        );
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Int8(2))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Int8(3))
            }
        );
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Int8(4))
            }
        );
    }

    #[test]
    fn test_string_scalar() {
        let values = vec![Some("1.0"), Some("2.0"), None, Some("3.0"), Some("4.0")];
        let array = StringArray::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("1.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("2.0".to_string()))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("3.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("4.0".to_string()))
            }
        );
    }

    #[test]
    fn test_large_string_scalar() {
        let values = vec![Some("1.0"), Some("2.0"), None, Some("3.0"), Some("4.0")];
        let array = LargeStringArray::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("1.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("2.0".to_string()))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("3.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("4.0".to_string()))
            }
        );
    }

    #[test]
    fn test_primitive_float_scalar() {
        let values = vec![Some(1.0), Some(2.0), None, Some(3.0), Some(4.0)];
        let array = Float32Array::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Float32(1.0))
            }
        );
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Float32(2.0))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Float32(3.0))
            }
        );
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Float32(4.0))
            }
        );
    }

    #[test]
    fn test_struct_scalar() {
        let values = vec![Some(true), Some(false), None, Some(true), Some(false)];
        let bool_array = Arc::new(BooleanArray::from(values));
        let values = vec![1.0, 2.0, 0.0, 3.0, 4.0];
        let float_array = Arc::new(Float64Array::from(values));
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
            Some(vec![Some(8)]),
        ];
        let list_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));
        let schema = vec![
            (
                Field::new("bo", DataType::Boolean, true),
                bool_array as ArrayRef,
            ),
            (
                Field::new("fl", DataType::Float64, false),
                float_array as ArrayRef,
            ),
            (
                Field::new(
                    "list",
                    DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                    false,
                ),
                list_array as ArrayRef,
            ),
        ];
        let struct_array = StructArray::from(schema);
        let first_scalar = table_scalar::Struct {
            elements: HashMap::from([
                (
                    "bo".to_owned(),
                    TableScalar {
                        value: Some(table_scalar::Value::Boolean(false)),
                    },
                ),
                (
                    "fl".to_owned(),
                    TableScalar {
                        value: Some(table_scalar::Value::Float64(2.0)),
                    },
                ),
                ("list".to_owned(), TableScalar { value: None }),
            ]),
        };
        let list_entries = table_list::Int32List {
            values: vec![3, 0, 5],
            set: vec![true, false, true],
        };
        let list_value = TableList {
            values: Some(table_list::Values::Int32(list_entries)),
        };
        let second_scalar = table_scalar::Struct {
            elements: HashMap::from([
                ("bo".to_owned(), TableScalar { value: None }),
                (
                    "f1".to_owned(),
                    TableScalar {
                        value: Some(table_scalar::Value::Float64(0.0)),
                    },
                ),
                (
                    "list".to_owned(),
                    TableScalar {
                        value: Some(table_scalar::Value::List(list_value)),
                    },
                ),
            ]),
        };

        assert_eq!(
            struct_array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Struct(first_scalar))
            }
        );
        assert_eq!(
            struct_array.scalar(2).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Struct(second_scalar))
            }
        );
    }

    #[test]
    fn test_dictionary_scalar() {
        let values = vec!["one", "one", "three", "one", "one"];
        let array: DictionaryArray<Int8Type> = values.into_iter().collect();

        let scalar = TableScalar {
            value: Some(table_scalar::Value::Utf8("one".to_string())),
        };
        let dict_scalar = TableScalar {
            value: Some(table_scalar::Value::Dictionary(Box::new(scalar))),
        };
        assert_eq!(array.scalar(1).unwrap(), dict_scalar);
    }

    /*
    #[test]
    fn test_dictionary_list() {
        let values = vec!["one", "one", "three", "one", "one"];
        let array: Arc<DictionaryArray<Int8Type>> = Arc::new(values.into_iter().collect());

        let list = array.clone_as_list();
        let intended_list = TableList {
            values: Some(table_list::Values::Dictionary(Box::new(TableList {
                values: Some(table_list::Values::Utf8(Utf8List {
                    values: vec![
                        "one".to_string(),
                        "one".to_string(),
                        "three".to_string(),
                        "one".to_string(),
                        "one".to_string(),
                    ],
                    set: vec![true, true, true, true, true],
                })),
            }))),
        };
        assert_eq!(intended_list, list);
        assert_eq!(
            as_dictionary_array::<Int8Type>(&list.to_array()),
            array.deref()
        );
    }
    */

    #[test]
    fn test_list_scalar() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
            Some(vec![Some(8)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        let list_entries = table_list::Int32List {
            values: vec![3, 0, 5],
            set: vec![true, false, true],
        };
        let list_value = TableList {
            values: Some(table_list::Values::Int32(list_entries)),
        };
        assert_eq!(
            list_array.scalar(2).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::List(list_value))
            }
        );
    }

    #[test]
    fn test_dict_list_scalar() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
            Some(vec![Some(8)]),
        ];
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let keys = Int8Array::from(vec![0, 0, 0, 4, 3, 1, 2]);
        let array: Arc<DictionaryArray<Int8Type>> =
            Arc::new(DictionaryArray::try_new(&keys, &list).unwrap());
        let list_entries = table_list::Int32List {
            values: vec![0, 1, 2],
            set: vec![true, true, true],
        };
        let list_value = TableList {
            values: Some(table_list::Values::Int32(list_entries)),
        };
        assert_eq!(
            array.scalar(2).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Dictionary(Box::new(TableScalar {
                    value: Some(table_scalar::Value::List(list_value))
                })))
            }
        );

        let list_entries = table_list::Int32List {
            values: vec![3, 0, 5],
            set: vec![true, false, true],
        };
        let list_value = TableList {
            values: Some(table_list::Values::Int32(list_entries)),
        };
        assert_eq!(
            array.scalar(6).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Dictionary(Box::new(TableScalar {
                    value: Some(table_scalar::Value::List(list_value))
                })))
            }
        );
    }
}
