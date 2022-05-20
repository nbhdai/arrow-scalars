use crate::list::ListValuable;
use crate::{table_scalar, TableScalar};
use arrow::array::*;
use arrow::datatypes::*;
use std::ops::Deref;

pub trait ScalarValuable {
    fn scalar(&self, i: usize) -> TableScalar;
}

impl<T: Array> ScalarValuable for T {
    fn scalar(&self, i: usize) -> TableScalar {
        if self.is_null(i) {
            return TableScalar { value: None };
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
            // Complex array types
            DataType::Timestamp(unit, tz) => {
                let (unit, value) = match unit {
                    TimeUnit::Second => (
                        table_scalar::TimeUnit::Second,
                        as_primitive_array::<TimestampSecondType>(self).value(i),
                    ),
                    TimeUnit::Millisecond => (
                        table_scalar::TimeUnit::Millisecond,
                        as_primitive_array::<TimestampMillisecondType>(self).value(i),
                    ),
                    TimeUnit::Microsecond => (
                        table_scalar::TimeUnit::Microsecond,
                        as_primitive_array::<TimestampMicrosecondType>(self).value(i),
                    ),
                    TimeUnit::Nanosecond => (
                        table_scalar::TimeUnit::Nanosecond,
                        as_primitive_array::<TimestampNanosecondType>(self).value(i),
                    ),
                };
                let time = table_scalar::Time {
                    unit: unit.into(),
                    time: value,
                    tz: tz.clone().unwrap_or_default(),
                };
                Some(table_scalar::Value::Timestamp(time))
            }
            DataType::Time32(unit) => {
                let (unit, value) = match unit {
                    TimeUnit::Second => (
                        table_scalar::TimeUnit::Second,
                        as_primitive_array::<Time32SecondType>(self).value(i).into(),
                    ),
                    TimeUnit::Millisecond => (
                        table_scalar::TimeUnit::Millisecond,
                        as_primitive_array::<Time32MillisecondType>(self)
                            .value(i)
                            .into(),
                    ),
                    _ => unreachable!(),
                };
                let time = table_scalar::Time {
                    unit: unit.into(),
                    time: value,
                    tz: String::new(),
                };
                Some(table_scalar::Value::Time32(time))
            }
            DataType::Time64(unit) => {
                let (unit, value) = match unit {
                    TimeUnit::Microsecond => (
                        table_scalar::TimeUnit::Microsecond,
                        as_primitive_array::<Time64MicrosecondType>(self).value(i),
                    ),
                    TimeUnit::Nanosecond => (
                        table_scalar::TimeUnit::Nanosecond,
                        as_primitive_array::<Time64NanosecondType>(self).value(i),
                    ),
                    _ => unreachable!(),
                };
                let time = table_scalar::Time {
                    unit: unit.into(),
                    time: value,
                    tz: String::new(),
                };
                Some(table_scalar::Value::Time64(time))
            }
            DataType::Interval(interval) => {
                let value = match interval {
                    IntervalUnit::YearMonth => table_scalar::interval::Unit::YearMonth(
                        as_primitive_array::<IntervalYearMonthType>(self).value(i),
                    ),
                    IntervalUnit::DayTime => table_scalar::interval::Unit::DayTime(
                        as_primitive_array::<IntervalDayTimeType>(self).value(i),
                    ),
                    IntervalUnit::MonthDayNano => {
                        let value = as_primitive_array::<IntervalMonthDayNanoType>(self)
                            .value(i)
                            .to_le_bytes();
                        table_scalar::interval::Unit::MonthDayNano(value.to_vec())
                    }
                };
                let interval = table_scalar::Interval { unit: Some(value) };
                Some(table_scalar::Value::Interval(interval))
            }
            DataType::Struct(_) => {
                let arrays = as_struct_array(self);
                let elements = arrays.columns().iter().map(|arr| arr.scalar(i)).collect();
                let value = table_scalar::Struct { elements };
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

                Some(table_scalar::Value::Dictionary(Box::new(value)))
            }
            DataType::List(_) => {
                let array = self
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Just checked it has this type.");
                let value = array.value(i).into_list();
                Some(table_scalar::Value::List(value))
            }
            _ => unimplemented!(),
        };
        TableScalar { value }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use crate::table_scalar::{table_list, TableList};

    use super::*;

    #[test]
    fn test_bool_scalar() {
        let values = vec![Some(true), Some(false), None, Some(true), Some(false)];
        let array = BooleanArray::from(values);
        assert_eq!(
            array.scalar(0),
            TableScalar {
                value: Some(table_scalar::Value::Boolean(true))
            }
        );
        assert_eq!(
            array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::Boolean(false))
            }
        );
        assert_eq!(array.scalar(2), TableScalar { value: None });
    }

    #[test]
    fn test_primitive_int_scalar() {
        let values = vec![Some(1), Some(2), None, Some(3), Some(4)];
        let array = Int8Array::from(values);
        assert_eq!(
            array.scalar(0),
            TableScalar {
                value: Some(table_scalar::Value::Int8(1))
            }
        );
        assert_eq!(
            array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::Int8(2))
            }
        );
        assert_eq!(array.scalar(2), TableScalar { value: None });
        assert_eq!(
            array.scalar(3),
            TableScalar {
                value: Some(table_scalar::Value::Int8(3))
            }
        );
        assert_eq!(
            array.scalar(4),
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
            array.scalar(0),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("1.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("2.0".to_string()))
            }
        );
        assert_eq!(array.scalar(2), TableScalar { value: None });
        assert_eq!(
            array.scalar(3),
            TableScalar {
                value: Some(table_scalar::Value::Utf8("3.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(4),
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
            array.scalar(0),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("1.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("2.0".to_string()))
            }
        );
        assert_eq!(array.scalar(2), TableScalar { value: None });
        assert_eq!(
            array.scalar(3),
            TableScalar {
                value: Some(table_scalar::Value::LargeUtf8("3.0".to_string()))
            }
        );
        assert_eq!(
            array.scalar(4),
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
            array.scalar(0),
            TableScalar {
                value: Some(table_scalar::Value::Float32(1.0))
            }
        );
        assert_eq!(
            array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::Float32(2.0))
            }
        );
        assert_eq!(array.scalar(2), TableScalar { value: None });
        assert_eq!(
            array.scalar(3),
            TableScalar {
                value: Some(table_scalar::Value::Float32(3.0))
            }
        );
        assert_eq!(
            array.scalar(4),
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
            elements: vec![
                TableScalar {
                    value: Some(table_scalar::Value::Boolean(false)),
                },
                TableScalar {
                    value: Some(table_scalar::Value::Float64(2.0)),
                },
                TableScalar { value: None },
            ],
        };
        let list_entries = table_list::Int32List {
            values: vec![3, 0, 5],
            set: vec![true, false, true],
        };
        let list_value = TableList {
            values: Some(table_list::Values::Int32(list_entries)),
        };
        let second_scalar = table_scalar::Struct {
            elements: vec![
                TableScalar { value: None },
                TableScalar {
                    value: Some(table_scalar::Value::Float64(0.0)),
                },
                TableScalar {
                    value: Some(table_scalar::Value::List(list_value)),
                },
            ],
        };

        assert_eq!(
            struct_array.scalar(1),
            TableScalar {
                value: Some(table_scalar::Value::Struct(first_scalar))
            }
        );
        assert_eq!(
            struct_array.scalar(2),
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
        assert_eq!(array.scalar(1), dict_scalar);
    }

    /*
    #[test]
    fn test_dictionary_list() {
        let values = vec!["one", "one", "three", "one", "one"];
        let array: Arc<DictionaryArray<Int8Type>> = Arc::new(values.into_iter().collect());

        let list = array.into_list();
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
            list_array.scalar(2),
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
            array.scalar(2),
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
            array.scalar(6),
            TableScalar {
                value: Some(table_scalar::Value::Dictionary(Box::new(TableScalar {
                    value: Some(table_scalar::Value::List(list_value))
                })))
            }
        );
    }
}
