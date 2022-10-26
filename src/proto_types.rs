use crate::data_type_proto::EmptyMessage;
use crate::{
    data_type_proto, FieldProto,
};
use crate::{DataTypeProto};
use arrow::datatypes::*;

impl DataTypeProto {
    pub fn from_arrow(value: &DataType) -> Self {
        let t = match value {
            DataType::Int8 => Some(data_type_proto::DataType::Int8(EmptyMessage {})),
            DataType::Int16 => Some(data_type_proto::DataType::Int16(EmptyMessage {})),
            DataType::Int32 => Some(data_type_proto::DataType::Int32(EmptyMessage {})),
            DataType::Int64 => Some(data_type_proto::DataType::Int64(EmptyMessage {})),
            DataType::UInt8 => Some(data_type_proto::DataType::Uint8(EmptyMessage {})),
            DataType::UInt16 => Some(data_type_proto::DataType::Uint16(EmptyMessage {})),
            DataType::UInt32 => Some(data_type_proto::DataType::Uint32(EmptyMessage {})),
            DataType::UInt64 => Some(data_type_proto::DataType::Uint64(EmptyMessage {})),
            DataType::Float16 => Some(data_type_proto::DataType::Float16(EmptyMessage {})),
            DataType::Float32 => Some(data_type_proto::DataType::Float32(EmptyMessage {})),
            DataType::Float64 => Some(data_type_proto::DataType::Float64(EmptyMessage {})),
            DataType::Date32 => Some(data_type_proto::DataType::Date32(EmptyMessage {})),
            DataType::Date64 => Some(data_type_proto::DataType::Date64(EmptyMessage {})),
            DataType::Boolean => Some(data_type_proto::DataType::Bool(EmptyMessage {})),
            DataType::Utf8 => Some(data_type_proto::DataType::Utf8(EmptyMessage {})),
            DataType::LargeUtf8 => Some(data_type_proto::DataType::LargeUtf8(EmptyMessage {})),
            DataType::Binary => Some(data_type_proto::DataType::Binary(EmptyMessage {})),
            DataType::LargeBinary => Some(data_type_proto::DataType::LargeBinary(EmptyMessage {})),
            DataType::Time32(data_type) => match data_type {
                TimeUnit::Second => Some(data_type_proto::DataType::Time32(
                    data_type_proto::TimeUnit::Second.into(),
                )),
                TimeUnit::Millisecond => Some(data_type_proto::DataType::Time32(
                    data_type_proto::TimeUnit::Millisecond.into(),
                )),
                _ => None,
            },
            DataType::Time64(data_type) => match data_type {
                TimeUnit::Microsecond => Some(data_type_proto::DataType::Time64(
                    data_type_proto::TimeUnit::Microsecond.into(),
                )),
                TimeUnit::Nanosecond => Some(data_type_proto::DataType::Time64(
                    data_type_proto::TimeUnit::Nanosecond.into(),
                )),
                _ => None,
            },
            DataType::Timestamp(data_type, tz) => {
                let unit = match data_type {
                    TimeUnit::Second => data_type_proto::TimeUnit::Second.into(),
                    TimeUnit::Millisecond => data_type_proto::TimeUnit::Millisecond.into(),
                    TimeUnit::Microsecond => data_type_proto::TimeUnit::Microsecond.into(),
                    TimeUnit::Nanosecond => data_type_proto::TimeUnit::Nanosecond.into(),
                };
                let tz = tz.to_owned();
                Some(data_type_proto::DataType::Timestamp(
                    data_type_proto::Timestamp { unit, tz },
                ))
            }
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_owned();
                        let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
                        FieldProto {
                            name,
                            data_type,
                            nullable: field.is_nullable(),
                        }
                    })
                    .collect();
                Some(data_type_proto::DataType::Struct(data_type_proto::Struct {
                    fields,
                }))
            }
            DataType::List(field) => {
                let name = field.name().to_owned();
                let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
                let list_type = Box::new(FieldProto {
                    name,
                    data_type,
                    nullable: field.is_nullable(),
                });
                Some(data_type_proto::DataType::List(list_type))
            }
            DataType::LargeList(field) => {
                let name = field.name().to_owned();
                let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
                let list_type = Box::new(FieldProto {
                    name,
                    data_type,
                    nullable: field.is_nullable(),
                });
                Some(data_type_proto::DataType::LargeList(list_type))
            }
            DataType::FixedSizeList(field, size) => {
                let name = field.name().to_owned();
                let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
                let list_type = Some(Box::new(FieldProto {
                    name,
                    data_type,
                    nullable: field.is_nullable(),
                }));
                Some(data_type_proto::DataType::FixedSizeList(Box::new(
                    data_type_proto::FixedSizeList {
                        list_type,
                        size: *size,
                    },
                )))
            }
            DataType::Dictionary(key_type, value_type) => {
                let key_type = Some(Box::new(DataTypeProto::from_arrow(key_type)));
              let value_type = Some(Box::new(DataTypeProto::from_arrow(value_type)));
              Some(data_type_proto::DataType::Dictionary(Box::new(
                    data_type_proto::Dictionary {
                        key_type,
                        value_type,
                    },
                )))
            }
            DataType::FixedSizeBinary(size) => Some(data_type_proto::DataType::FixedSizeBinary(*size)),
            DataType::Interval(interval_type) => match interval_type {
                IntervalUnit::YearMonth => Some(data_type_proto::DataType::Interval(
                    data_type_proto::IntervalUnit::YearMonth.into(),
                )),
                IntervalUnit::DayTime => Some(data_type_proto::DataType::Interval(
                    data_type_proto::IntervalUnit::DayTime.into(),
                )),
                IntervalUnit::MonthDayNano => Some(data_type_proto::DataType::Interval(
                    data_type_proto::IntervalUnit::MonthDayNano.into(),
                )),
            },
            DataType::Union(fields, type_ids, union_mode) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_owned();
                        let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
                        FieldProto {
                            name,
                            data_type,
                            nullable: field.is_nullable(),
                        }
                    })
                    .collect();
                let type_ids = type_ids.iter().map(|type_id| *type_id as i32).collect();
                let mode = match union_mode {
                    UnionMode::Sparse => data_type_proto::union::Mode::Sparse,
                    UnionMode::Dense => data_type_proto::union::Mode::Dense,
                };
                Some(data_type_proto::DataType::Union(data_type_proto::Union {
                    fields,
                    type_ids,
                    mode: mode.into(),
                }))
            }
            DataType::Duration(data_type) => match data_type {
                TimeUnit::Second => Some(data_type_proto::DataType::Duration(
                    data_type_proto::TimeUnit::Second.into(),
                )),
                TimeUnit::Millisecond => Some(data_type_proto::DataType::Duration(
                    data_type_proto::TimeUnit::Millisecond.into(),
                )),
                TimeUnit::Microsecond => Some(data_type_proto::DataType::Duration(
                    data_type_proto::TimeUnit::Microsecond.into(),
                )),
                TimeUnit::Nanosecond => Some(data_type_proto::DataType::Duration(
                    data_type_proto::TimeUnit::Nanosecond.into(),
                )),
            },
            DataType::Decimal128(precision, scale) => Some(data_type_proto::DataType::Decimal128(
                data_type_proto::Decimal {
                    precision: *precision as i32,
                    scale: *scale as i32,
                },
            )),
            DataType::Decimal256(precision, scale) => Some(data_type_proto::DataType::Decimal256(
                data_type_proto::Decimal {
                    precision: *precision as i32,
                    scale: *scale as i32,
                },
            )),
            DataType::Map(struct_field, keys_sorted) => {
                let name = struct_field.name().to_owned();
                let data_type = Some(Box::new(DataTypeProto::from_arrow(struct_field.data_type())));
                let struct_field = Some(Box::new(FieldProto {
                    name,
                    data_type,
                    nullable: struct_field.is_nullable(),
                }));
                Some(data_type_proto::DataType::Map(Box::new(
                    data_type_proto::Map {
                        struct_field,
                        keys_sorted: *keys_sorted,
                    },
                )))
            }
            DataType::Null => Some(data_type_proto::DataType::Null(
                data_type_proto::EmptyMessage {},
            )),
        };
        DataTypeProto { data_type: t }
    }
}
