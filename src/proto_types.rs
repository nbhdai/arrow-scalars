use crate::data_type_proto::EmptyMessage;
use crate::{data_type_proto, FieldProto, SchemaProto};
use crate::{ArrowScalarError, DataTypeProto};
use arrow::datatypes::*;

impl data_type_proto::IntervalUnit {
    pub fn to_arrow(&self) -> IntervalUnit {
        match self {
            data_type_proto::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            data_type_proto::IntervalUnit::DayTime => IntervalUnit::DayTime,
            data_type_proto::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }

    pub fn from_arrow(field: &IntervalUnit) -> Self {
        match field {
            IntervalUnit::YearMonth => data_type_proto::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => data_type_proto::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => data_type_proto::IntervalUnit::MonthDayNano,
        }
    }
}

impl FieldProto {
    pub fn to_arrow(&self) -> Result<Field, ArrowScalarError> {
        let data_type = if let Some(data_type) = self.data_type.as_ref() {
            data_type.to_arrow()?
        } else {
            return Err(ArrowScalarError::InvalidProtobuf);
        };
        Ok(Field::new(&self.name, data_type, self.nullable))
    }

    pub fn from_arrow(field: &Field) -> Self {
        let name = field.name().to_owned();
        let data_type = Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
        FieldProto {
            name,
            data_type,
            nullable: field.is_nullable(),
        }
    }
}

impl SchemaProto {
    pub fn to_arrow(&self) -> Result<Schema, ArrowScalarError> {
        let fields = self
            .fields
            .iter()
            .map(|field| field.to_arrow())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }

    pub fn from_arrow(schema: &Schema) -> Self {
        let fields = schema.fields().iter().map(FieldProto::from_arrow).collect();
        SchemaProto { fields }
    }
}

impl DataTypeProto {
    pub fn to_arrow(&self) -> Result<DataType, ArrowScalarError> {
        if let Some(proto_type) = self.data_type.as_ref() {
            let dt = match proto_type {
                data_type_proto::DataType::Int8(_) => DataType::Int8,
                data_type_proto::DataType::Int16(_) => DataType::Int16,
                data_type_proto::DataType::Int32(_) => DataType::Int32,
                data_type_proto::DataType::Int64(_) => DataType::Int64,
                data_type_proto::DataType::Uint8(_) => DataType::UInt8,
                data_type_proto::DataType::Uint16(_) => DataType::UInt16,
                data_type_proto::DataType::Uint32(_) => DataType::UInt32,
                data_type_proto::DataType::Uint64(_) => DataType::UInt64,
                data_type_proto::DataType::Float16(_) => DataType::Float16,
                data_type_proto::DataType::Float32(_) => DataType::Float32,
                data_type_proto::DataType::Float64(_) => DataType::Float64,
                data_type_proto::DataType::Date32(_) => DataType::Date32,
                data_type_proto::DataType::Date64(_) => DataType::Date64,
                data_type_proto::DataType::Bool(_) => DataType::Boolean,
                data_type_proto::DataType::Utf8(_) => DataType::Utf8,
                data_type_proto::DataType::LargeUtf8(_) => DataType::LargeUtf8,
                data_type_proto::DataType::Binary(_) => DataType::Binary,
                data_type_proto::DataType::LargeBinary(_) => DataType::LargeBinary,
                data_type_proto::DataType::FixedSizeBinary(size) => {
                    DataType::FixedSizeBinary(*size)
                }
                data_type_proto::DataType::Time32Second(_) => DataType::Time32(TimeUnit::Second),
                data_type_proto::DataType::Time32Millisecond(_) => {
                    DataType::Time32(TimeUnit::Millisecond)
                }
                data_type_proto::DataType::Time64Microsecond(_) => {
                    DataType::Time64(TimeUnit::Microsecond)
                }
                data_type_proto::DataType::Time64Nanosecond(_) => {
                    DataType::Time64(TimeUnit::Nanosecond)
                }
                data_type_proto::DataType::TimestampSecond(tz) => {
                    DataType::Timestamp(TimeUnit::Second, tz.tz.clone())
                }
                data_type_proto::DataType::TimestampMillisecond(tz) => {
                    DataType::Timestamp(TimeUnit::Millisecond, tz.tz.clone())
                }
                data_type_proto::DataType::TimestampMicrosecond(tz) => {
                    DataType::Timestamp(TimeUnit::Microsecond, tz.tz.clone())
                }
                data_type_proto::DataType::TimestampNanosecond(tz) => {
                    DataType::Timestamp(TimeUnit::Nanosecond, tz.tz.clone())
                }
                data_type_proto::DataType::IntervalYearMonth(_) => {
                    DataType::Interval(IntervalUnit::YearMonth)
                }
                data_type_proto::DataType::IntervalDayTime(_) => {
                    DataType::Interval(IntervalUnit::DayTime)
                }
                data_type_proto::DataType::IntervalMonthDayNano(_) => {
                    DataType::Interval(IntervalUnit::MonthDayNano)
                }
                data_type_proto::DataType::DurationSecond(_) => {
                    DataType::Duration(TimeUnit::Second)
                }
                data_type_proto::DataType::DurationMillisecond(_) => {
                    DataType::Duration(TimeUnit::Millisecond)
                }
                data_type_proto::DataType::DurationMicrosecond(_) => {
                    DataType::Duration(TimeUnit::Microsecond)
                }
                data_type_proto::DataType::DurationNanosecond(_) => {
                    DataType::Duration(TimeUnit::Nanosecond)
                }
                data_type_proto::DataType::List(field) => {
                    DataType::List(Box::new(field.to_arrow()?))
                }
                data_type_proto::DataType::LargeList(field) => {
                    DataType::List(Box::new(field.to_arrow()?))
                }
                data_type_proto::DataType::FixedSizeList(fsl) => {
                    let data_type_proto::FixedSizeList { list_type, size } = fsl.as_ref();
                    match list_type {
                        Some(field) => DataType::FixedSizeList(Box::new(field.to_arrow()?), *size),
                        None => return Err(ArrowScalarError::InvalidProtobuf),
                    }
                }
                data_type_proto::DataType::Struct(data_type_proto::Struct { fields }) => {
                    let fields = fields
                        .iter()
                        .map(|field| field.to_arrow())
                        .collect::<Result<Vec<_>, _>>()?;
                    DataType::Struct(fields)
                }
                data_type_proto::DataType::Union(data_type_proto::Union {
                    fields,
                    type_ids,
                    mode,
                }) => {
                    let fields = fields
                        .iter()
                        .map(|field| field.to_arrow())
                        .collect::<Result<Vec<_>, _>>()?;
                    let type_ids = type_ids.iter().map(|i| *i as i8).collect();
                    let mode = match data_type_proto::union::Mode::from_i32(*mode) {
                        Some(data_type_proto::union::Mode::Dense) => UnionMode::Dense,
                        Some(data_type_proto::union::Mode::Sparse) => UnionMode::Sparse,
                        _ => return Err(ArrowScalarError::InvalidProtobuf),
                    };
                    DataType::Union(fields, type_ids, mode)
                }
                data_type_proto::DataType::Dictionary(dict) => {
                    let data_type_proto::Dictionary {
                        key_type,
                        value_type,
                    } = dict.as_ref();
                    match (key_type, value_type) {
                        (Some(key_type), Some(value_type)) => DataType::Dictionary(
                            Box::new(key_type.to_arrow()?),
                            Box::new(value_type.to_arrow()?),
                        ),
                        _ => return Err(ArrowScalarError::InvalidProtobuf),
                    }
                }
                data_type_proto::DataType::Decimal128(data_type_proto::Decimal {
                    precision,
                    scale,
                }) => DataType::Decimal128(
                    (*precision).try_into().unwrap(),
                    (*scale).try_into().unwrap(),
                ),
                data_type_proto::DataType::Decimal256(data_type_proto::Decimal {
                    precision,
                    scale,
                }) => DataType::Decimal256(
                    (*precision).try_into().unwrap(),
                    (*scale).try_into().unwrap(),
                ),
                data_type_proto::DataType::Map(map) => {
                    let data_type_proto::Map {
                        struct_field,
                        keys_sorted,
                    } = map.as_ref();
                    if let Some(field) = struct_field {
                        DataType::Map(Box::new(field.to_arrow()?), *keys_sorted)
                    } else {
                        return Err(ArrowScalarError::InvalidProtobuf);
                    }
                }
                data_type_proto::DataType::Null(_) => DataType::Null,
            };
            Ok(dt)
        } else {
            Err(ArrowScalarError::InvalidProtobuf)
        }
    }
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
                TimeUnit::Second => Some(data_type_proto::DataType::Time32Second(EmptyMessage {})),
                TimeUnit::Millisecond => Some(data_type_proto::DataType::Time32Millisecond(
                    EmptyMessage {},
                )),
                _ => None,
            },
            DataType::Time64(data_type) => match data_type {
                TimeUnit::Microsecond => Some(data_type_proto::DataType::Time64Microsecond(
                    EmptyMessage {},
                )),
                TimeUnit::Nanosecond => {
                    Some(data_type_proto::DataType::Time64Nanosecond(EmptyMessage {}))
                }
                _ => None,
            },
            DataType::Timestamp(data_type, tz) => {
                let tz = data_type_proto::Timestamp { tz: tz.to_owned() };
                let unit = match data_type {
                    TimeUnit::Second => data_type_proto::DataType::TimestampSecond(tz),
                    TimeUnit::Millisecond => data_type_proto::DataType::TimestampMillisecond(tz),
                    TimeUnit::Microsecond => data_type_proto::DataType::TimestampMicrosecond(tz),
                    TimeUnit::Nanosecond => data_type_proto::DataType::TimestampNanosecond(tz),
                };
                Some(unit)
            }
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_owned();
                        let data_type =
                            Some(Box::new(DataTypeProto::from_arrow(field.data_type())));
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
            DataType::List(field) => Some(data_type_proto::DataType::List(Box::new(
                FieldProto::from_arrow(field),
            ))),
            DataType::LargeList(field) => Some(data_type_proto::DataType::List(Box::new(
                FieldProto::from_arrow(field),
            ))),
            DataType::FixedSizeList(field, size) => {
                let list_type = Some(Box::new(FieldProto::from_arrow(field)));

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
            DataType::FixedSizeBinary(size) => {
                Some(data_type_proto::DataType::FixedSizeBinary(*size))
            }
            DataType::Interval(interval_type) => match interval_type {
                IntervalUnit::YearMonth => Some(data_type_proto::DataType::IntervalYearMonth(
                    EmptyMessage {},
                )),
                IntervalUnit::DayTime => {
                    Some(data_type_proto::DataType::IntervalDayTime(EmptyMessage {}))
                }
                IntervalUnit::MonthDayNano => Some(
                    data_type_proto::DataType::IntervalMonthDayNano(EmptyMessage {}),
                ),
            },
            DataType::Union(fields, type_ids, union_mode) => {
                let fields = fields.iter().map(FieldProto::from_arrow).collect();
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
                TimeUnit::Second => {
                    Some(data_type_proto::DataType::DurationSecond(EmptyMessage {}))
                }
                TimeUnit::Millisecond => Some(data_type_proto::DataType::DurationMillisecond(
                    EmptyMessage {},
                )),
                TimeUnit::Microsecond => Some(data_type_proto::DataType::DurationMicrosecond(
                    EmptyMessage {},
                )),
                TimeUnit::Nanosecond => Some(data_type_proto::DataType::DurationNanosecond(
                    EmptyMessage {},
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
                let struct_field = Some(Box::new(FieldProto::from_arrow(struct_field)));
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
