use crate::{data_type_proto, table_list::TimeList, table_scalar};
use arrow::{
    array::{as_primitive_array, Array},
    datatypes::*,
};
use chrono::Timelike;

use prost_types::Timestamp;

impl From<table_scalar::Time> for chrono::NaiveDateTime {
    fn from(val: table_scalar::Time) -> Self {
        match val.unit() {
            data_type_proto::TimeUnit::Second => chrono::NaiveDateTime::from_timestamp(val.time, 0),
            data_type_proto::TimeUnit::Millisecond => chrono::NaiveDateTime::from_timestamp(
                val.time / 1000,
                (val.time as u32 % 1000) * 1_000_000,
            ),
            data_type_proto::TimeUnit::Microsecond => chrono::NaiveDateTime::from_timestamp(
                val.time / 1_000_000,
                (val.time as u32 % 1_000_000) * 1_000,
            ),
            data_type_proto::TimeUnit::Nanosecond => chrono::NaiveDateTime::from_timestamp(
                val.time / 1_000_000_000,
                val.time as u32 % 1_000_000_000,
            ),
        }
    }
}

trait IntoScalarTime {
    fn scalar_seconds_from_midnight(&self) -> table_scalar::Time;
    fn scalar_milliseconds_from_midnight(&self) -> table_scalar::Time;
    fn scalar_microseconds_from_midnight(&self) -> table_scalar::Time;
    fn scalar_nanoseconds_from_midnight(&self) -> table_scalar::Time;
}

impl IntoScalarTime for chrono::NaiveTime {
    fn scalar_seconds_from_midnight(&self) -> table_scalar::Time {
        let seconds = self.num_seconds_from_midnight().into();
        table_scalar::Time {
            unit: data_type_proto::TimeUnit::Second.into(),
            time: seconds,
            tz: None,
        }
    }
    fn scalar_milliseconds_from_midnight(&self) -> table_scalar::Time {
        let seconds: i64 = self.num_seconds_from_midnight().into();
        let milli: i64 = self.nanosecond().into();
        table_scalar::Time {
            unit: data_type_proto::TimeUnit::Millisecond.into(),
            time: seconds * 1000 + milli / 1_000_000,
            tz: None,
        }
    }
    fn scalar_microseconds_from_midnight(&self) -> table_scalar::Time {
        let seconds: i64 = self.num_seconds_from_midnight().into();
        let micro: i64 = self.nanosecond().into();
        table_scalar::Time {
            unit: data_type_proto::TimeUnit::Microsecond.into(),
            time: seconds * 1_000_000 + micro / 1_000,
            tz: None,
        }
    }
    fn scalar_nanoseconds_from_midnight(&self) -> table_scalar::Time {
        let seconds: i64 = self.num_seconds_from_midnight().into();
        let nano: i64 = self.nanosecond().into();
        table_scalar::Time {
            unit: data_type_proto::TimeUnit::Nanosecond.into(),
            time: seconds * 1_000_000_000 + nano,
            tz: None,
        }
    }
}

impl From<table_scalar::Time> for chrono::NaiveTime {
    fn from(val: table_scalar::Time) -> Self {
        match val.unit() {
            data_type_proto::TimeUnit::Second => {
                chrono::NaiveTime::from_num_seconds_from_midnight(val.time as u32, 0)
            }
            data_type_proto::TimeUnit::Millisecond => {
                chrono::NaiveTime::from_num_seconds_from_midnight(
                    (val.time / 1000) as u32,
                    ((val.time % 1000) * 1_000_000) as u32,
                )
            }
            data_type_proto::TimeUnit::Microsecond => {
                chrono::NaiveTime::from_num_seconds_from_midnight(
                    (val.time / 1_000_000) as u32,
                    ((val.time % 1_000_000) * 1_000) as u32,
                )
            }
            data_type_proto::TimeUnit::Nanosecond => {
                chrono::NaiveTime::from_num_seconds_from_midnight(
                    (val.time / 1_000_000_000) as u32,
                    (val.time % 1_000_000_000) as u32,
                )
            }
        }
    }
}

impl From<table_scalar::Time> for Timestamp {
    fn from(val: table_scalar::Time) -> Self {
        let dt = chrono::NaiveDateTime::from(val);
        Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }
    }
}

impl IntoScalarTime for Timestamp {
    fn scalar_seconds_from_midnight(&self) -> table_scalar::Time {
        let dt = chrono::NaiveDateTime::from_timestamp(self.seconds, self.nanos as u32);
        dt.time().scalar_seconds_from_midnight()
    }
    fn scalar_milliseconds_from_midnight(&self) -> table_scalar::Time {
        let dt = chrono::NaiveDateTime::from_timestamp(self.seconds, self.nanos as u32);
        dt.time().scalar_milliseconds_from_midnight()
    }
    fn scalar_microseconds_from_midnight(&self) -> table_scalar::Time {
        let dt = chrono::NaiveDateTime::from_timestamp(self.seconds, self.nanos as u32);
        dt.time().scalar_microseconds_from_midnight()
    }
    fn scalar_nanoseconds_from_midnight(&self) -> table_scalar::Time {
        let dt = chrono::NaiveDateTime::from_timestamp(self.seconds, self.nanos as u32);
        dt.time().scalar_nanoseconds_from_midnight()
    }
}

pub trait TimeArray {
    fn value_chrono(&self, index: usize) -> Option<chrono::NaiveDateTime>;
    fn value_timestamp(&self, index: usize) -> Option<Timestamp> {
        self.value_chrono(index).map(|dt| Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        })
    }
}

// todo: Test this
impl TimeArray for TimeList {
    fn value_chrono(&self, index: usize) -> Option<chrono::NaiveDateTime> {
        let time = self.times[index];
        let chrono = match self.unit() {
            data_type_proto::TimeUnit::Second => chrono::NaiveDateTime::from_timestamp(time, 0),
            data_type_proto::TimeUnit::Millisecond => {
                chrono::NaiveDateTime::from_timestamp(time / 1000, (time as u32 % 1000) * 1_000_000)
            }
            data_type_proto::TimeUnit::Microsecond => chrono::NaiveDateTime::from_timestamp(
                time / 1_000_000,
                (time as u32 % 1_000_000) * 1_000,
            ),
            data_type_proto::TimeUnit::Nanosecond => chrono::NaiveDateTime::from_timestamp(
                time / 1_000_000_000,
                time as u32 % 1_000_000_000,
            ),
        };
        Some(chrono)
    }
}

impl<T: AsRef<dyn Array>> TimeArray for T {
    fn value_chrono(&self, index: usize) -> Option<chrono::NaiveDateTime> {
        let arr = self.as_ref();
        match arr.data_type() {
            DataType::Timestamp(unit, _tz) => match unit {
                TimeUnit::Second => {
                    as_primitive_array::<TimestampSecondType>(arr).value_as_datetime(index)
                }
                TimeUnit::Millisecond => {
                    as_primitive_array::<TimestampMillisecondType>(arr).value_as_datetime(index)
                }
                TimeUnit::Microsecond => {
                    as_primitive_array::<TimestampMicrosecondType>(arr).value_as_datetime(index)
                }
                TimeUnit::Nanosecond => {
                    as_primitive_array::<TimestampNanosecondType>(arr).value_as_datetime(index)
                }
            },
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    as_primitive_array::<Time32SecondType>(arr).value_as_datetime(index)
                }
                TimeUnit::Millisecond => {
                    as_primitive_array::<Time32MillisecondType>(arr).value_as_datetime(index)
                }
                _ => None,
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    as_primitive_array::<Time64MicrosecondType>(arr).value_as_datetime(index)
                }
                TimeUnit::Nanosecond => {
                    as_primitive_array::<Time64NanosecondType>(arr).value_as_datetime(index)
                }
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::*;
    use arrow::array::{Time32SecondArray, Time64NanosecondArray, TimestampMillisecondArray};
    use chrono::{self, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

    #[test]
    fn test_time_roundtrip() {
        let time_1 = NaiveTime::from_hms_milli(12, 34, 56, 0);
        let time_2 = NaiveTime::from_hms_milli(12, 50, 0, 789);
        let time_3 = NaiveTime::from_hms_milli(14, 0, 56, 789);
        let time_4 = NaiveTime::from_hms_milli(0, 34, 30, 789);
        assert_eq!(
            NaiveTime::from(time_1.scalar_nanoseconds_from_midnight()),
            time_1
        );
        assert_eq!(
            NaiveTime::from(time_2.scalar_nanoseconds_from_midnight()),
            time_2
        );
        assert_eq!(
            NaiveTime::from(time_3.scalar_nanoseconds_from_midnight()),
            time_3
        );
        assert_eq!(
            NaiveTime::from(time_4.scalar_nanoseconds_from_midnight()),
            time_4
        );
        print!("{:?}", time_2.scalar_microseconds_from_midnight());
        assert_eq!(
            NaiveTime::from(time_2.scalar_microseconds_from_midnight()),
            time_2
        );
        assert_eq!(
            NaiveTime::from(time_2.scalar_milliseconds_from_midnight()),
            time_2
        );
        assert_eq!(
            NaiveTime::from(time_1.scalar_seconds_from_midnight()),
            time_1
        );
    }

    #[test]
    fn test_time_seconds_scalar() {
        let time_1 = NaiveTime::from_hms(12, 34, 56);
        let time_2 = NaiveTime::from_hms(12, 50, 56);
        let time_3 = NaiveTime::from_hms(14, 34, 56);
        let time_4 = NaiveTime::from_hms(14, 34, 30);
        let values = vec![
            Some(time_1.num_seconds_from_midnight() as i32),
            Some(time_2.num_seconds_from_midnight() as i32),
            None,
            Some(time_3.num_seconds_from_midnight() as i32),
            Some(time_4.num_seconds_from_midnight() as i32),
        ];
        let array = Time32SecondArray::from(values);
        let mut time_base = table_scalar::Time {
            unit: data_type_proto::TimeUnit::Second.into(),
            time: 0,
            tz: None,
        };
        time_base.time = time_1.num_seconds_from_midnight() as i64;
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time32(time_base.clone()))
            }
        );
        assert_eq!(array.value_as_time(0), Some(time_1));
        assert!(array.value_as_time(0).is_some());
        time_base.time = time_2.num_seconds_from_midnight() as i64;
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time32(time_base.clone()))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        time_base.time = time_3.num_seconds_from_midnight() as i64;
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time32(time_base.clone()))
            }
        );
        time_base.time = time_4.num_seconds_from_midnight() as i64;
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time32(time_base))
            }
        );
    }

    #[test]
    fn test_time_nanoseconds_scalar() {
        let time_1 = NaiveTime::from_hms_milli(12, 34, 56, 789);
        let time_2 = NaiveTime::from_hms_milli(12, 50, 56, 789);
        let time_3 = NaiveTime::from_hms_milli(14, 34, 56, 789);
        let time_4 = NaiveTime::from_hms_milli(14, 34, 30, 789);
        let values = vec![
            Some(time_1.scalar_nanoseconds_from_midnight().time),
            Some(time_2.scalar_nanoseconds_from_midnight().time),
            None,
            Some(time_3.scalar_nanoseconds_from_midnight().time),
            Some(time_4.scalar_nanoseconds_from_midnight().time),
        ];

        let array = Time64NanosecondArray::from(values);
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time64(
                    time_1.scalar_nanoseconds_from_midnight()
                ))
            }
        );
        assert_eq!(array.value_as_time(0), Some(time_1));
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time64(
                    time_2.scalar_nanoseconds_from_midnight()
                ))
            }
        );
        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time64(
                    time_3.scalar_nanoseconds_from_midnight()
                ))
            }
        );
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Time64(
                    time_4.scalar_nanoseconds_from_midnight()
                ))
            }
        );
    }

    #[test]
    fn test_timestamp_milliseconds_scalar() {
        let date = NaiveDate::from_ymd(2021, 2, 21);
        let time_1 = NaiveTime::from_hms_milli(12, 34, 56, 789);
        let datetime_1 = NaiveDateTime::new(date, time_1);
        let time_2 = NaiveTime::from_hms_milli(12, 50, 56, 789);
        let datetime_2 = NaiveDateTime::new(date, time_2);
        let time_3 = NaiveTime::from_hms_milli(14, 34, 56, 789);
        let datetime_3 = NaiveDateTime::new(date, time_3);
        let time_4 = NaiveTime::from_hms_milli(14, 34, 30, 789);
        let datetime_4 = NaiveDateTime::new(date, time_4);
        let values = vec![
            Some(datetime_1.timestamp_millis()),
            Some(datetime_2.timestamp_millis()),
            None,
            Some(datetime_3.timestamp_millis()),
            Some(datetime_4.timestamp_millis()),
        ];

        let array = TimestampMillisecondArray::from(values);
        let mut time_base = table_scalar::Time {
            unit: data_type_proto::TimeUnit::Millisecond.into(),
            time: 0,
            tz: None,
        };
        time_base.time = datetime_1.timestamp_millis();
        assert_eq!(
            array.scalar(0).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Timestamp(time_base.clone()))
            }
        );
        assert_eq!(array.value_as_datetime(0), Some(datetime_1));
        time_base.time = datetime_2.timestamp_millis();
        assert_eq!(
            array.scalar(1).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Timestamp(time_base.clone()))
            }
        );

        assert_eq!(array.scalar(2).unwrap(), TableScalar { value: None });
        time_base.time = datetime_3.timestamp_millis();
        assert_eq!(
            array.scalar(3).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Timestamp(time_base.clone()))
            }
        );
        time_base.time = datetime_4.timestamp_millis();
        assert_eq!(
            array.scalar(4).unwrap(),
            TableScalar {
                value: Some(table_scalar::Value::Timestamp(time_base))
            }
        );
    }
}
