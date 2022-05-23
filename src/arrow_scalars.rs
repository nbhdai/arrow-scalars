#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableScalar {
    #[prost(oneof="table_scalar::Value", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26")]
    pub value: ::core::option::Option<table_scalar::Value>,
}
/// Nested message and enum types in `TableScalar`.
pub mod table_scalar {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Time {
        #[prost(enumeration="TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, tag="2")]
        pub time: i64,
        #[prost(string, tag="3")]
        pub tz: ::prost::alloc::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Interval {
        #[prost(oneof="interval::Unit", tags="1, 2, 3")]
        pub unit: ::core::option::Option<interval::Unit>,
    }
    /// Nested message and enum types in `Interval`.
    pub mod interval {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Unit {
            #[prost(int32, tag="1")]
            YearMonth(i32),
            #[prost(int64, tag="2")]
            DayTime(i64),
            /// le encoded u128
            #[prost(bytes, tag="3")]
            MonthDayNano(::prost::alloc::vec::Vec<u8>),
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Struct {
        #[prost(message, repeated, tag="1")]
        pub elements: ::prost::alloc::vec::Vec<super::TableScalar>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Map {
        #[prost(message, optional, tag="1")]
        pub keys: ::core::option::Option<TableList>,
        #[prost(message, optional, tag="2")]
        pub values: ::core::option::Option<TableList>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TableList {
        #[prost(oneof="table_list::Values", tags="1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24")]
        pub values: ::core::option::Option<table_list::Values>,
    }
    /// Nested message and enum types in `TableList`.
    pub mod table_list {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct BooleanList {
            #[prost(bool, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<bool>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Int8List {
            #[prost(int32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<i32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Int16List {
            #[prost(int32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<i32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Int32List {
            #[prost(int32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<i32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Int64List {
            #[prost(int64, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<i64>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct UInt8List {
            #[prost(uint32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<u32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct UInt16List {
            #[prost(uint32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<u32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct UInt32List {
            #[prost(uint32, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<u32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct UInt64List {
            #[prost(uint64, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<u64>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Float16List {
            #[prost(float, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<f32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Float32List {
            #[prost(float, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<f32>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Float64List {
            #[prost(double, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<f64>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TimeList {
            #[prost(enumeration="super::TimeUnit", tag="1")]
            pub unit: i32,
            #[prost(int64, repeated, tag="2")]
            pub times: ::prost::alloc::vec::Vec<i64>,
            #[prost(string, tag="3")]
            pub tz: ::prost::alloc::string::String,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct IntervalList {
            #[prost(message, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<super::Interval>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct BinaryList {
            #[prost(bytes="vec", repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Utf8List {
            #[prost(string, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct DictionaryList {
            #[prost(message, optional, tag="1")]
            pub values: ::core::option::Option<super::TableList>,
            #[prost(enumeration="super::super::PrimitiveDataType", tag="2")]
            pub index_type: i32,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct StructList {
            #[prost(message, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<super::TableList>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct ListList {
            #[prost(message, repeated, tag="1")]
            pub values: ::prost::alloc::vec::Vec<super::TableList>,
            #[prost(bool, repeated, tag="2")]
            pub set: ::prost::alloc::vec::Vec<bool>,
            #[prost(enumeration="super::super::PrimitiveDataType", tag="3")]
            pub list_type: i32,
        }
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Values {
            #[prost(message, tag="1")]
            Boolean(BooleanList),
            #[prost(message, tag="3")]
            Int8(Int8List),
            #[prost(message, tag="4")]
            Int16(Int16List),
            #[prost(message, tag="5")]
            Int32(Int32List),
            #[prost(message, tag="6")]
            Int64(Int64List),
            #[prost(message, tag="7")]
            Uint8(UInt8List),
            #[prost(message, tag="8")]
            Uint16(UInt16List),
            #[prost(message, tag="9")]
            Uint32(UInt32List),
            #[prost(message, tag="10")]
            Uint64(UInt64List),
            #[prost(message, tag="11")]
            Float16(Float16List),
            #[prost(message, tag="12")]
            Float32(Float32List),
            #[prost(message, tag="13")]
            Float64(Float64List),
            #[prost(message, tag="14")]
            Timestamp(TimeList),
            #[prost(message, tag="15")]
            Date32(Int32List),
            #[prost(message, tag="16")]
            Date64(Int64List),
            #[prost(message, tag="17")]
            Time32(TimeList),
            #[prost(message, tag="18")]
            Time64(TimeList),
            #[prost(message, tag="19")]
            Interval(IntervalList),
            #[prost(message, tag="20")]
            Binary(BinaryList),
            #[prost(message, tag="21")]
            Utf8(Utf8List),
            #[prost(message, tag="22")]
            LargeUtf8(Utf8List),
            #[prost(message, tag="23")]
            List(ListList),
            #[prost(message, tag="24")]
            LargeList(ListList),
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TimeUnit {
        Second = 0,
        Millisecond = 1,
        Microsecond = 2,
        Nanosecond = 3,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag="1")]
        Null(bool),
        #[prost(bool, tag="2")]
        Boolean(bool),
        #[prost(int32, tag="3")]
        Int8(i32),
        #[prost(int32, tag="4")]
        Int16(i32),
        #[prost(int32, tag="5")]
        Int32(i32),
        #[prost(int64, tag="6")]
        Int64(i64),
        #[prost(uint32, tag="7")]
        Uint8(u32),
        #[prost(uint32, tag="8")]
        Uint16(u32),
        #[prost(uint32, tag="9")]
        Uint32(u32),
        #[prost(uint64, tag="10")]
        Uint64(u64),
        #[prost(float, tag="11")]
        Float16(f32),
        #[prost(float, tag="12")]
        Float32(f32),
        #[prost(double, tag="13")]
        Float64(f64),
        #[prost(message, tag="14")]
        Timestamp(Time),
        #[prost(int32, tag="15")]
        Date32(i32),
        #[prost(int64, tag="16")]
        Date64(i64),
        #[prost(message, tag="17")]
        Time32(Time),
        #[prost(message, tag="18")]
        Time64(Time),
        #[prost(message, tag="19")]
        Interval(Interval),
        #[prost(bytes, tag="20")]
        Binary(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag="21")]
        Utf8(::prost::alloc::string::String),
        #[prost(string, tag="22")]
        LargeUtf8(::prost::alloc::string::String),
        #[prost(message, tag="23")]
        Struct(Struct),
        /// We don't care about the exact index in dictionaries, so we don't encode it.
        #[prost(message, tag="24")]
        Dictionary(::prost::alloc::boxed::Box<super::TableScalar>),
        #[prost(message, tag="25")]
        List(TableList),
        #[prost(message, tag="26")]
        Map(Map),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PrimitiveDataType {
    Null = 0,
    Boolean = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    Uint8 = 6,
    Uint16 = 7,
    Uint32 = 8,
    Uint64 = 9,
    Float16 = 10,
    Float32 = 11,
    Float64 = 12,
    Timestamp = 13,
    Date32 = 14,
    Date64 = 15,
    Time32 = 16,
    Time64 = 17,
    Interval = 18,
    Binary = 19,
    Utf8 = 20,
    LargeUtf8 = 21,
}
