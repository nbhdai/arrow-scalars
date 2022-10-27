#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldProto {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag="2")]
    pub data_type: ::core::option::Option<::prost::alloc::boxed::Box<DataTypeProto>>,
    #[prost(bool, tag="3")]
    pub nullable: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataTypeProto {
    #[prost(oneof="data_type_proto::DataType", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34")]
    pub data_type: ::core::option::Option<data_type_proto::DataType>,
}
/// Nested message and enum types in `DataTypeProto`.
pub mod data_type_proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EmptyMessage {
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Timestamp {
        #[prost(enumeration="TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(string, optional, tag="2")]
        pub tz: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FixedSizeList {
        #[prost(int32, tag="1")]
        pub size: i32,
        #[prost(message, optional, boxed, tag="2")]
        pub list_type: ::core::option::Option<::prost::alloc::boxed::Box<super::FieldProto>>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Struct {
        #[prost(message, repeated, tag="1")]
        pub fields: ::prost::alloc::vec::Vec<super::FieldProto>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Dictionary {
        #[prost(message, optional, boxed, tag="1")]
        pub key_type: ::core::option::Option<::prost::alloc::boxed::Box<super::DataTypeProto>>,
        #[prost(message, optional, boxed, tag="2")]
        pub value_type: ::core::option::Option<::prost::alloc::boxed::Box<super::DataTypeProto>>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Union {
        #[prost(message, repeated, tag="1")]
        pub fields: ::prost::alloc::vec::Vec<super::FieldProto>,
        #[prost(int32, repeated, tag="2")]
        pub type_ids: ::prost::alloc::vec::Vec<i32>,
        #[prost(enumeration="union::Mode", tag="3")]
        pub mode: i32,
    }
    /// Nested message and enum types in `Union`.
    pub mod union {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Mode {
            Sparse = 0,
            Dense = 1,
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Decimal {
        #[prost(int32, tag="1")]
        pub precision: i32,
        #[prost(int32, tag="2")]
        pub scale: i32,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Map {
        #[prost(message, optional, boxed, tag="1")]
        pub struct_field: ::core::option::Option<::prost::alloc::boxed::Box<super::FieldProto>>,
        #[prost(bool, tag="2")]
        pub keys_sorted: bool,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TimeUnit {
        Second = 0,
        Millisecond = 1,
        Microsecond = 2,
        Nanosecond = 3,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum IntervalUnit {
        YearMonth = 0,
        DayTime = 1,
        MonthDayNano = 2,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum DataType {
        #[prost(message, tag="1")]
        Null(EmptyMessage),
        #[prost(message, tag="2")]
        Bool(EmptyMessage),
        #[prost(message, tag="3")]
        Int8(EmptyMessage),
        #[prost(message, tag="4")]
        Int16(EmptyMessage),
        #[prost(message, tag="5")]
        Int32(EmptyMessage),
        #[prost(message, tag="6")]
        Int64(EmptyMessage),
        #[prost(message, tag="7")]
        Uint8(EmptyMessage),
        #[prost(message, tag="8")]
        Uint16(EmptyMessage),
        #[prost(message, tag="9")]
        Uint32(EmptyMessage),
        #[prost(message, tag="10")]
        Uint64(EmptyMessage),
        #[prost(message, tag="11")]
        Float16(EmptyMessage),
        #[prost(message, tag="12")]
        Float32(EmptyMessage),
        #[prost(message, tag="13")]
        Float64(EmptyMessage),
        #[prost(message, tag="14")]
        Date32(EmptyMessage),
        #[prost(message, tag="15")]
        Date64(EmptyMessage),
        #[prost(enumeration="TimeUnit", tag="16")]
        Time32(i32),
        #[prost(enumeration="TimeUnit", tag="17")]
        Time64(i32),
        #[prost(message, tag="18")]
        Timestamp(Timestamp),
        #[prost(enumeration="IntervalUnit", tag="19")]
        Interval(i32),
        #[prost(enumeration="TimeUnit", tag="20")]
        Duration(i32),
        #[prost(message, tag="21")]
        Binary(EmptyMessage),
        #[prost(message, tag="22")]
        LargeBinary(EmptyMessage),
        #[prost(int32, tag="23")]
        FixedSizeBinary(i32),
        #[prost(message, tag="24")]
        Utf8(EmptyMessage),
        #[prost(message, tag="25")]
        LargeUtf8(EmptyMessage),
        #[prost(message, tag="26")]
        List(::prost::alloc::boxed::Box<super::FieldProto>),
        #[prost(message, tag="27")]
        LargeList(::prost::alloc::boxed::Box<super::FieldProto>),
        #[prost(message, tag="28")]
        FixedSizeList(::prost::alloc::boxed::Box<FixedSizeList>),
        #[prost(message, tag="29")]
        Struct(Struct),
        #[prost(message, tag="30")]
        Dictionary(::prost::alloc::boxed::Box<Dictionary>),
        #[prost(message, tag="31")]
        Union(Union),
        #[prost(message, tag="32")]
        Decimal128(Decimal),
        #[prost(message, tag="33")]
        Decimal256(Decimal),
        #[prost(message, tag="34")]
        Map(::prost::alloc::boxed::Box<Map>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableScalar {
    #[prost(oneof="table_scalar::Value", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32")]
    pub value: ::core::option::Option<table_scalar::Value>,
}
/// Nested message and enum types in `TableScalar`.
pub mod table_scalar {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Time {
        #[prost(enumeration="super::data_type_proto::TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, tag="2")]
        pub time: i64,
        #[prost(string, optional, tag="3")]
        pub tz: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Duration {
        #[prost(enumeration="super::data_type_proto::TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, tag="2")]
        pub duration: i64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Interval {
        #[prost(oneof="interval::Interval", tags="1, 2, 3")]
        pub interval: ::core::option::Option<interval::Interval>,
    }
    /// Nested message and enum types in `Interval`.
    pub mod interval {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Interval {
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
        #[prost(map="string, message", tag="1")]
        pub elements: ::std::collections::HashMap<::prost::alloc::string::String, super::TableScalar>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Union {
        #[prost(int32, tag="1")]
        pub type_id: i32,
        #[prost(message, optional, tag="2")]
        pub value: ::core::option::Option<super::TableScalar>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Map {
        #[prost(message, optional, tag="1")]
        pub keys: ::core::option::Option<super::TableList>,
        #[prost(message, optional, tag="2")]
        pub values: ::core::option::Option<super::TableList>,
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
        #[prost(message, tag="20")]
        Duration(Duration),
        #[prost(bytes, tag="21")]
        Binary(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag="22")]
        FixedSizeBinary(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag="23")]
        LargeBinary(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag="24")]
        Utf8(::prost::alloc::string::String),
        #[prost(string, tag="25")]
        LargeUtf8(::prost::alloc::string::String),
        #[prost(message, tag="26")]
        Struct(Struct),
        #[prost(message, tag="27")]
        Union(::prost::alloc::boxed::Box<super::TableScalar>),
        /// We don't care about the exact index in dictionaries, so we don't encode it.
        #[prost(message, tag="28")]
        Dictionary(::prost::alloc::boxed::Box<super::TableScalar>),
        #[prost(message, tag="29")]
        List(super::TableList),
        #[prost(message, tag="30")]
        FixedSizeList(super::TableList),
        #[prost(message, tag="31")]
        LargeList(super::TableList),
        #[prost(message, tag="32")]
        Map(Map),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableList {
    #[prost(oneof="table_list::Values", tags="1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31")]
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
        #[prost(enumeration="super::data_type_proto::TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, repeated, tag="2")]
        pub times: ::prost::alloc::vec::Vec<i64>,
        #[prost(string, optional, tag="3")]
        pub tz: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(bool, repeated, tag="4")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DurationList {
        #[prost(enumeration="super::data_type_proto::TimeUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, repeated, tag="2")]
        pub durations: ::prost::alloc::vec::Vec<i64>,
        #[prost(bool, repeated, tag="3")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct IntervalList {
        #[prost(enumeration="super::data_type_proto::IntervalUnit", tag="1")]
        pub unit: i32,
        #[prost(int64, repeated, tag="2")]
        pub intervals: ::prost::alloc::vec::Vec<i64>,
        #[prost(bool, repeated, tag="3")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BinaryList {
        #[prost(bytes="vec", repeated, tag="1")]
        pub values: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
        #[prost(bool, repeated, tag="2")]
        pub set: ::prost::alloc::vec::Vec<bool>,
        #[prost(int32, optional, tag="3")]
        pub size: ::core::option::Option<i32>,
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
        #[prost(message, optional, boxed, tag="1")]
        pub values: ::core::option::Option<::prost::alloc::boxed::Box<super::TableList>>,
        #[prost(message, optional, tag="2")]
        pub index_type: ::core::option::Option<super::DataTypeProto>,
        #[prost(bool, repeated, tag="3")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StructList {
        #[prost(map="string, message", tag="1")]
        pub values: ::std::collections::HashMap<::prost::alloc::string::String, super::TableList>,
        #[prost(bool, repeated, tag="2")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UnionList {
        #[prost(message, repeated, tag="1")]
        pub values: ::prost::alloc::vec::Vec<super::TableScalar>,
        #[prost(bool, repeated, tag="2")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ListList {
        #[prost(message, repeated, tag="1")]
        pub values: ::prost::alloc::vec::Vec<super::TableList>,
        #[prost(bool, repeated, tag="2")]
        pub set: ::prost::alloc::vec::Vec<bool>,
        #[prost(message, optional, tag="3")]
        pub list_type: ::core::option::Option<super::FieldProto>,
        #[prost(int32, optional, tag="4")]
        pub size: ::core::option::Option<i32>,
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
        Duration(DurationList),
        #[prost(message, tag="20")]
        Interval(IntervalList),
        #[prost(message, tag="21")]
        Binary(BinaryList),
        #[prost(message, tag="22")]
        LargeBinary(BinaryList),
        #[prost(message, tag="23")]
        FixedSizeBinary(BinaryList),
        #[prost(message, tag="24")]
        Utf8(Utf8List),
        #[prost(message, tag="25")]
        LargeUtf8(Utf8List),
        #[prost(message, tag="26")]
        List(ListList),
        #[prost(message, tag="27")]
        LargeList(ListList),
        #[prost(message, tag="28")]
        FixedSizeList(ListList),
        #[prost(message, tag="29")]
        Union(UnionList),
        #[prost(message, tag="30")]
        Dictionary(::prost::alloc::boxed::Box<DictionaryList>),
        #[prost(message, tag="31")]
        Struct(StructList),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableRow {
    #[prost(map="string, message", tag="1")]
    pub values: ::std::collections::HashMap<::prost::alloc::string::String, TableScalar>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Table {
    #[prost(map="string, message", tag="1")]
    pub values: ::std::collections::HashMap<::prost::alloc::string::String, TableList>,
}
