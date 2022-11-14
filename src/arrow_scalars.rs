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
pub struct SchemaProto {
    #[prost(message, repeated, tag="1")]
    pub fields: ::prost::alloc::vec::Vec<FieldProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataTypeProto {
    #[prost(oneof="data_type_proto::DataType", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44")]
    pub data_type: ::core::option::Option<data_type_proto::DataType>,
}
/// Nested message and enum types in `DataTypeProto`.
pub mod data_type_proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EmptyMessage {
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Timestamp {
        #[prost(string, optional, tag="1")]
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
        #[prost(message, tag="16")]
        Time32Second(EmptyMessage),
        #[prost(message, tag="17")]
        Time32Millisecond(EmptyMessage),
        #[prost(message, tag="18")]
        Time64Microsecond(EmptyMessage),
        #[prost(message, tag="19")]
        Time64Nanosecond(EmptyMessage),
        #[prost(message, tag="20")]
        TimestampSecond(Timestamp),
        #[prost(message, tag="21")]
        TimestampMillisecond(Timestamp),
        #[prost(message, tag="22")]
        TimestampMicrosecond(Timestamp),
        #[prost(message, tag="23")]
        TimestampNanosecond(Timestamp),
        #[prost(message, tag="24")]
        DurationSecond(EmptyMessage),
        #[prost(message, tag="25")]
        DurationMillisecond(EmptyMessage),
        #[prost(message, tag="26")]
        DurationMicrosecond(EmptyMessage),
        #[prost(message, tag="27")]
        DurationNanosecond(EmptyMessage),
        #[prost(message, tag="28")]
        IntervalYearMonth(EmptyMessage),
        #[prost(message, tag="29")]
        IntervalDayTime(EmptyMessage),
        #[prost(message, tag="30")]
        IntervalMonthDayNano(EmptyMessage),
        #[prost(message, tag="31")]
        Binary(EmptyMessage),
        #[prost(message, tag="32")]
        LargeBinary(EmptyMessage),
        #[prost(int32, tag="33")]
        FixedSizeBinary(i32),
        #[prost(message, tag="34")]
        Utf8(EmptyMessage),
        #[prost(message, tag="35")]
        LargeUtf8(EmptyMessage),
        #[prost(message, tag="36")]
        List(::prost::alloc::boxed::Box<super::FieldProto>),
        #[prost(message, tag="37")]
        LargeList(::prost::alloc::boxed::Box<super::FieldProto>),
        #[prost(message, tag="38")]
        FixedSizeList(::prost::alloc::boxed::Box<FixedSizeList>),
        #[prost(message, tag="39")]
        Struct(Struct),
        #[prost(message, tag="40")]
        Dictionary(::prost::alloc::boxed::Box<Dictionary>),
        #[prost(message, tag="41")]
        Union(Union),
        #[prost(message, tag="42")]
        Decimal128(Decimal),
        #[prost(message, tag="43")]
        Decimal256(Decimal),
        #[prost(message, tag="44")]
        Map(::prost::alloc::boxed::Box<Map>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableScalar {
    #[prost(oneof="table_scalar::Value", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42")]
    pub value: ::core::option::Option<table_scalar::Value>,
}
/// Nested message and enum types in `TableScalar`.
pub mod table_scalar {
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
        #[prost(int32, tag="15")]
        Date32(i32),
        #[prost(int64, tag="16")]
        Date64(i64),
        #[prost(int32, tag="17")]
        Time32Second(i32),
        #[prost(int32, tag="18")]
        Time32Millisecond(i32),
        #[prost(int64, tag="19")]
        Time64Microsecond(i64),
        #[prost(int64, tag="20")]
        Time64Nanosecond(i64),
        #[prost(int64, tag="21")]
        TimestampSecond(i64),
        #[prost(int64, tag="22")]
        TimestampMillisecond(i64),
        #[prost(int64, tag="23")]
        TimestampMicrosecond(i64),
        #[prost(int64, tag="24")]
        TimestampNanosecond(i64),
        #[prost(int64, tag="25")]
        DurationSecond(i64),
        #[prost(int64, tag="26")]
        DurationMillisecond(i64),
        #[prost(int64, tag="27")]
        DurationMicrosecond(i64),
        #[prost(int64, tag="28")]
        DurationNanosecond(i64),
        #[prost(int32, tag="29")]
        IntervalYearMonth(i32),
        #[prost(int64, tag="30")]
        IntervalDayTime(i64),
        #[prost(bytes, tag="31")]
        Binary(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag="32")]
        FixedSizeBinary(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag="33")]
        LargeBinary(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag="34")]
        Utf8(::prost::alloc::string::String),
        #[prost(string, tag="35")]
        LargeUtf8(::prost::alloc::string::String),
        #[prost(message, tag="36")]
        Struct(Struct),
        #[prost(message, tag="37")]
        Union(::prost::alloc::boxed::Box<super::TableScalar>),
        /// We don't care about the exact index in dictionaries, so we don't encode it.
        #[prost(message, tag="38")]
        Dictionary(::prost::alloc::boxed::Box<super::TableScalar>),
        #[prost(message, tag="39")]
        List(super::TableList),
        #[prost(message, tag="40")]
        FixedSizeList(super::TableList),
        #[prost(message, tag="41")]
        LargeList(super::TableList),
        #[prost(message, tag="42")]
        Map(Map),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableList {
    #[prost(oneof="table_list::Values", tags="2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40")]
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
        #[prost(int64, repeated, tag="2")]
        pub times: ::prost::alloc::vec::Vec<i64>,
        #[prost(string, optional, tag="3")]
        pub tz: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(bool, repeated, tag="4")]
        pub set: ::prost::alloc::vec::Vec<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DurationList {
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
        #[prost(message, repeated, tag="1")]
        pub fields: ::prost::alloc::vec::Vec<super::FieldProto>,
        #[prost(message, repeated, tag="2")]
        pub values: ::prost::alloc::vec::Vec<super::TableList>,
        #[prost(bool, repeated, tag="3")]
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
        #[prost(message, optional, tag="2")]
        pub list_type: ::core::option::Option<super::FieldProto>,
        #[prost(int32, optional, tag="3")]
        pub size: ::core::option::Option<i32>,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Values {
        #[prost(message, tag="2")]
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
        Date32(Int32List),
        #[prost(message, tag="15")]
        Date64(Int64List),
        #[prost(message, tag="16")]
        Time32Second(Int32List),
        #[prost(message, tag="17")]
        Time32Millisecond(Int32List),
        #[prost(message, tag="18")]
        Time64Microsecond(Int64List),
        #[prost(message, tag="19")]
        Time64Nanosecond(Int64List),
        #[prost(message, tag="20")]
        TimestampSecond(TimeList),
        #[prost(message, tag="21")]
        TimestampMillisecond(TimeList),
        #[prost(message, tag="22")]
        TimestampMicrosecond(TimeList),
        #[prost(message, tag="23")]
        TimestampNanosecond(TimeList),
        #[prost(message, tag="24")]
        DurationSecond(Int64List),
        #[prost(message, tag="25")]
        DurationMillisecond(Int64List),
        #[prost(message, tag="26")]
        DurationMicrosecond(Int64List),
        #[prost(message, tag="27")]
        DurationNanosecond(Int64List),
        #[prost(message, tag="28")]
        IntervalYearMonth(Int32List),
        #[prost(message, tag="29")]
        IntervalDayTime(Int64List),
        #[prost(message, tag="30")]
        Binary(BinaryList),
        #[prost(message, tag="31")]
        LargeBinary(BinaryList),
        #[prost(message, tag="32")]
        FixedSizeBinary(BinaryList),
        #[prost(message, tag="33")]
        Utf8(Utf8List),
        #[prost(message, tag="34")]
        LargeUtf8(Utf8List),
        #[prost(message, tag="35")]
        List(ListList),
        #[prost(message, tag="36")]
        LargeList(ListList),
        #[prost(message, tag="37")]
        FixedSizeList(ListList),
        #[prost(message, tag="38")]
        Union(UnionList),
        #[prost(message, tag="39")]
        Dictionary(::prost::alloc::boxed::Box<DictionaryList>),
        #[prost(message, tag="40")]
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
    #[prost(message, repeated, tag="1")]
    pub fields: ::prost::alloc::vec::Vec<FieldProto>,
    #[prost(message, repeated, tag="2")]
    pub values: ::prost::alloc::vec::Vec<TableList>,
}
