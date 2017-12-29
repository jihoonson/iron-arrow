use std::ops::Index;

use common::status::ArrowError;
use common::field::{Field};
use array;
use array::Array;

#[macro_use]
use std;
use std::mem;
use std::fmt::{Debug, Formatter, Error};

/// Data types in this library are all *logical*. They can be expressed as
/// either a primitive physical type (bytes or bits of some fixed size), a
/// nested type consisting of other data types, or another data type (e.g. a
/// timestamp encoded as an int64)
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Ty {
  // A degenerate NULL type represented as 0 bytes/bits
  NA,

  // A boolean value represented as 1 bit
  Bool,

  // Little-endian integer types
  UInt8,
  Int8,
  UInt16,
  Int16,
  UInt32,
  Int32,
  UInt64,
  Int64,

  // 2-byte floating point value
  HalfFloat,

  // 4-byte floating point value
  Float,

  // 8-byte floating point value
  Double,

  // UTF8 variable-length string as List<Char>
  String,

  // Variable-length bytes (no guarantee of UTF8-ness)
  Binary,

  // Fixed-size binary. Each value occupies the same number of bytes
  FixedSizedBinary,

  // int64_t milliseconds since the UNIX epoch
  Date64,

  // int32_t days since the UNIX epoch
  Date32,

  // Exact timestamp encoded with int64 since UNIX epoch
  // Default unit millisecond
  Timestamp,

  // Time as signed 32-bit integer, representing either seconds or
  // milliseconds since midnight
  Time32,

  // Time as signed 64-bit integer, representing either microseconds or
  // nanoseconds since midnight
  Time64,

  // YearMonth or DayTime interval in SQL style
  Interval,

  // Precision- and scale-based decimal type. Storage type depends on the
  // parameters.
  Decimal,

  // A list of some logical data type
  List,

  // Struct of logical types
  Struct,

  // Unions of logical types
  Union,

  // Dictionary aka Category type
  Dictionary
}

/// Data types in this library are all *logical*. They can be expressed as
/// either a primitive physical type (bytes or bits of some fixed size), a
/// nested type consisting of other data types, or another data type (e.g. a
/// timestamp encoded as an int64)
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DataType {
  // A degenerate NULL type represented as 0 bytes/bits
  Null,

  // A boolean value represented as 1 bit
  Bool,

  // Little-endian integer types
  UInt8,
  Int8,
  UInt16,
  Int16,
  UInt32,
  Int32,
  UInt64,
  Int64,

  // 2-byte floating point value
  HalfFloat,

  // 4-byte floating point value
  Float,

  // 8-byte floating point value
  Double,

  // UTF8 variable-length string as List<Char>
  String,

  // Variable-length bytes (no guarantee of UTF8-ness)
  Binary,

  // Fixed-size binary. Each value occupies the same number of bytes
  FixedSizedBinary {
    byte_width: i32
  },

  // int64_t milliseconds since the UNIX epoch
  Date64 {
    unit: DateUnit
  },

  // int32_t days since the UNIX epoch
  Date32 {
    unit: DateUnit
  },

  // Exact timestamp encoded with int64 since UNIX epoch
  // Default unit millisecond
  Timestamp {
    unit: TimeUnit,
    timezone: String
  },

  // Time as signed 32-bit integer, representing either seconds or
  // milliseconds since midnight
  Time32 {
    unit: TimeUnit
  },

  // Time as signed 64-bit integer, representing either microseconds or
  // nanoseconds since midnight
  Time64 {
    unit: TimeUnit
  },

  // YearMonth or DayTime interval in SQL style
  Interval {
    unit: IntervalUnit
  },

  // Precision- and scale-based decimal type. Storage type depends on the
  // parameters.
  Decimal {
    precision: i32,
    scale: i32
  },

  // A list of some logical data type
  List {
    value_type: Box<DataType>
  },

  // Struct of logical types
  Struct {
    fields: Vec<Field>
  },

  // Unions of logical types
  Union {
    fields: Vec<Field>,
    type_codes: Vec<u8>,
    mode: UnionMode
  },

  // Dictionary aka Category type
  Dictionary {
    index_type: Box<DataType>,
    dictionary: Box<Array>,
    ordered: bool
  }
}

impl Ty {
  pub fn is_integer(&self) -> bool {
    match self {
      &Ty::Int8 => true,
      &Ty::Int16 => true,
      &Ty::Int32 => true,
      &Ty::Int64 => true,
      &Ty::UInt8 => true,
      &Ty::UInt16 => true,
      &Ty::UInt32 => true,
      &Ty::UInt64 => true,
      _ => false
    }
  }

  pub fn is_signed(&self) -> bool {
    match self {
      &Ty::UInt8 => false,
      &Ty::UInt16 => false,
      &Ty::UInt32 => false,
      &Ty::UInt64 => false,
      &Ty::Int8 => true,
      &Ty::Int16 => true,
      &Ty::Int32 => true,
      &Ty::Int64 => true,
      _ => panic!("{:?} is not an integer", self)
    }
  }

  pub fn is_float(&self) -> bool {
    match self {
      &Ty::HalfFloat => true,
      &Ty::Float => true,
      &Ty::Double => true,
      _ => false
    }
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TimeUnit {
  Second,
  Milli,
  Micro,
  Nano
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BufferType {
  Data,
  Offset,
  Type,
  Validity
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BufferDesc {
  ty: BufferType,
  bit_width: i32
}

impl BufferDesc {
  pub fn new(ty: BufferType, bit_width: i32) -> BufferDesc {
    BufferDesc {
      ty,
      bit_width
    }
  }

  pub fn k_validity_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Validity,
      bit_width: 1
    }
  }

  pub fn k_offset_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Offset,
      bit_width: 32
    }
  }

  pub fn k_type_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Type,
      bit_width: 32
    }
  }

  pub fn k_data_buffer(bit_width: i32) -> BufferDesc {
    BufferDesc {
      ty: BufferType::Data,
      bit_width
    }
  }
}

impl DataType {
  pub fn null() -> DataType {
    DataType::Null
  }

  pub fn bool() -> DataType {
    DataType::Bool
  }

  pub fn uint8() -> DataType {
    DataType::UInt8
  }

  pub fn int8() -> DataType {
    DataType::Int8
  }

  pub fn uint16() -> DataType {
    DataType::UInt16
  }

  pub fn int16() -> DataType {
    DataType::Int16
  }

  pub fn uint32() -> DataType {
    DataType::UInt32
  }

  pub fn int32() -> DataType {
    DataType::Int32
  }

  pub fn uint64() -> DataType {
    DataType::UInt64
  }

  pub fn int64() -> DataType {
    DataType::Int64
  }

  pub fn halffloat() -> DataType {
    DataType::HalfFloat
  }

  pub fn float() -> DataType {
    DataType::Float
  }

  pub fn double() -> DataType {
    DataType::Double
  }

  pub fn string() -> DataType {
    DataType::String
  }

  pub fn binary() -> DataType {
    DataType::Binary
  }

  pub fn fixed_sized_binary(byte_width: i32) -> DataType {
    DataType::FixedSizedBinary {
      byte_width
    }
  }

  pub fn date64() -> DataType {
    DataType::Date64 {
      unit: DateUnit::Milli
    }
  }

  pub fn date64_with_unit(unit: DateUnit) -> DataType {
    DataType::Date64 {
      unit
    }
  }

  pub fn date32() -> DataType {
    DataType::Date32 {
      unit: DateUnit::Milli
    }
  }

  pub fn date32_with_unit(unit: DateUnit) -> DataType {
    DataType::Date32 {
      unit
    }
  }

  pub fn timestamp() -> DataType {
    DataType::Timestamp {
      unit: TimeUnit::Milli,
      timezone: String::new(),
    }
  }

  pub fn timestamp_with_unit(unit: TimeUnit) -> DataType {
    DataType::Timestamp {
      unit,
      timezone: String::new()
    }
  }

  pub fn timestamp_with_timezone(timezone: String) -> DataType {
    DataType::Timestamp {
      unit: TimeUnit::Milli,
      timezone
    }
  }

  pub fn timestamp_with_unit_and_timestamp(unit: TimeUnit, timezone: String) -> DataType {
    DataType::Timestamp {
      unit,
      timezone
    }
  }

  pub fn time32() -> DataType {
    DataType::Time32 {
      unit: TimeUnit::Milli
    }
  }

  pub fn time32_with_unit(unit: TimeUnit) -> DataType {
    DataType::Time32 {
      unit
    }
  }

  pub fn time64() -> DataType {
    DataType::Time64 {
      unit: TimeUnit::Milli
    }
  }

  pub fn time64_with_unit(unit: TimeUnit) -> DataType {
    DataType::Time64 {
      unit
    }
  }

  pub fn interval() -> DataType {
    DataType::Interval {
      unit: IntervalUnit::YearMonth
    }
  }

  pub fn interval_with_unit(unit: IntervalUnit) -> DataType {
    DataType::Interval {
      unit
    }
  }

  pub fn decimal(precision: i32, scale: i32) -> DataType {
    DataType::Decimal {
      precision,
      scale
    }
  }

  pub fn list(value_type: Box<DataType>) -> DataType {
    DataType::List {
      value_type
    }
  }

  pub fn struct_type(fields: Vec<Field>) -> DataType {
    DataType::Struct {
      fields
    }
  }

  pub fn union(fields: Vec<Field>, type_codes: Vec<u8>) -> DataType {
    DataType::Union {
      fields,
      type_codes,
      mode: UnionMode::SPARSE
    }
  }

  pub fn union_with_mode(fields: Vec<Field>, type_codes: Vec<u8>, mode: UnionMode) -> DataType {
    DataType::Union {
      fields,
      type_codes,
      mode
    }
  }

  pub fn dictionary(index_type: Box<DataType>, dictionary: Box<Array>) -> DataType {
    if !index_type.is_integer() {
      panic!("index type [{:?}] is not an integer", index_type.ty())
    }

    DataType::Dictionary {
      index_type,
      dictionary,
      ordered: false
    }
  }

  pub fn ordered_dictionary(index_type: Box<DataType>, dictionary: Box<Array>) -> DataType {
    if !index_type.is_integer() {
      panic!("index type [{:?}] is not an integer", index_type.ty())
    }

    DataType::Dictionary {
      index_type,
      dictionary,
      ordered: true
    }
  }

  pub fn ty(&self) -> Ty {
    match self {
      &DataType::Null => Ty::NA,
      &DataType::Bool => Ty::Bool,
      &DataType::UInt8 => Ty::UInt8,
      &DataType::Int8 => Ty::Int8,
      &DataType::UInt16 => Ty::UInt16,
      &DataType::Int16 => Ty::Int16,
      &DataType::UInt32 => Ty::UInt32,
      &DataType::Int32 => Ty::Int32,
      &DataType::UInt64 => Ty::UInt64,
      &DataType::Int64 => Ty::Int64,
      &DataType::HalfFloat => Ty::HalfFloat,
      &DataType::Float => Ty::Float,
      &DataType::Double => Ty::Double,
      &DataType::String => Ty::String,
      &DataType::Binary => Ty::Binary,
      &DataType::FixedSizedBinary { byte_width } => Ty::FixedSizedBinary,
      &DataType::Date32 { ref unit } => Ty::Date32,
      &DataType::Date64 { ref unit } => Ty::Date64,
      &DataType::Timestamp  { ref unit, ref timezone } => Ty::Timestamp,
      &DataType::Time32 { ref unit } => Ty::Time32,
      &DataType::Time64 { ref unit } => Ty::Time64,
      &DataType::Interval { ref unit } => Ty::Interval,
      &DataType::Decimal { precision, scale } => Ty::Decimal,
      &DataType::List { ref value_type } => Ty::List,
      &DataType::Struct { ref fields } => Ty::Struct,
      &DataType::Union { ref fields, ref type_codes, ref mode } => Ty::Union,
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => Ty::Dictionary,
    }
  }

  pub fn bit_width(&self) -> i32 {
    match self {
      &DataType::Bool => 1,

      &DataType::UInt8 => 8,
      &DataType::Int8 => 8,
      &DataType::UInt16 => 16,
      &DataType::Int16 => 16,
      &DataType::UInt32 => 32,
      &DataType::Int32 => 32,
      &DataType::UInt64 => 64,
      &DataType::Int64 => 64,

      &DataType::HalfFloat => 16,
      &DataType::Float => 32,
      &DataType::Double => 64,

      &DataType::FixedSizedBinary { byte_width } => byte_width * 8,

      &DataType::Date32 { ref unit } => 32,
      &DataType::Date64 { ref unit } => 64,

      &DataType::Timestamp { ref unit, ref timezone } => 64,
      &DataType::Time32 { ref unit } => 32,
      &DataType::Time64 { ref unit } => 64,
      &DataType::Interval { ref unit } => 64,

      &DataType::Decimal { precision, scale } => 16 * 8,

      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => index_type.bit_width(),

      _ => panic!("{:?} is not fixed width type", self.ty())
    }
  }

  pub fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    match self {
      &DataType::Null => Vec::new(),
      &DataType::Bool => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 1)],

      &DataType::UInt8 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &DataType::Int8 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &DataType::UInt16 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &DataType::Int16 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &DataType::UInt32 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &DataType::Int32 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &DataType::UInt64 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &DataType::Int64 => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &DataType::HalfFloat => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &DataType::Float => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &DataType::Double => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &DataType::String => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &DataType::Binary => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)],

      &DataType::FixedSizedBinary { byte_width } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, byte_width * 8)],

      &DataType::Date32 { ref unit } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &DataType::Date64 { ref unit } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &DataType::Timestamp { ref unit, ref timezone } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &DataType::Time32 { ref unit } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &DataType::Time64 { ref unit } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &DataType::Interval { ref unit } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &DataType::Decimal { precision, scale } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 16 * 8)],

      &DataType::List { ref value_type } => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer()],
      &DataType::Struct { ref fields } => vec![BufferDesc::k_validity_buffer()],
      &DataType::Union { ref fields, ref type_codes, ref mode } => {
        match mode {
          &UnionMode::SPARSE => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer()],
          _ => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer(), BufferDesc::k_offset_buffer()]
        }
      },
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
    }
  }

  pub fn name(&self) -> &str {
    match self {
      &DataType::Null => "null",
      &DataType::Bool => "bool",
      &DataType::UInt8 => "uint8",
      &DataType::Int8 => "int8",
      &DataType::UInt16 => "uint16",
      &DataType::Int16 => "int16",
      &DataType::UInt32 => "uint32",
      &DataType::Int32 => "int32",
      &DataType::UInt64 => "uint64",
      &DataType::Int64 => "int64",
      &DataType::HalfFloat => "halffloat",
      &DataType::Float => "float",
      &DataType::Double => "double",
      &DataType::String => "utf8",
      &DataType::Binary => "binary",
      &DataType::FixedSizedBinary { byte_width } => "fixed_size_binary",
      &DataType::Date32 { ref unit } => "date32",
      &DataType::Date64 { ref unit } => "date64",
      &DataType::Timestamp { ref unit, ref timezone } => "timestamp",
      &DataType::Time32 { ref unit } => "time32",
      &DataType::Time64 { ref unit } => "time64",
      &DataType::Interval { ref unit } => "interval",
      &DataType::Decimal { precision, scale } => "decimal",
      &DataType::List { ref value_type } => "list",
      &DataType::Struct { ref fields } => "struct",
      &DataType::Union { ref fields, ref type_codes, ref mode } => "union",
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => "dictionary",
    }
  }

  pub fn is_integer(&self) -> bool {
    match self {
      &DataType::Int8 => true,
      &DataType::Int16 => true,
      &DataType::Int32 => true,
      &DataType::Int64 => true,
      &DataType::UInt8 => true,
      &DataType::UInt16 => true,
      &DataType::UInt32 => true,
      &DataType::UInt64 => true,
      _ => false
    }
  }

  pub fn is_signed(&self) -> bool {
    match self {
      &DataType::UInt8 => false,
      &DataType::UInt16 => false,
      &DataType::UInt32 => false,
      &DataType::UInt64 => false,
      &DataType::Int8 => true,
      &DataType::Int16 => true,
      &DataType::Int32 => true,
      &DataType::Int64 => true,
      _ => panic!("{:?} is not an integer", self.ty())
    }
  }

  pub fn is_float(&self) -> bool {
    match self {
      &DataType::HalfFloat => true,
      &DataType::Float => true,
      &DataType::Double => true,
      _ => false
    }
  }

  pub fn precision(&self) -> Precision {
    match self {
      &DataType::HalfFloat => Precision::Half,
      &DataType::Float => Precision::Single,
      &DataType::Double => Precision::Double,
      _ => panic!("{:?} is not a floating point type", self.ty())
    }
  }

  pub fn child(&self, i: usize) -> &Field {
    match self {
      &DataType::Struct { ref fields } => &fields[i],
      &DataType::Union { ref fields, ref type_codes, ref mode } => &fields[i],
      _ => panic!("{:?} is not a nested type", self.ty())
    }
  }

  pub fn get_children(&self) -> &Vec<Field> {
    match self {
      &DataType::Struct { ref fields } => &fields,
      &DataType::Union { ref fields, ref type_codes, ref mode } => &fields,
      _ => panic!("{:?} is not a nested type", self.ty())
    }
  }

  pub fn num_children(&self) -> i32 {
    match self {
      &DataType::Struct { ref fields } => fields.len() as i32,
      &DataType::Union { ref fields, ref type_codes, ref mode } => fields.len() as i32,
      _ => panic!("{:?} is not a nested type", self.ty())
    }
  }

  pub fn date_unit(&self) -> &DateUnit {
    match self {
      &DataType::Date32 { ref unit } => unit,
      &DataType::Date64 { ref unit } => unit,
      _ => panic!("{:?} is not a date type", self.ty())
    }
  }

  pub fn time_unit(&self) -> &TimeUnit {
    match self {
      &DataType::Timestamp { ref unit, ref timezone } => unit,
      &DataType::Time32 { ref unit } => unit,
      &DataType::Time64 { ref unit } => unit,
      _ => panic!("{:?} is not a time type", self.ty())
    }
  }

  pub fn interval_unit(&self) -> &IntervalUnit {
    match self {
      &DataType::Interval { ref unit } => unit,
      _ => panic!("{:?} is not an interval type", self.ty())
    }
  }

  pub fn decimal_precision(&self) -> i32 {
    match self {
      &DataType::Decimal { precision, scale } => precision,
      _ => panic!("{:?} is not a decimal type", self.ty())
    }
  }

  pub fn decimal_scale(&self) -> i32 {
    match self {
      &DataType::Decimal { precision, scale } => scale,
      _ => panic!("{:?} is not a decimal type", self.ty())
    }
  }

  pub fn list_value_type(&self) -> &Box<DataType> {
    match self {
      &DataType::List { ref value_type } => &value_type,
      _ => panic!("{:?} is not a list type", self.ty())
    }
  }

  pub fn union_type_codes(&self) -> &Vec<u8> {
    match self {
      &DataType::Union { ref fields, ref type_codes, ref mode } => type_codes,
      _ => panic!("{:?} is not an union type", self.ty())
    }
  }

  pub fn union_mode(&self) -> &UnionMode {
    match self {
      &DataType::Union { ref fields, ref type_codes, ref mode } => mode,
      _ => panic!("{:?} is not an union type", self.ty())
    }
  }

  pub fn dictionary_index_type(&self) -> &Box<DataType> {
    match self {
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => &index_type,
      _ => panic!("{:?} is not a dictionary type", self.ty())
    }
  }

  pub fn get_dictionary(&self) -> &Box<Array> {
    match self {
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => &dictionary,
      _ => panic!("{:?} is not a dictionary type", self.ty())
    }
  }

  pub fn is_dictionary_ordered(&self) -> bool {
    match self {
      &DataType::Dictionary { ref index_type, ref dictionary, ordered } => ordered,
      _ => panic!("{:?} is not a dictionary type", self.ty())
    }
  }
}



//// Required to implement this trait for every data types
//pub trait BaseDataType {
//  fn ty(&self) -> Ty;
//  fn get_buffer_layout(&self) -> Vec<BufferDesc>;
//  fn name(&self) -> &str;
//
//  fn box_clone(&self) -> Box<BaseDataType>;
//
//  fn is_integer(&self) -> bool {
//    false
//  }
//
//  fn is_float(&self) -> bool {
//    false
//  }
//}

//pub trait Cast {
//  fn as_null(&self) -> &NullType {
//    panic!("Cannot cast to null")
//  }
//
//  fn as_bool(&self) -> &BooleanType {
//    panic!("Cannot cast to bool")
//  }
//
//  fn as_uint8(&self) -> &UInt8Type  {
//    panic!("Cannot cast to uint8")
//  }
//
//  fn as_int8(&self) -> &Int8Type  {
//    panic!("Cannot cast to int8")
//  }
//
//  fn as_uint16(&self) -> &UInt16Type  {
//    panic!("Cannot cast to uint16")
//  }
//
//  fn as_int16(&self) -> &Int16Type  {
//    panic!("Cannot cast to int16")
//  }
//
//  fn as_uint32(&self) -> &UInt32Type  {
//    panic!("Cannot cast to uint32")
//  }
//
//  fn as_int32(&self) -> &Int32Type  {
//    panic!("Cannot cast to int32")
//  }
//
//  fn as_uint64(&self) -> &UInt64Type  {
//    panic!("Cannot cast to uint64")
//  }
//
//  fn as_int64(&self) -> &Int64Type  {
//    panic!("Cannot cast to int64")
//  }
//
//  fn as_half_float(&self) -> &HalfFloatType  {
//    panic!("Cannot cast to half_float")
//  }
//
//  fn as_float(&self) -> &FloatType  {
//    panic!("Cannot cast to float")
//  }
//
//  fn as_double(&self) -> &DoubleType  {
//    panic!("Cannot cast to double")
//  }
//
//  fn as_string(&self) -> &StringType  {
//    panic!("Cannot cast to string")
//  }
//
//  fn as_binary(&self) -> &BinaryType  {
//    panic!("Cannot cast to binary")
//  }
//
//  fn as_fixed_sized_binary(&self) -> &FixedSizedBinaryType  {
//    panic!("Cannot cast to fixed_sized_binary")
//  }
//
//  fn as_date64(&self) -> &Date64Type  {
//    panic!("Cannot cast to date64")
//  }
//
//  fn as_date32(&self) -> &Date32Type  {
//    panic!("Cannot cast to date32")
//  }
//
//  fn as_timestamp(&self) -> &TimestampType  {
//    panic!("Cannot cast to timestamp")
//  }
//
//  fn as_time32(&self) -> &Time32Type  {
//    panic!("Cannot cast to time32")
//  }
//
//  fn as_time64(&self) -> &Time64Type  {
//    panic!("Cannot cast to time64")
//  }
//
//  fn as_interval(&self) -> &IntervalType  {
//    panic!("Cannot cast to interval")
//  }
//
//  fn as_decimal(&self) -> &DecimalType  {
//    panic!("Cannot cast to decimal")
//  }
//
//  fn as_list(&self) -> &ListType  {
//    panic!("Cannot cast to list")
//  }
//
//  fn as_struct(&self) -> &StructType  {
//    panic!("Cannot cast to struct")
//  }
//
//  fn as_union(&self) -> &UnionType  {
//    panic!("Cannot cast to union")
//  }
//
//  fn as_dictionary(&self) -> &DictionaryType  {
//    panic!("Cannot cast to dictionary")
//  }
//}
//
//macro_rules! define_cast {
//    ($data_type: ident, $method_name: ident) => {
//      fn $method_name(&self) -> &$data_type {
//        &self
//      }
//    };
//}

//pub trait DataType: BaseDataType + Cast {}
//
//// Required to implement this trait for structured data types
//pub trait NestedType : DataType {
//  fn child(&self, i: usize) -> &Box<Field>;
//  fn get_children(&self) -> &Vec<Box<Field>>;
//  fn num_children(&self) -> i32;
//}
//
//// Required to implement this trait for fixed-size data types
//pub trait FixedWidthType : DataType {
//  fn bit_width(&self) -> i32;
//}
//
//pub trait Number : FixedWidthType {}
//
//pub trait Integer : Number {
//  fn is_signed(&self) -> bool;
//}
//
//fn eq_integer(i1: &Integer, i2: &Integer) -> bool {
//  unimplemented!()
//}
//
//pub trait FloatingPoint : Number {
//  fn precision(&self) -> Precision;
//}
//
//impl Clone for Box<BaseDataType> {
//  fn clone(&self) -> Self {
//    self.box_clone()
//  }
//}
//
//impl Clone for Box<Integer> {
//  fn clone(&self) -> Self {
//    unimplemented!()
//  }
//}
//
//impl PartialEq for Box<Integer> {
//  fn eq(&self, other: &Self) -> bool {
//    unimplemented!()
//  }
//}
//
//impl Eq for Box<Integer> {
//
//}
//
//impl Debug for Box<Integer> {
//  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//    unimplemented!()
//  }
//}
//
//pub struct TypeVisitor {}
//
//impl TypeVisitor {
//  pub fn visit_null(&self, ty: &NullType) {
//    unimplemented!()
//  }
//
//  pub fn visit_bool(&self, ty: &BooleanType) {
//    unimplemented!()
//  }
//
//  pub fn visit_uint8(&self, ty: &UInt8Type) {
//    unimplemented!()
//  }
//}
//
//pub trait Visit: Sized {
//  fn accept(&self, visitor: &TypeVisitor) -> Result<&Self, ArrowError>;
//}
//
//macro_rules! impl_default_traits {
//  ($data_type: ident, $method_name: ident) => {
//    impl ToString for $data_type {
//      fn to_string(&self) -> String {
//        String::from(self.name())
//      }
//    }
//
//    impl Cast for $data_type {
//      fn $method_name(&self) -> &$data_type {
//        &self
//      }
//    }
//
//    impl DataType for $data_type {}
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct NullType {}
//
//impl NullType {
//  pub fn new() -> NullType {
//    NullType {}
//  }
//}
//
//impl BaseDataType for NullType {
//  fn ty(&self) -> Ty {
//    Ty::NA
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    Vec::new()
//  }
//
//  fn name(&self) -> &str {
//    "null"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl Visit for NullType {
//  fn accept(&self, visitor: &TypeVisitor) -> Result<&NullType, ArrowError> {
//    visitor.visit_null(&self);
//    Ok(&self)
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct BooleanType {}
//
//impl BooleanType {
//  pub fn new() -> BooleanType {
//    BooleanType {}
//  }
//}
//
//impl BaseDataType for BooleanType {
//  fn ty(&self) -> Ty {
//    Ty::Bool
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "bool"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for BooleanType {
//  fn bit_width(&self) -> i32 {
//    1
//  }
//}
//
//macro_rules! define_integer {
//  ($type_name: ident, $ty: path, $name: expr, $bit_width: expr, $signed: expr) => {
//
//    #[derive(Debug, Eq, PartialEq, Clone)]
//    pub struct $type_name {}
//
//    impl $type_name {
//      pub fn new() -> $type_name {
//        $type_name {}
//      }
//    }
//
//    impl BaseDataType for $type_name {
//      fn ty(&self) -> Ty {
//        $ty
//      }
//
//      fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//        vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//      }
//
//      fn name(&self) -> &str {
//        $name
//      }
//
//      fn box_clone(&self) -> Box<BaseDataType> {
//        Box::from(self.clone())
//      }
//
//      fn is_integer(&self) -> bool {
//        true
//      }
//   }
//
//    impl FixedWidthType for $type_name {
//      fn bit_width(&self) -> i32 {
//        $bit_width
//      }
//    }
//
//    impl Number for $type_name {}
//
//    impl Integer for $type_name {
//      fn is_signed(&self) -> bool {
//        $signed
//      }
//    }
//  }
//}
//
//macro_rules! define_float {
//  ($type_name: ident, $ty: path, $name: expr, $bit_width: expr, $precision: path) => {
//
//    #[derive(Debug, Eq, PartialEq, Clone)]
//    pub struct $type_name {}
//
//    impl $type_name {
//      pub fn new() -> $type_name {
//        $type_name {}
//      }
//    }
//
//    impl BaseDataType for $type_name {
//      fn ty(&self) -> Ty {
//        $ty
//      }
//
//      fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//        vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//      }
//
//      fn name(&self) -> &str {
//        $name
//      }
//
//      fn box_clone(&self) -> Box<BaseDataType> {
//        Box::from(self.clone())
//      }
//
//      fn is_float(&self) -> bool {
//        true
//      }
//   }
//
//    impl FixedWidthType for $type_name {
//      fn bit_width(&self) -> i32 {
//        $bit_width
//      }
//    }
//
//    impl Number for $type_name {}
//
//    impl FloatingPoint for $type_name {
//      fn precision(&self) -> Precision {
//        $precision
//      }
//    }
//  }
//}
//
//define_integer!(UInt8Type, Ty::UInt8, "uint8", 8, false);
//define_integer!(UInt16Type, Ty::UInt16, "uint16", 16, false);
//define_integer!(UInt32Type, Ty::UInt32, "uint32", 32, false);
//define_integer!(UInt64Type, Ty::UInt64, "uint64", 64, false);
//define_integer!(Int8Type, Ty::Int8, "int8", 8, true);
//define_integer!(Int16Type, Ty::Int16, "int16", 16, true);
//define_integer!(Int32Type, Ty::Int32, "int32", 32, true);
//define_integer!(Int64Type, Ty::Int64, "int64", 64, true);
//
//define_float!(HalfFloatType, Ty::HalfFloat, "halffloat", 16, Precision::Half);
//define_float!(FloatType, Ty::Float, "float", 32, Precision::Single);
//define_float!(DoubleType, Ty::Double, "double", 64, Precision::Double);
//
//#[derive(Debug, Clone)]
//pub struct ListType {
//  value_field: Box<Field>
//}
//
//impl ListType {
//  pub fn new(value_field: Box<Field>) -> ListType {
//    ListType {
//      value_field
//    }
//  }
//
//  pub fn value_field(&self) -> &Box<Field> {
//    &self.value_field
//  }
//
//  pub fn value_type(&self) -> &DataType {
//    self.value_field.get_type()
//  }
//}
//
//impl BaseDataType for ListType {
//  fn ty(&self) -> Ty {
//    Ty::List
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer()]
//  }
//
//  fn name(&self) -> &str {
//    "list"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    unimplemented!()
//  }
//}
//
//impl PartialEq for ListType {
//  fn eq(&self, other: &ListType) -> bool {
//    unimplemented!()
//  }
//}
//
//impl Eq for ListType {
//
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct BinaryType {}
//
//impl BinaryType {
//  pub fn new() -> BinaryType {
//    BinaryType {}
//  }
//}
//
//impl BaseDataType for BinaryType {
//  fn ty(&self) -> Ty {
//    Ty::Binary
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)]
//  }
//
//  fn name(&self) -> &str {
//    "binary"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct FixedSizedBinaryType {
//  byte_width: i32
//}
//
//impl FixedSizedBinaryType {
//  pub fn new(byte_width: i32) -> FixedSizedBinaryType {
//    FixedSizedBinaryType {
//      byte_width
//    }
//  }
//
//  pub fn byte_width(&self) -> i32 {
//    self.byte_width
//  }
//}
//
//impl BaseDataType for FixedSizedBinaryType {
//  fn ty(&self) -> Ty {
//    Ty::FixedSizedBinary
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "fixed_size_binary"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for FixedSizedBinaryType {
//  fn bit_width(&self) -> i32 {
//    self.byte_width * 8
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct StringType {}
//
//impl StringType {
//  pub fn new() -> StringType {
//    StringType {}
//  }
//}
//
//impl BaseDataType for StringType {
//  fn ty(&self) -> Ty {
//    Ty::String
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)]
//  }
//
//  fn name(&self) -> &str {
//    "utf8"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct StructType {
//  fields: Vec<Box<Field>>
//}
//
//impl StructType {
//  pub fn new(fields: Vec<Box<Field>>) -> StructType {
//    StructType {
//      fields
//    }
//  }
//}
//
//impl BaseDataType for StructType {
//  fn ty(&self) -> Ty {
//    Ty::Struct
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer()]
//  }
//
//  fn name(&self) -> &str {
//    "struct"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl NestedType for StructType {
//  fn child(&self, i: usize) -> &Box<Field> {
//    &self.fields[i]
//  }
//
//  fn get_children(&self) -> &Vec<Box<Field>> {
//    &self.fields
//  }
//
//  fn num_children(&self) -> i32 {
//    self.fields.len() as i32
//  }
//}
//
//impl Index<usize> for StructType {
//  type Output = Box<Field>;
//
//  #[inline]
//  fn index(&self, index: usize) -> &Box<Field> {
//    &self.fields[index]
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct DecimalType {
//  precision: i32,
//  scale: i32
//}
//
//impl DecimalType {
//  pub fn new(precision: i32, scale: i32) -> DecimalType {
//    DecimalType {
//      precision,
//      scale
//    }
//  }
//
//  pub fn precision(&self) -> i32 {
//    self.precision
//  }
//
//  pub fn scale(&self) -> i32 {
//    self.scale
//  }
//}
//
//impl BaseDataType for DecimalType {
//  fn ty(&self) -> Ty {
//    Ty::Decimal
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "decimal"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for DecimalType {
//  fn bit_width(&self) -> i32 {
//    16 * 8
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct UnionType {
//  fields: Vec<Box<Field>>,
//  type_codes: Vec<u8>,
//  mode: UnionMode
//}
//
//impl UnionType {
//  pub fn new(fields: Vec<Box<Field>>, type_codes: Vec<u8>) -> UnionType {
//    UnionType {
//      fields,
//      type_codes,
//      mode: UnionMode::SPARSE,
//    }
//  }
//
//  pub fn with_mode(fields: Vec<Box<Field>>, type_codes: Vec<u8>, mode: UnionMode) -> UnionType {
//    UnionType {
//      fields,
//      type_codes,
//      mode
//    }
//  }
//
//  pub fn type_codes(&self) -> &Vec<u8> {
//    &self.type_codes
//  }
//
//  pub fn mode(&self) -> &UnionMode {
//    &self.mode
//  }
//}
//
//impl BaseDataType for UnionType {
//  fn ty(&self) -> Ty {
//    Ty::Union
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    match self.mode {
//      UnionMode::SPARSE => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer()],
//      _ => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer(), BufferDesc::k_offset_buffer()]
//    }
//  }
//
//  fn name(&self) -> &str {
//    "union"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl NestedType for UnionType {
//  fn child(&self, i: usize) -> &Box<Field> {
//    &self.fields[i]
//  }
//
//  fn get_children(&self) -> &Vec<Box<Field>> {
//    &self.fields
//  }
//
//  fn num_children(&self) -> i32 {
//    self.fields.len() as i32
//  }
//}
//
//impl Index<usize> for UnionType {
//  type Output = Box<Field>;
//
//  #[inline]
//  fn index(&self, index: usize) -> &Box<Field> {
//    &self.fields[index]
//  }
//}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DateUnit {
  Day,
  Milli
}

//pub trait DateType : FixedWidthType {
//  fn unit(&self) -> &DateUnit;
//}
//
//macro_rules! define_date_type {
//    ($type_name: ident, $ty: path, $name: expr, $bit_width: expr) => {
//        #[derive(Debug, Eq, PartialEq, Clone)]
//        pub struct $type_name {
//          unit: DateUnit
//        }
//
//        impl $type_name {
//          pub fn new(unit: DateUnit) -> $type_name {
//            $type_name {
//              unit
//            }
//          }
//        }
//
//        impl BaseDataType for $type_name {
//          fn ty(&self) -> Ty {
//            $ty
//          }
//
//          fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//            vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//          }
//
//          fn name(&self) -> &str {
//            $name
//          }
//
//          fn box_clone(&self) -> Box<BaseDataType> {
//            Box::from(self.clone())
//          }
//       }
//
//        impl FixedWidthType for $type_name {
//          fn bit_width(&self) -> i32 {
//            $bit_width
//          }
//        }
//
//        impl DateType for $type_name {
//          fn unit(&self) -> &DateUnit {
//            &self.unit
//          }
//        }
//    }
//}
//
//define_date_type!(Date32Type, Ty::Date32, "date32", 32);
//define_date_type!(Date64Type, Ty::Date64, "date64", 64);
//
//
//pub trait TimeType : FixedWidthType {
//  fn unit(&self) -> &TimeUnit;
//}
//
//macro_rules! define_time_type {
//    ($type_name: ident, $ty: path, $name: expr, $bit_width: expr) => {
//        #[derive(Debug, Eq, PartialEq, Clone)]
//        pub struct $type_name {
//          unit: TimeUnit
//        }
//
//        impl $type_name {
//          #[inline]
//          pub fn default_unit() -> TimeUnit {
//            TimeUnit::Milli
//          }
//
//          pub fn new() -> $type_name {
//            $type_name {
//              unit: TimeUnit::Milli
//            }
//          }
//
//          pub fn with_unit(unit: TimeUnit) -> $type_name {
//            $type_name {
//              unit
//            }
//          }
//        }
//
//        impl BaseDataType for $type_name {
//          fn ty(&self) -> Ty {
//            $ty
//          }
//
//          fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//            vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//          }
//
//          fn name(&self) -> &str {
//            $name
//          }
//
//          fn box_clone(&self) -> Box<BaseDataType> {
//            Box::from(self.clone())
//          }
//       }
//
//        impl FixedWidthType for $type_name {
//          fn bit_width(&self) -> i32 {
//            $bit_width
//          }
//        }
//
//        impl TimeType for $type_name {
//          fn unit(&self) -> &TimeUnit {
//            &self.unit
//          }
//        }
//    }
//}
//
//define_time_type!(Time32Type, Ty::Time32, "time32", 32);
//define_time_type!(Time64Type, Ty::Time64, "time64", 64);
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct TimestampType {
//  unit: TimeUnit,
//  timezone: String
//}
//
//impl TimestampType {
//  pub fn default_unit() -> TimeUnit {
//    TimeUnit::Milli
//  }
//
//  pub fn default_timezone() -> String {
//    String::new()
//  }
//
//  pub fn new() -> TimestampType {
//    TimestampType {
//      unit: TimeUnit::Milli,
//      timezone: String::new()
//    }
//  }
//
//  pub fn with_unit(unit: TimeUnit) -> TimestampType {
//    TimestampType {
//      unit,
//      timezone: String::new()
//    }
//  }
//
//  pub fn with_unit_and_timezone(unit: TimeUnit, timezone: String) -> TimestampType {
//    TimestampType {
//      unit,
//      timezone
//    }
//  }
//
//  pub fn unit(&self) -> &TimeUnit {
//    &self.unit
//  }
//
//  pub fn timezone(&self) -> &String {
//    &self.timezone
//  }
//}
//
//impl BaseDataType for TimestampType {
//  fn ty(&self) -> Ty {
//    Ty::Timestamp
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "timestamp"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for TimestampType {
//  fn bit_width(&self) -> i32 {
//    64
//  }
//}
//
//#[derive(Debug, Eq, PartialEq, Clone)]
//pub struct IntervalType {
//  unit: IntervalUnit
//}
//
//impl IntervalType {
//  pub fn new() -> IntervalType {
//    IntervalType {
//      unit: IntervalUnit::YearMonth
//    }
//  }
//
//  pub fn with_unit(unit: IntervalUnit) -> IntervalType {
//    IntervalType {
//      unit
//    }
//  }
//}
//
//impl BaseDataType for IntervalType {
//  fn ty(&self) -> Ty {
//    Ty::Interval
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "interval"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for IntervalType {
//  fn bit_width(&self) -> i32 {
//    64
//  }
//}
//
//#[derive(Debug, Clone)]
//pub struct DictionaryType {
//  index_type: Box<Integer>,
//  dictionary: Box<Array>,
//  ordered: bool
//}
//
//impl DictionaryType {
//  pub fn unordered(index_type: Box<Integer>, dictionary: Box<Array>) -> DictionaryType {
//    DictionaryType {
//      index_type,
//      dictionary,
//      ordered: false
//    }
//  }
//
//  pub fn ordered(index_type: Box<Integer>, dictionary: Box<Array>) -> DictionaryType {
//    DictionaryType {
//      index_type,
//      dictionary,
//      ordered: true
//    }
//  }
//
//  pub fn new(index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool) -> DictionaryType {
//    DictionaryType {
//      index_type,
//      dictionary,
//      ordered
//    }
//  }
//
//  pub fn index_type(&self) -> &Box<Integer> {
//    &self.index_type
//  }
//
//  pub fn dictionary(&self) -> &Box<Array> {
//    &self.dictionary
//  }
//
//  pub fn is_ordered(&self) -> bool {
//    self.ordered
//  }
//}
//
//impl BaseDataType for DictionaryType {
//  fn ty(&self) -> Ty {
//    Ty::Dictionary
//  }
//
//  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
//    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
//  }
//
//  fn name(&self) -> &str {
//    "dictionary"
//  }
//
//  fn box_clone(&self) -> Box<BaseDataType> {
//    Box::from(self.clone())
//  }
//}
//
//impl FixedWidthType for DictionaryType {
//  fn bit_width(&self) -> i32 {
//    self.index_type.bit_width()
//  }
//}
//
//impl PartialEq for DictionaryType {
//  fn eq(&self, other: &Self) -> bool {
//    eq_integer(self.index_type.as_ref(), other.index_type.as_ref()) &&
//      array::array_eq(self.dictionary.as_ref(), other.dictionary.as_ref()) &&
//      self.ordered == other.ordered
//  }
//}
//
//impl Eq for DictionaryType {}
//
//
//impl Visit for BooleanType {
//  fn accept(&self, visitor: &TypeVisitor) -> Result<&BooleanType, ArrowError> {
//    visitor.visit_bool(&self);
//    Ok(&self)
//  }
//}
//
//impl Visit for UInt8Type {
//  fn accept(&self, visitor: &TypeVisitor) -> Result<&Self, ArrowError> {
//    visitor.visit_uint8(&self);
//    Ok(&self)
//  }
//}
//
//// TODO: impl Visit
//
//impl_default_traits!(NullType, as_null);
//impl_default_traits!(BooleanType, as_bool);
//impl_default_traits!(UInt8Type, as_uint8);
//impl_default_traits!(UInt16Type, as_uint16);
//impl_default_traits!(UInt32Type, as_uint32);
//impl_default_traits!(UInt64Type, as_uint64);
//impl_default_traits!(Int8Type, as_int8);
//impl_default_traits!(Int16Type, as_int16);
//impl_default_traits!(Int32Type, as_int32);
//impl_default_traits!(Int64Type, as_int64);
//impl_default_traits!(HalfFloatType, as_half_float);
//impl_default_traits!(FloatType, as_float);
//impl_default_traits!(DoubleType, as_double);
//impl_default_traits!(BinaryType, as_binary);
//impl_default_traits!(FixedSizedBinaryType, as_fixed_sized_binary);
//impl_default_traits!(StringType, as_string);
//impl_default_traits!(DecimalType, as_decimal);
//impl_default_traits!(Date32Type, as_date32);
//impl_default_traits!(Date64Type, as_date64);
//impl_default_traits!(Time32Type, as_time32);
//impl_default_traits!(Time64Type, as_time64);
//impl_default_traits!(TimestampType, as_timestamp);
//impl_default_traits!(IntervalType, as_interval);
//impl_default_traits!(ListType, as_list);
//impl_default_traits!(UnionType, as_union);
//impl_default_traits!(StructType, as_struct);
//impl_default_traits!(DictionaryType, as_dictionary);

#[derive(Debug, Eq, PartialEq)]
pub enum Precision {
  Half,
  Single,
  Double
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum IntervalUnit {
  YearMonth,
  DayTime
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum UnionMode {
  SPARSE,
  DENSE
}
