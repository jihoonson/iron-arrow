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
pub enum Ty<'a> {
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
  FixedSizeBinary {
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
    value_type: Box<Ty<'a>>
  },

  // Struct of logical types
  Struct {
    fields: Vec<Field<'a>>
  },

  // Unions of logical types
  Union {
    fields: Vec<Field<'a>>,
    type_codes: Vec<u8>,
    mode: UnionMode
  },

  // Dictionary aka Category type
  Dictionary {
    index_type: Box<Ty<'a>>,
    dictionary: Box<Array<'a>>,
    ordered: bool
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

  pub fn validity_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Validity,
      bit_width: 1
    }
  }

  pub fn offset_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Offset,
      bit_width: 32
    }
  }

  pub fn type_buffer() -> BufferDesc {
    BufferDesc {
      ty: BufferType::Type,
      bit_width: 32
    }
  }

  pub fn data_buffer(bit_width: i32) -> BufferDesc {
    BufferDesc {
      ty: BufferType::Data,
      bit_width
    }
  }
}

impl <'a> Ty<'a> {
  pub fn null() -> Ty<'a> {
    Ty::NA
  }

  pub fn bool() -> Ty<'a> {
    Ty::Bool
  }

  pub fn uint8() -> Ty<'a> {
    Ty::UInt8
  }

  pub fn int8() -> Ty<'a> {
    Ty::Int8
  }

  pub fn uint16() -> Ty<'a> {
    Ty::UInt16
  }

  pub fn int16() -> Ty<'a> {
    Ty::Int16
  }

  pub fn uint32() -> Ty<'a> {
    Ty::UInt32
  }

  pub fn int32() -> Ty<'a> {
    Ty::Int32
  }

  pub fn uint64() -> Ty<'a> {
    Ty::UInt64
  }

  pub fn int64() -> Ty<'a> {
    Ty::Int64
  }

  pub fn halffloat() -> Ty<'a> {
    Ty::HalfFloat
  }

  pub fn float() -> Ty<'a> {
    Ty::Float
  }

  pub fn double() -> Ty<'a> {
    Ty::Double
  }

  pub fn string() -> Ty<'a> {
    Ty::String
  }

  pub fn binary() -> Ty<'a> {
    Ty::Binary
  }

  pub fn fixed_sized_binary(byte_width: i32) -> Ty<'a> {
    Ty::FixedSizeBinary {
      byte_width
    }
  }

  pub fn date64() -> Ty<'a> {
    Ty::Date64 {
      unit: DateUnit::Milli
    }
  }

  pub fn date64_with_unit(unit: DateUnit) -> Ty<'a> {
    Ty::Date64 {
      unit
    }
  }

  pub fn date32() -> Ty<'a> {
    Ty::Date32 {
      unit: DateUnit::Milli
    }
  }

  pub fn date32_with_unit(unit: DateUnit) -> Ty<'a> {
    Ty::Date32 {
      unit
    }
  }

  pub fn timestamp() -> Ty<'a> {
    Ty::Timestamp {
      unit: TimeUnit::Milli,
      timezone: String::new(),
    }
  }

  pub fn timestamp_with_unit(unit: TimeUnit) -> Ty<'a> {
    Ty::Timestamp {
      unit,
      timezone: String::new()
    }
  }

  pub fn timestamp_with_timezone(timezone: String) -> Ty<'a> {
    Ty::Timestamp {
      unit: TimeUnit::Milli,
      timezone
    }
  }

  pub fn timestamp_with_unit_and_timestamp(unit: TimeUnit, timezone: String) -> Ty<'a> {
    Ty::Timestamp {
      unit,
      timezone
    }
  }

  pub fn time32() -> Ty<'a> {
    Ty::Time32 {
      unit: TimeUnit::Milli
    }
  }

  pub fn time32_with_unit(unit: TimeUnit) -> Ty<'a> {
    Ty::Time32 {
      unit
    }
  }

  pub fn time64() -> Ty<'a> {
    Ty::Time64 {
      unit: TimeUnit::Milli
    }
  }

  pub fn time64_with_unit(unit: TimeUnit) -> Ty<'a> {
    Ty::Time64 {
      unit
    }
  }

  pub fn interval() -> Ty<'a> {
    Ty::Interval {
      unit: IntervalUnit::YearMonth
    }
  }

  pub fn interval_with_unit(unit: IntervalUnit) -> Ty<'a> {
    Ty::Interval {
      unit
    }
  }

  pub fn decimal(precision: i32, scale: i32) -> Ty<'a> {
    Ty::Decimal {
      precision,
      scale
    }
  }

  pub fn list(value_type: Box<Ty<'a>>) -> Ty<'a> {
    Ty::List {
      value_type
    }
  }

  pub fn struct_type(fields: Vec<Field<'a>>) -> Ty<'a> {
    Ty::Struct {
      fields
    }
  }

  pub fn union(fields: Vec<Field<'a>>, type_codes: Vec<u8>) -> Ty<'a> {
    Ty::Union {
      fields,
      type_codes,
      mode: UnionMode::SPARSE
    }
  }

  pub fn union_with_mode(fields: Vec<Field<'a>>, type_codes: Vec<u8>, mode: UnionMode) -> Ty<'a> {
    Ty::Union {
      fields,
      type_codes,
      mode
    }
  }

  pub fn dictionary(index_type: Box<Ty<'a>>, dictionary: Box<Array<'a>>) -> Ty<'a> {
    if !index_type.is_integer() {
      panic!("index type [{:?}] is not an integer", index_type)
    }

    Ty::Dictionary {
      index_type,
      dictionary,
      ordered: false
    }
  }

  pub fn ordered_dictionary(index_type: Box<Ty<'a>>, dictionary: Box<Array<'a>>) -> Ty<'a> {
    if !index_type.is_integer() {
      panic!("index type [{:?}] is not an integer", index_type)
    }

    Ty::Dictionary {
      index_type,
      dictionary,
      ordered: true
    }
  }

  pub fn bit_width(&self) -> i32 {
    match self {
      &Ty::Bool => 1,

      &Ty::UInt8 => 8,
      &Ty::Int8 => 8,
      &Ty::UInt16 => 16,
      &Ty::Int16 => 16,
      &Ty::UInt32 => 32,
      &Ty::Int32 => 32,
      &Ty::UInt64 => 64,
      &Ty::Int64 => 64,

      &Ty::HalfFloat => 16,
      &Ty::Float => 32,
      &Ty::Double => 64,

      &Ty::FixedSizeBinary { byte_width } => byte_width * 8,

      &Ty::Date32 { ref unit } => 32,
      &Ty::Date64 { ref unit } => 64,

      &Ty::Timestamp { ref unit, ref timezone } => 64,
      &Ty::Time32 { ref unit } => 32,
      &Ty::Time64 { ref unit } => 64,
      &Ty::Interval { ref unit } => 64,

      &Ty::Decimal { precision, scale } => 16 * 8,

      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => index_type.bit_width(),

      _ => panic!("{:?} is not fixed width type", self)
    }
  }

  pub fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    match self {
      &Ty::NA => Vec::new(),
      &Ty::Bool => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 1)],

      &Ty::UInt8 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &Ty::Int8 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &Ty::UInt16 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &Ty::Int16 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &Ty::UInt32 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &Ty::Int32 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &Ty::UInt64 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &Ty::Int64 => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &Ty::HalfFloat => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 16)],
      &Ty::Float => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &Ty::Double => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &Ty::String => vec![BufferDesc::validity_buffer(), BufferDesc::offset_buffer(), BufferDesc::new(BufferType::Data, 8)],
      &Ty::Binary => vec![BufferDesc::validity_buffer(), BufferDesc::offset_buffer(), BufferDesc::new(BufferType::Data, 8)],

      &Ty::FixedSizeBinary { byte_width } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, byte_width * 8)],

      &Ty::Date32 { ref unit } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &Ty::Date64 { ref unit } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &Ty::Timestamp { ref unit, ref timezone } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &Ty::Time32 { ref unit } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 32)],
      &Ty::Time64 { ref unit } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],
      &Ty::Interval { ref unit } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 64)],

      &Ty::Decimal { precision, scale } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, 16 * 8)],

      &Ty::List { ref value_type } => vec![BufferDesc::validity_buffer(), BufferDesc::offset_buffer()],
      &Ty::Struct { ref fields } => vec![BufferDesc::validity_buffer()],
      &Ty::Union { ref fields, ref type_codes, ref mode } => {
        match mode {
          &UnionMode::SPARSE => vec![BufferDesc::validity_buffer(), BufferDesc::type_buffer()],
          _ => vec![BufferDesc::validity_buffer(), BufferDesc::type_buffer(), BufferDesc::offset_buffer()]
        }
      },
      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => vec![BufferDesc::validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
    }
  }

  pub fn name(&self) -> &str {
    match self {
      &Ty::NA => "null",
      &Ty::Bool => "bool",
      &Ty::UInt8 => "uint8",
      &Ty::Int8 => "int8",
      &Ty::UInt16 => "uint16",
      &Ty::Int16 => "int16",
      &Ty::UInt32 => "uint32",
      &Ty::Int32 => "int32",
      &Ty::UInt64 => "uint64",
      &Ty::Int64 => "int64",
      &Ty::HalfFloat => "halffloat",
      &Ty::Float => "float",
      &Ty::Double => "double",
      &Ty::String => "utf8",
      &Ty::Binary => "binary",
      &Ty::FixedSizeBinary { byte_width } => "fixed_size_binary",
      &Ty::Date32 { ref unit } => "date32",
      &Ty::Date64 { ref unit } => "date64",
      &Ty::Timestamp { ref unit, ref timezone } => "timestamp",
      &Ty::Time32 { ref unit } => "time32",
      &Ty::Time64 { ref unit } => "time64",
      &Ty::Interval { ref unit } => "interval",
      &Ty::Decimal { precision, scale } => "decimal",
      &Ty::List { ref value_type } => "list",
      &Ty::Struct { ref fields } => "struct",
      &Ty::Union { ref fields, ref type_codes, ref mode } => "union",
      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => "dictionary",
    }
  }

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

  pub fn precision(&self) -> Precision {
    match self {
      &Ty::HalfFloat => Precision::Half,
      &Ty::Float => Precision::Single,
      &Ty::Double => Precision::Double,
      _ => panic!("{:?} is not a floating point type", self)
    }
  }

  pub fn child(&self, i: usize) -> &Field {
    match self {
      &Ty::Struct { ref fields } => &fields[i],
      &Ty::Union { ref fields, ref type_codes, ref mode } => &fields[i],
      _ => panic!("{:?} is not a nested type", self)
    }
  }

  pub fn get_children(&self) -> &Vec<Field> {
    match self {
      &Ty::Struct { ref fields } => &fields,
      &Ty::Union { ref fields, ref type_codes, ref mode } => &fields,
      _ => panic!("{:?} is not a nested type", self)
    }
  }

  pub fn num_children(&self) -> i32 {
    match self {
      &Ty::Struct { ref fields } => fields.len() as i32,
      &Ty::Union { ref fields, ref type_codes, ref mode } => fields.len() as i32,
      _ => panic!("{:?} is not a nested type", self)
    }
  }

  pub fn date_unit(&self) -> &DateUnit {
    match self {
      &Ty::Date32 { ref unit } => unit,
      &Ty::Date64 { ref unit } => unit,
      _ => panic!("{:?} is not a date type", self)
    }
  }

  pub fn time_unit(&self) -> &TimeUnit {
    match self {
      &Ty::Timestamp { ref unit, ref timezone } => unit,
      &Ty::Time32 { ref unit } => unit,
      &Ty::Time64 { ref unit } => unit,
      _ => panic!("{:?} is not a time type", self)
    }
  }

  pub fn interval_unit(&self) -> &IntervalUnit {
    match self {
      &Ty::Interval { ref unit } => unit,
      _ => panic!("{:?} is not an interval type", self)
    }
  }

  pub fn decimal_precision(&self) -> i32 {
    match self {
      &Ty::Decimal { precision, scale } => precision,
      _ => panic!("{:?} is not a decimal type", self)
    }
  }

  pub fn decimal_scale(&self) -> i32 {
    match self {
      &Ty::Decimal { precision, scale } => scale,
      _ => panic!("{:?} is not a decimal type", self)
    }
  }

  pub fn list_value_type(&self) -> &Box<Ty<'a>> {
    match self {
      &Ty::List { ref value_type } => &value_type,
      _ => panic!("{:?} is not a list type", self)
    }
  }

  pub fn union_type_codes(&self) -> &Vec<u8> {
    match self {
      &Ty::Union { ref fields, ref type_codes, ref mode } => type_codes,
      _ => panic!("{:?} is not an union type", self)
    }
  }

  pub fn union_mode(&self) -> &UnionMode {
    match self {
      &Ty::Union { ref fields, ref type_codes, ref mode } => mode,
      _ => panic!("{:?} is not an union type", self)
    }
  }

  pub fn dictionary_index_type(&self) -> &Box<Ty<'a>> {
    match self {
      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => &index_type,
      _ => panic!("{:?} is not a dictionary type", self)
    }
  }

  pub fn get_dictionary(&self) -> &Box<Array<'a>> {
    match self {
      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => &dictionary,
      _ => panic!("{:?} is not a dictionary type", self)
    }
  }

  pub fn is_dictionary_ordered(&self) -> bool {
    match self {
      &Ty::Dictionary { ref index_type, ref dictionary, ordered } => ordered,
      _ => panic!("{:?} is not a dictionary type", self)
    }
  }
}

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

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DateUnit {
  Day,
  Milli
}

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
