use std::ops::Index;

use common::KeyValueMetadata;
use common::status::ArrowError;
use array::Array;

#[macro_use]
use std;

/// Data types in this library are all *logical*. They can be expressed as
/// either a primitive physical type (bytes or bits of some fixed size), a
/// nested type consisting of other data types, or another data type (e.g. a
/// timestamp encoded as an int64)
#[derive(Debug, Eq, PartialEq)]
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

// Required to implement this trait for every data types
pub trait DataType : Eq + PartialEq + Clone + ToString {
  fn ty(&self) -> Ty;
  fn get_buffer_layout(&self) -> Vec<BufferDesc>;
  fn name(&self) -> &str;
}

// Required to implement this trait for structured data types
pub trait NestedType<T: DataType> : DataType {
  fn child(&self, i: usize) -> &Field<T>;
  fn get_children(&self) -> &Vec<Field<T>>;
  fn num_children(&self) -> i32;
}

// Required to implement this trait for fixed-size data types
pub trait FixedWidthType : DataType {
  fn bit_width(&self) -> i32;
}

pub trait Number : FixedWidthType {}

pub trait Integer : Number {
  fn is_signed(&self) -> bool;
}

pub trait FloatingPoint : Number {
  fn precision(&self) -> Precision;
}

pub struct TypeVisitor {}

impl TypeVisitor {
  pub fn visit_null(&self, ty: &NullType) {
    unimplemented!()
  }

  pub fn visit_bool(&self, ty: &BooleanType) {
    unimplemented!()
  }

  pub fn visit_uint8(&self, ty: &UInt8Type) {
    unimplemented!()
  }
}

pub trait Visit: Sized {
  fn accept(&self, visitor: &TypeVisitor) -> Result<&Self, ArrowError>;
}

macro_rules! impl_default_traits {
  ($data_type: ident) => {
    impl ToString for $data_type {
      fn to_string(&self) -> String {
        String::from(self.name())
      }
    }
  }
}

macro_rules! impl_default_traits_for_generics {
  ($data_type: ident) => {
    impl <T: DataType> ToString for $data_type<T> {
      fn to_string(&self) -> String {
        String::from(self.name())
      }
    }
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NullType {}

impl NullType {
  pub fn new() -> NullType {
    NullType {}
  }
}

impl DataType for NullType {
  fn ty(&self) -> Ty {
    Ty::NA
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    Vec::new()
  }

  fn name(&self) -> &str {
    "null"
  }
}

impl Visit for NullType {
  fn accept(&self, visitor: &TypeVisitor) -> Result<&NullType, ArrowError> {
    visitor.visit_null(&self);
    Ok(&self)
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BooleanType {}

impl BooleanType {
  pub fn new() -> BooleanType {
    BooleanType {}
  }
}

impl DataType for BooleanType {
  fn ty(&self) -> Ty {
    Ty::Bool
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "bool"
  }
}

impl FixedWidthType for BooleanType {
  fn bit_width(&self) -> i32 {
    1
  }
}

macro_rules! define_integer {
  ($type_name: ident, $ty: path, $name: expr, $bit_width: expr, $signed: expr) => {

    #[derive(Debug, Eq, PartialEq, Clone)]
    pub struct $type_name {}

    impl $type_name {
      pub fn new() -> $type_name {
        $type_name {}
      }
    }

    impl DataType for $type_name {
      fn ty(&self) -> Ty {
        $ty
      }

      fn get_buffer_layout(&self) -> Vec<BufferDesc> {
        vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
      }

      fn name(&self) -> &str {
        $name
      }
    }

    impl FixedWidthType for $type_name {
      fn bit_width(&self) -> i32 {
        $bit_width
      }
    }

    impl Number for $type_name {}

    impl Integer for $type_name {
      fn is_signed(&self) -> bool {
        $signed
      }
    }
  }
}

macro_rules! define_float {
  ($type_name: ident, $ty: path, $name: expr, $bit_width: expr, $precision: path) => {

    #[derive(Debug, Eq, PartialEq, Clone)]
    pub struct $type_name {}

    impl $type_name {
      pub fn new() -> $type_name {
        $type_name {}
      }
    }

    impl DataType for $type_name {
      fn ty(&self) -> Ty {
        $ty
      }

      fn get_buffer_layout(&self) -> Vec<BufferDesc> {
        vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
      }

      fn name(&self) -> &str {
        $name
      }
    }

    impl FixedWidthType for $type_name {
      fn bit_width(&self) -> i32 {
        $bit_width
      }
    }

    impl Number for $type_name {}

    impl FloatingPoint for $type_name {
      fn precision(&self) -> Precision {
        $precision
      }
    }
  }
}

define_integer!(UInt8Type, Ty::UInt8, "uint8", 8, false);
define_integer!(UInt16Type, Ty::UInt16, "uint16", 16, false);
define_integer!(UInt32Type, Ty::UInt32, "uint32", 32, false);
define_integer!(UInt64Type, Ty::UInt64, "uint64", 64, false);
define_integer!(Int8Type, Ty::Int8, "int8", 8, true);
define_integer!(Int16Type, Ty::Int16, "int16", 16, true);
define_integer!(Int32Type, Ty::Int32, "int32", 32, true);
define_integer!(Int64Type, Ty::Int64, "int64", 64, true);

define_float!(HalfFloatType, Ty::HalfFloat, "halffloat", 16, Precision::Half);
define_float!(FloatType, Ty::Float, "float", 32, Precision::Single);
define_float!(DoubleType, Ty::Double, "double", 64, Precision::Double);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ListType<T: DataType> {
  value_field: Box<Field<T>>
}

impl <T: DataType> ListType<T> {
  pub fn new(value_field: Box<Field<T>>) -> ListType<T> {
    ListType {
      value_field
    }
  }

  pub fn value_field(&self) -> &Box<Field<T>> {
    &self.value_field
  }

  pub fn value_type(&self) -> &T {
    &self.value_field.get_type()
  }
}

impl <T: DataType> DataType for ListType<T> {
  fn ty(&self) -> Ty {
    Ty::List
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer()]
  }

  fn name(&self) -> &str {
    "list"
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BinaryType {}

impl BinaryType {
  pub fn new() -> BinaryType {
    BinaryType {}
  }
}

impl DataType for BinaryType {
  fn ty(&self) -> Ty {
    Ty::Binary
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)]
  }

  fn name(&self) -> &str {
    "binary"
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FixedSizedBinaryType {
  byte_width: i32
}

impl FixedSizedBinaryType {
  pub fn new(byte_width: i32) -> FixedSizedBinaryType {
    FixedSizedBinaryType {
      byte_width
    }
  }

  pub fn byte_width(&self) -> i32 {
    self.byte_width
  }
}

impl DataType for FixedSizedBinaryType {
  fn ty(&self) -> Ty {
    Ty::FixedSizedBinary
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "fixed_size_binary"
  }
}

impl FixedWidthType for FixedSizedBinaryType {
  fn bit_width(&self) -> i32 {
    self.byte_width * 8
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct StringType {}

impl StringType {
  pub fn new() -> StringType {
    StringType {}
  }
}

impl DataType for StringType {
  fn ty(&self) -> Ty {
    Ty::String
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::new(BufferType::Data, 8)]
  }

  fn name(&self) -> &str {
    "utf8"
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct StructType<T: DataType> {
  fields: Vec<Field<T>>
}

impl <T: DataType> StructType<T> {
  pub fn new(fields: Vec<Field<T>>) -> StructType<T> {
    StructType {
      fields
    }
  }
}

impl <T: DataType> DataType for StructType<T> {
  fn ty(&self) -> Ty {
    Ty::Struct
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer()]
  }

  fn name(&self) -> &str {
    "struct"
  }
}

impl <T: DataType> NestedType<T> for StructType<T> {
  fn child(&self, i: usize) -> &Field<T> {
    &self.fields[i]
  }

  fn get_children(&self) -> &Vec<Field<T>> {
    &self.fields
  }

  fn num_children(&self) -> i32 {
    self.fields.len() as i32
  }
}

impl <T: DataType> Index<usize> for StructType<T> {
  type Output = Field<T>;

  #[inline]
  fn index(&self, index: usize) -> &Field<T> {
    &self.fields[index]
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DecimalType {
  precision: i32,
  scale: i32
}

impl DecimalType {
  pub fn new(precision: i32, scale: i32) -> DecimalType {
    DecimalType {
      precision,
      scale
    }
  }

  pub fn precision(&self) -> i32 {
    self.precision
  }

  pub fn scale(&self) -> i32 {
    self.scale
  }
}

impl DataType for DecimalType {
  fn ty(&self) -> Ty {
    Ty::Decimal
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "decimal"
  }
}

impl FixedWidthType for DecimalType {
  fn bit_width(&self) -> i32 {
    16 * 8
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UnionType<T: DataType> {
  fields: Vec<Field<T>>,
  type_codes: Vec<u8>,
  mode: UnionMode
}

impl <T: DataType> UnionType<T> {
  pub fn new(fields: Vec<Field<T>>, type_codes: Vec<u8>) -> UnionType<T> {
    UnionType {
      fields,
      type_codes,
      mode: UnionMode::SPARSE,
    }
  }

  pub fn with_mode(fields: Vec<Field<T>>, type_codes: Vec<u8>, mode: UnionMode) -> UnionType<T> {
    UnionType {
      fields,
      type_codes,
      mode
    }
  }

  pub fn type_codes(&self) -> &Vec<u8> {
    &self.type_codes
  }

  pub fn mode(&self) -> &UnionMode {
    &self.mode
  }
}

impl <T: DataType> DataType for UnionType<T> {
  fn ty(&self) -> Ty {
    Ty::Union
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    match self.mode {
      UnionMode::SPARSE => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer()],
      _ => vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer(), BufferDesc::k_offset_buffer()]
    }
  }

  fn name(&self) -> &str {
    "union"
  }
}

impl <T: DataType> NestedType<T> for UnionType<T> {
  fn child(&self, i: usize) -> &Field<T> {
    &self.fields[i]
  }

  fn get_children(&self) -> &Vec<Field<T>> {
    &self.fields
  }

  fn num_children(&self) -> i32 {
    self.fields.len() as i32
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DateUnit {
  Day,
  Milli
}

pub trait DateType : FixedWidthType {
  fn unit(&self) -> &DateUnit;
}

macro_rules! define_date_type {
    ($type_name: ident, $ty: path, $name: expr, $bit_width: expr) => {
        #[derive(Debug, Eq, PartialEq, Clone)]
        pub struct $type_name {
          unit: DateUnit
        }

        impl $type_name {
          pub fn new(unit: DateUnit) -> $type_name {
            $type_name {
              unit
            }
          }
        }

        impl DataType for $type_name {
          fn ty(&self) -> Ty {
            $ty
          }

          fn get_buffer_layout(&self) -> Vec<BufferDesc> {
            vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
          }

          fn name(&self) -> &str {
            $name
          }
        }

        impl FixedWidthType for $type_name {
          fn bit_width(&self) -> i32 {
            $bit_width
          }
        }

        impl DateType for $type_name {
          fn unit(&self) -> &DateUnit {
            &self.unit
          }
        }
    }
}

define_date_type!(Date32Type, Ty::Date32, "date32", 32);
define_date_type!(Date64Type, Ty::Date64, "date64", 64);


pub trait TimeType : FixedWidthType {
  fn unit(&self) -> &TimeUnit;
}

macro_rules! define_time_type {
    ($type_name: ident, $ty: path, $name: expr, $bit_width: expr) => {
        #[derive(Debug, Eq, PartialEq, Clone)]
        pub struct $type_name {
          unit: TimeUnit
        }

        impl $type_name {
          pub fn new() -> $type_name {
            $type_name {
              unit: TimeUnit::Milli
            }
          }

          pub fn with_unit(unit: TimeUnit) -> $type_name {
            $type_name {
              unit
            }
          }
        }

        impl DataType for $type_name {
          fn ty(&self) -> Ty {
            $ty
          }

          fn get_buffer_layout(&self) -> Vec<BufferDesc> {
            vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
          }

          fn name(&self) -> &str {
            $name
          }
        }

        impl FixedWidthType for $type_name {
          fn bit_width(&self) -> i32 {
            $bit_width
          }
        }

        impl TimeType for $type_name {
          fn unit(&self) -> &TimeUnit {
            &self.unit
          }
        }
    }
}

define_time_type!(Time32Type, Ty::Time32, "time32", 32);
define_time_type!(Time64Type, Ty::Time64, "time64", 64);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TimestampType {
  unit: TimeUnit,
  timezone: String
}

impl TimestampType {
  pub fn new() -> TimestampType {
    TimestampType {
      unit: TimeUnit::Milli,
      timezone: String::new()
    }
  }

  pub fn with_unit(unit: TimeUnit) -> TimestampType {
    TimestampType {
      unit,
      timezone: String::new()
    }
  }

  pub fn with_unit_and_timezone(unit: TimeUnit, timezone: String) -> TimestampType {
    TimestampType {
      unit,
      timezone
    }
  }

  pub fn unit(&self) -> &TimeUnit {
    &self.unit
  }

  pub fn timezone(&self) -> &String {
    &self.timezone
  }
}

impl DataType for TimestampType {
  fn ty(&self) -> Ty {
    Ty::Timestamp
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "timestamp"
  }
}

impl FixedWidthType for TimestampType {
  fn bit_width(&self) -> i32 {
    64
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct IntervalType {
  unit: IntervalUnit
}

impl IntervalType {
  pub fn new() -> IntervalType {
    IntervalType {
      unit: IntervalUnit::YearMonth
    }
  }

  pub fn with_unit(unit: IntervalUnit) -> IntervalType {
    IntervalType {
      unit
    }
  }
}

impl DataType for IntervalType {
  fn ty(&self) -> Ty {
    Ty::Interval
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "interval"
  }
}

impl FixedWidthType for IntervalType {
  fn bit_width(&self) -> i32 {
    64
  }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DictionaryType<T: Integer, A: Array> {
  index_type: Box<T>,
  dictionary: Box<A>,
  ordered: bool
}

impl <T: Integer, A: Array> DictionaryType<T, A> {
  pub fn unordered(index_type: Box<T>, dictionary: Box<A>) -> DictionaryType<T, A> {
    DictionaryType {
      index_type,
      dictionary,
      ordered: false
    }
  }

  pub fn ordered(index_type: Box<T>, dictionary: Box<A>) -> DictionaryType<T, A> {
    DictionaryType {
      index_type,
      dictionary,
      ordered: true
    }
  }

  pub fn zindex_type(&self) -> &Box<T> {
    &self.index_type
  }

  pub fn dictionary(&self) -> &Box<A> {
    &self.dictionary
  }

  pub fn is_ordered(&self) -> bool {
    self.ordered
  }
}

impl <T: Integer, A: Array> DataType for DictionaryType<T, A> {
  fn ty(&self) -> Ty {
    Ty::Dictionary
  }

  fn get_buffer_layout(&self) -> Vec<BufferDesc> {
    vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, self.bit_width())]
  }

  fn name(&self) -> &str {
    "dictionary"
  }
}

impl <T: Integer, A: Array> FixedWidthType for DictionaryType<T, A> {
  fn bit_width(&self) -> i32 {
    self.index_type.bit_width()
  }
}

impl <T: Integer, A: Array> ToString for DictionaryType<T, A> {
  fn to_string(&self) -> String {
    String::from(self.name())
  }
}


impl Visit for BooleanType {
  fn accept(&self, visitor: &TypeVisitor) -> Result<&BooleanType, ArrowError> {
    visitor.visit_bool(&self);
    Ok(&self)
  }
}

impl Visit for UInt8Type {
  fn accept(&self, visitor: &TypeVisitor) -> Result<&Self, ArrowError> {
    visitor.visit_uint8(&self);
    Ok(&self)
  }
}

// TODO: impl Visit

impl_default_traits!(NullType);
impl_default_traits!(BooleanType);
impl_default_traits!(UInt8Type);
impl_default_traits!(UInt16Type);
impl_default_traits!(UInt32Type);
impl_default_traits!(UInt64Type);
impl_default_traits!(Int8Type);
impl_default_traits!(Int16Type);
impl_default_traits!(Int32Type);
impl_default_traits!(Int64Type);
impl_default_traits!(HalfFloatType);
impl_default_traits!(FloatType);
impl_default_traits!(DoubleType);
impl_default_traits!(BinaryType);
impl_default_traits!(FixedSizedBinaryType);
impl_default_traits!(StringType);
impl_default_traits!(DecimalType);
impl_default_traits!(Date32Type);
impl_default_traits!(Date64Type);
impl_default_traits!(Time32Type);
impl_default_traits!(Time64Type);
impl_default_traits!(TimestampType);
impl_default_traits!(IntervalType);

impl_default_traits_for_generics!(ListType);
impl_default_traits_for_generics!(UnionType);
impl_default_traits_for_generics!(StructType);

//#[derive(Debug, Eq, PartialEq)]
//pub enum DataType {
//  NullType { type_info: NullType },
//
//  BooleanType { type_info: BooleanType },
//
//  Int8Type { type_info: IntegerType },
//  Int16Type { type_info: IntegerType },
//  Int32Type { type_info: IntegerType },
//  Int64Type { type_info: IntegerType },
//  UInt8Type { type_info: IntegerType },
//  UInt16Type { type_info: IntegerType },
//  UInt32Type { type_info: IntegerType },
//  UInt64Type { type_info: IntegerType },
//
//  HalfFloatType { type_info: FloatingPointType },
//  FloatType { type_info: FloatingPointType },
//  DoubleType { type_info: FloatingPointType },
//
//  BinaryType { type_info: BinaryType },
//  StringType { type_info: StringType },
//
//  Date32Type { type_info: Date32Type },
//  Date64Type { type_info: Date64Type },
//  Time32Type { type_info: Time32Type },
//  Time64Type { type_info: Time64Type },
//  TimestampType { type_info: TimestampType },
//  IntervalType { type_info: IntervalType },
//
//  DecimalType { type_info: DecimalType },
//  ListType { type_info: ListType },
//  StructType { type_info: StructType },
//  UnionType { type_info: UnionType },
//  DictionaryType { type_info: DictionaryType }
//}


//macro_rules! as_type_info {
//    ($method_name: ident, $type_info: ty, $data_type: path) => {
//      pub fn $method_name(&self) -> &$type_info {
//        match self {
//          &$data_type { type_info: ref type_info } => type_info,
//          _ => panic!()
//        }
//      }
//    };
//}
//
//impl DataType {
//  pub fn null() -> DataType {
//    DataType::NullType { type_info: NullType::new() }
//  }
//
//  pub fn boolean() -> DataType {
//    DataType::BooleanType { type_info: BooleanType::new() }
//  }
//
//  pub fn int8() -> DataType {
//    DataType::Int8Type { type_info: IntegerType::int8() }
//  }
//
//  pub fn int16() -> DataType {
//    DataType::Int16Type { type_info: IntegerType::int16() }
//  }
//
//  pub fn int32() -> DataType {
//    DataType::Int32Type { type_info: IntegerType::int32() }
//  }
//
//  pub fn int64() -> DataType {
//    DataType::Int64Type { type_info: IntegerType::int64() }
//  }
//
//  pub fn uint8() -> DataType {
//    DataType::UInt8Type { type_info: IntegerType::uint8() }
//  }
//
//  pub fn uint16() -> DataType {
//    DataType::UInt16Type { type_info: IntegerType::uint16() }
//  }
//
//  pub fn uint32() -> DataType {
//    DataType::UInt32Type { type_info: IntegerType::uint32() }
//  }
//
//  pub fn uint64() -> DataType {
//    DataType::UInt64Type { type_info: IntegerType::uint64() }
//  }
//
//  pub fn half_float() -> DataType {
//    DataType::HalfFloatType { type_info: FloatingPointType::half_float() }
//  }
//
//  pub fn float() -> DataType {
//    DataType::FloatType { type_info: FloatingPointType::float() }
//  }
//
//  pub fn double() -> DataType {
//    DataType::DoubleType { type_info: FloatingPointType::double() }
//  }
//
//  pub fn binary() -> DataType {
//    DataType::BinaryType { type_info: BinaryType::new() }
//  }
//
//  pub fn string() -> DataType {
//    DataType::StringType { type_info: StringType::new() }
//  }
//
//  pub fn date32() -> DataType {
//    DataType::Date32Type { type_info: Date32Type::new() }
//  }
//
//  pub fn date64() -> DataType {
//    DataType::Date64Type { type_info: Date64Type::new() }
//  }
//
//  pub fn time32() -> DataType {
//    DataType::Time32Type { type_info: Time32Type::new(TimeUnit::Milli) }
//  }
//
//  pub fn time32_with(unit: TimeUnit) -> DataType {
//    DataType::Time32Type { type_info: Time32Type::new(unit) }
//  }
//
//  pub fn time64() -> DataType {
//    DataType::Time64Type { type_info: Time64Type::new(TimeUnit::Milli) }
//  }
//
//  pub fn time64_with(unit: TimeUnit) -> DataType {
//    DataType::Time64Type { type_info: Time64Type::new(unit) }
//  }
//
//  pub fn timestamp() -> DataType {
//    DataType::TimestampType { type_info: TimestampType::new(TimeUnit::Milli, "")}
//  }
//
//  pub fn timestamp_with(unit: TimeUnit) -> DataType {
//    DataType::TimestampType { type_info: TimestampType::new(unit, "") }
//  }
//
//  pub fn timestamp_for(unit: TimeUnit, timezone: &'static str) -> DataType {
//    DataType::TimestampType { type_info: TimestampType::new(unit, timezone)}
//  }
//
//  pub fn interval() -> DataType {
//    DataType::IntervalType { type_info: IntervalType::new(IntervalUnit::YearMonth) }
//  }
//
//  pub fn interval_with(unit: IntervalUnit) -> DataType {
//    DataType::IntervalType { type_info: IntervalType::new(unit) }
//  }
//
//  pub fn decimal(precision: i32, scale: i32) -> DataType {
//    DataType::DecimalType { type_info: DecimalType::new(precision, scale) }
//  }
//
//  pub fn list(ty: DataType) -> DataType {
//    DataType::ListType {type_info: ListType::new(Field::basic("item", ty))}
//  }
//
//  pub fn list_with(value_field: Field) -> DataType {
//    DataType::ListType { type_info: ListType::new(value_field) }
//  }
//
//  pub fn struc(fields: Vec<Field>) -> DataType {
//    DataType::StructType { type_info: StructType::new(fields) }
//  }
//
//  pub fn sparse_union(fields: Vec<Field>, type_codes: Vec<u8>) -> DataType {
//    DataType::UnionType { type_info: UnionType::new(UnionMode::SPARSE, fields, type_codes) }
//  }
//
//  pub fn dense_union(fields: Vec<Field>, type_codes: Vec<u8>) -> DataType {
//    DataType::UnionType { type_info: UnionType::new(UnionMode::DENSE, fields, type_codes) }
//  }
//
//  pub fn dictionary(index_type: IntegerType, dictionary: array::ArrayType) -> DataType {
//    DataType::DictionaryType { type_info: DictionaryType::new(index_type, dictionary, false) }
//  }
//
//  pub fn ordered_dictionary(index_type: IntegerType, dictionary: array::ArrayType, ordered: bool) -> DataType {
//    DataType::DictionaryType { type_info: DictionaryType::new(index_type, dictionary, ordered) }
//  }
//
//  // TODO: compile time check
//  as_type_info!(as_null_info, NullType, DataType::NullType);
//
//  as_type_info!(as_bool_info, BooleanType, DataType::BooleanType);
//
//  as_type_info!(as_int8_info, IntegerType, DataType::Int8Type);
//  as_type_info!(as_int16_info, IntegerType, DataType::Int16Type);
//  as_type_info!(as_int32_info, IntegerType, DataType::Int32Type);
//  as_type_info!(as_int64_info, IntegerType, DataType::Int64Type);
//  as_type_info!(as_uint8_info, IntegerType, DataType::UInt8Type);
//  as_type_info!(as_uint16_info, IntegerType, DataType::UInt16Type);
//  as_type_info!(as_uint32_info, IntegerType, DataType::UInt32Type);
//  as_type_info!(as_uint64_info, IntegerType, DataType::UInt64Type);
//
//  as_type_info!(as_half_float_info, FloatingPointType, DataType::HalfFloatType);
//  as_type_info!(as_float_info, FloatingPointType, DataType::FloatType);
//  as_type_info!(as_double_info, FloatingPointType, DataType::DoubleType);
//
//  as_type_info!(as_binary_info, BinaryType, DataType::BinaryType);
//  as_type_info!(as_string_info, StringType, DataType::StringType);
//
//  as_type_info!(as_date64_info, Date64Type, DataType::Date64Type);
//  as_type_info!(as_date32_info, Date32Type, DataType::Date32Type);
//  as_type_info!(as_timestamp_info, TimestampType, DataType::TimestampType);
//  as_type_info!(as_time32_info, Time32Type, DataType::Time32Type);
//  as_type_info!(as_time64_info, Time64Type, DataType::Time64Type);
//  as_type_info!(as_interval_info, IntervalType, DataType::IntervalType);
//
//  as_type_info!(as_decimal_info, DecimalType, DataType::DecimalType);
//  as_type_info!(as_list_info, ListType, DataType::ListType);
//  as_type_info!(as_struct_info, StructType, DataType::StructType);
//  as_type_info!(as_union_info, UnionType, DataType::UnionType);
//  as_type_info!(as_dictionary_info, DictionaryType, DataType::DictionaryType);
//
//  pub fn is_integer(ty: &Ty) -> bool {
//    match ty {
//      &Ty::Int8 => true,
//      &Ty::Int16 => true,
//      &Ty::Int32 => true,
//      &Ty::Int64 => true,
//      &Ty::UInt8 => true,
//      &Ty::UInt16 => true,
//      &Ty::UInt32 => true,
//      &Ty::UInt64 => true,
//      _ => false
//    }
//  }
//
//  pub fn is_float(ty: &Ty) -> bool {
//    match ty {
//      &Ty::HalfFloat => true,
//      &Ty::Float => true,
//      &Ty::Double => true,
//      _ => false
//    }
//  }
//
//  pub fn is_primitive(ty: &Ty) -> bool {
//    match ty {
//      &Ty::Null => true,
//      &Ty::Bool => true,
//      &Ty::Int8 => true,
//      &Ty::Int16 => true,
//      &Ty::Int32 => true,
//      &Ty::Int64 => true,
//      &Ty::UInt8 => true,
//      &Ty::UInt16 => true,
//      &Ty::UInt32 => true,
//      &Ty::UInt64 => true,
//      &Ty::HalfFloat => true,
//      &Ty::Float => true,
//      &Ty::Double => true,
//      &Ty::Date32 => true,
//      &Ty::Date64 => true,
//      &Ty::Time32 => true,
//      &Ty::Time64 => true,
//      &Ty::Timestamp => true,
//      &Ty::Interval => true,
//      _ => false
//    }
//  }
//
//  pub fn is_binary_like(ty: &Ty) -> bool {
//    match ty {
//      &Ty::Binary => true,
//      &Ty::String => true,
//      _ => false
//    }
//  }
//}
//
//impl ToString for DataType {
//  fn to_string(&self) -> String {
//    match self {
//      &DataType::NullType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::BooleanType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Int8Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Int16Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Int32Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Int64Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::UInt8Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::UInt16Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::UInt32Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::UInt64Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::HalfFloatType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::FloatType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::DoubleType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::BinaryType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::StringType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Date32Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Date64Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Time32Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::Time64Type { type_info: ref type_info } => type_info.to_string(),
//      &DataType::TimestampType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::IntervalType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::DecimalType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::ListType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::StructType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::UnionType { type_info: ref type_info } => type_info.to_string(),
//      &DataType::DictionaryType { type_info: ref type_info } => type_info.to_string(),
//      _ => panic!()
//    }
//  }
//}
//
//macro_rules! define_basic_type {
//    ($type_name: ident, $ty: expr, $buffer_layout: expr, $name: expr) => {
//      #[derive(Debug, Eq, PartialEq)]
//      pub struct $type_name {
//        ty: Ty,
//        buffer_layout: Vec<&'static BufferDesc>,
//        name: String
//      }
//
//      impl $type_name {
//        pub fn new() -> $type_name {
//          $type_name {
//            ty: $ty,
//            buffer_layout: $buffer_layout,
//            name: String::from($name)
//          }
//        }
//      }
//    };
//}
//
//macro_rules! define_fixed_width_type {
//    ($type_name: ident, $ty: expr, $buffer_layout: expr, $name: expr, $bit_width: expr) => {
//      #[derive(Debug, Eq, PartialEq)]
//      pub struct $type_name {
//        ty: Ty,
//        buffer_layout: Vec<&'static BufferDesc>,
//        name: String,
//        bit_width: i32
//      }
//
//      impl $type_name {
//        pub fn new() -> $type_name {
//          $type_name {
//            ty: $ty,
//            buffer_layout: $buffer_layout,
//            name: String::from($name),
//            bit_width: $bit_width
//          }
//        }
//      }
//    };
//}
//
//macro_rules! impl_arrow_type {
//    ($type_name: ident) => {
//      impl ArrowType for $type_name {
//        fn get_type(&self) -> &Ty {
//          &self.ty
//        }
//
//        fn get_buffer_layout(&self) -> &Vec<&BufferDesc> {
//          &self.buffer_layout
//        }
//
//        fn get_name(&self) -> &String {
//          &self.name
//        }
//      }
//
//      impl ToString for $type_name {
//        fn to_string(&self) -> String {
//          self.get_name().clone()
//        }
//      }
//    };
//}
//
//macro_rules! impl_fixed_width_type {
//    ($type_name: ident) => {
//      impl FixedWidthType for $type_name {
//        fn get_bit_width(&self) -> i32 {
//          self.bit_width
//        }
//      }
//    };
//}
//
//macro_rules! impl_nested_type {
//    ($type_name: ident) => {
//      impl NestedType for $type_name {
//        #[inline]
//        fn child(&self, i: usize) -> &Field {
//          &self.fields[i]
//        }
//
//        #[inline]
//        fn get_children(&self) -> &Vec<Field> {
//          &self.fields
//        }
//
//        #[inline]
//        fn num_children(&self) -> i32 {
//          self.fields.len() as i32
//        }
//      }
//
//      impl Index<usize> for $type_name {
//        type Output = Field;
//
//        #[inline]
//        fn index(&self, index: usize) -> &Field {
//          &self.fields[index]
//        }
//      }
//    };
//}
//
//define_basic_type!(NullType, Ty::Null, vec![], "null");
//impl_arrow_type!(NullType);
//define_basic_type!(BooleanType, Ty::Bool, vec![K_VALIDITY_BUFFER, K_VALUES_1], "bool");
//impl_arrow_type!(BooleanType);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct IntegerType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  is_signed: bool
//}
//
//impl IntegerType {
//  pub fn int8() -> IntegerType {
//    IntegerType::new(Ty::Int8, vec![K_VALIDITY_BUFFER, K_VALUES_8], "int8", 8, true)
//  }
//
//  pub fn int16() -> IntegerType {
//    IntegerType::new(Ty::Int16, vec![K_VALIDITY_BUFFER, K_VALUES_16], "int16", 16, true)
//  }
//
//  pub fn int32() -> IntegerType {
//    IntegerType::new(Ty::Int32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "int32", 32, true)
//  }
//
//  pub fn int64() -> IntegerType {
//    IntegerType::new(Ty::Int64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "int64", 64, true)
//  }
//
//  pub fn uint8() -> IntegerType {
//    IntegerType::new(Ty::UInt8, vec![K_VALIDITY_BUFFER, K_VALUES_8], "uint8", 8, false)
//  }
//
//  pub fn uint16() -> IntegerType {
//    IntegerType::new(Ty::UInt16, vec![K_VALIDITY_BUFFER, K_VALUES_16], "uint16", 16, false)
//  }
//
//  pub fn uint32() -> IntegerType {
//    IntegerType::new(Ty::UInt32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "uint32", 32, false)
//  }
//
//  pub fn uint64() -> IntegerType {
//    IntegerType::new(Ty::UInt64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "uint64", 64, false)
//  }
//
//  fn new(ty: Ty, buffer_layout: Vec<&'static BufferDesc>, name: &'static str, bit_width: i32, is_signed: bool) -> IntegerType {
//    IntegerType {
//      ty: ty,
//      buffer_layout: buffer_layout,
//      name: String::from(name),
//      bit_width: bit_width,
//      is_signed: is_signed
//    }
//  }
//
//  pub fn is_signed(&self) -> bool {
//    self.is_signed
//  }
//}
//
//impl_arrow_type!(IntegerType);
//impl_fixed_width_type!(IntegerType);

#[derive(Debug, Eq, PartialEq)]
pub enum Precision {
  Half,
  Single,
  Double
}

//#[derive(Debug, Eq, PartialEq)]
//pub struct FloatingPointType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  precision: Precision
//}
//
//impl FloatingPointType {
//  pub fn half_float() -> FloatingPointType {
//    FloatingPointType::new(Ty::HalfFloat, vec![K_VALIDITY_BUFFER, K_VALUES_16], "halffloat", 16, Precision::Half)
//  }
//
//  pub fn float() -> FloatingPointType {
//    FloatingPointType::new(Ty::Float, vec![K_VALIDITY_BUFFER, K_VALUES_32], "float", 32, Precision::Single)
//  }
//
//  pub fn double() -> FloatingPointType {
//    FloatingPointType::new(Ty::Double, vec![K_VALIDITY_BUFFER, K_VALUES_64], "double", 64, Precision::Double)
//  }
//
//  fn new(ty: Ty, buffer_layout: Vec<&'static BufferDesc>, name: &'static str, bit_width: i32, precision: Precision) -> FloatingPointType {
//    FloatingPointType {
//      ty: ty,
//      buffer_layout: buffer_layout,
//      name: String::from(name),
//      bit_width: bit_width,
//      precision: precision
//    }
//  }
//
//  pub fn precision(&self) -> &Precision {
//    &self.precision
//  }
//}
//
//impl_arrow_type!(FloatingPointType);
//impl_fixed_width_type!(FloatingPointType);
//
//define_basic_type!(StringType, Ty::String, vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], "utf8");
//impl_arrow_type!(StringType);
//define_basic_type!(BinaryType, Ty::Binary, vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], "binary");
//impl_arrow_type!(BinaryType);
//
//define_fixed_width_type!(Date32Type, Ty::Date32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "date32", 32);
//impl_arrow_type!(Date32Type);
//impl_fixed_width_type!(Date32Type);
//define_fixed_width_type!(Date64Type, Ty::Date64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "date64", 64);
//impl_arrow_type!(Date64Type);
//impl_fixed_width_type!(Date64Type);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct Time32Type {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  unit: TimeUnit
//}
//
//impl Time32Type {
//  pub fn new(unit: TimeUnit) -> Time32Type {
//    Time32Type {
//      ty: Ty::Time32,
//      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_32],
//      name: String::from("time32"),
//      bit_width: 32,
//      unit: unit
//    }
//  }
//
//  pub fn unit(&self) -> &TimeUnit {
//    &self.unit
//  }
//}
//
//impl_arrow_type!(Time32Type);
//impl_fixed_width_type!(Time32Type);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct Time64Type {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  unit: TimeUnit
//}
//
//impl Time64Type {
//  pub fn new(unit: TimeUnit) -> Time64Type {
//    Time64Type {
//      ty: Ty::Time64,
//      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
//      name: String::from("time64"),
//      bit_width: 64,
//      unit: unit
//    }
//  }
//
//  pub fn unit(&self) -> &TimeUnit {
//    &self.unit
//  }
//}
//
//impl_arrow_type!(Time64Type);
//impl_fixed_width_type!(Time64Type);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct TimestampType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  unit: TimeUnit,
//  timezone: String
//}
//
//impl TimestampType {
//  pub fn new(unit: TimeUnit, timezone: &'static str) -> TimestampType {
//    TimestampType {
//      ty: Ty::Timestamp,
//      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
//      name: String::from("timestamp"),
//      bit_width: 64,
//      unit: unit,
//      timezone: String::from(timezone)
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
//impl_arrow_type!(TimestampType);
//impl_fixed_width_type!(TimestampType);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum IntervalUnit {
  YearMonth,
  DayTime
}

//#[derive(Debug, Eq, PartialEq)]
//pub struct IntervalType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  bit_width: i32,
//  unit: IntervalUnit
//}
//
//impl IntervalType {
//  pub fn new(unit: IntervalUnit) -> IntervalType {
//    IntervalType {
//      ty: Ty::Interval,
//      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
//      name: String::from("interval"),
//      bit_width: 64,
//      unit: unit
//    }
//  }
//
//  pub fn unit(&self) -> &IntervalUnit {
//    &self.unit
//  }
//}
//
//impl_arrow_type!(IntervalType);
//impl_fixed_width_type!(IntervalType);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct DecimalType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  precision: i32,
//  scale: i32
//}
//
//impl DecimalType {
//  pub fn new(precision: i32, scale: i32) -> DecimalType {
//    DecimalType {
//      ty: Ty::Decimal,
//      buffer_layout: vec![], // TODO
//      name: String::from("decimal"),
//      precision: precision,
//      scale: scale
//    }
//  }
//
//  pub fn precision(&self) -> i32 {
//    self.precision
//  }
//
//  pub fn scale(&self) -> i32 {
//    self.precision
//  }
//}
//
//impl_arrow_type!(DecimalType);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct ListType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  value_field: Box<Field>
//}
//
//impl ListType {
//  pub fn new(value_field: Field) -> ListType {
//    ListType {
//      ty: Ty::List,
//      buffer_layout: vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER],
//      name: String::from("list"),
//      value_field: Box::new(value_field)
//    }
//  }
//
//  pub fn value_type(&self) -> &DataType {
//    self.value_field.get_type()
//  }
//
//  pub fn value_field(&self) -> &Field {
//    &self.value_field
//  }
//}
//
//impl_arrow_type!(ListType);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct StructType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  fields: Box<Vec<Field>>
//}
//
//impl StructType {
//  pub fn new(fields: Vec<Field>) -> StructType {
//    StructType {
//      ty: Ty::Struct,
//      buffer_layout: vec![K_VALIDITY_BUFFER],
//      name: String::from("struct"),
//      fields: Box::new(fields)
//    }
//  }
//}
//
//impl_arrow_type!(StructType);
//impl_nested_type!(StructType);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum UnionMode {
  SPARSE,
  DENSE
}

//#[derive(Debug, Eq, PartialEq)]
//pub struct UnionType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  mode: UnionMode,
//  fields: Box<Vec<Field>>,
//  type_codes: Vec<u8>
//}
//
//impl UnionType {
//  pub fn new(mode: UnionMode, fields: Vec<Field>, type_codes: Vec<u8>) -> UnionType {
//    let buffer_layout = if mode == UnionMode::SPARSE {
//      vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER]
//    } else {
//      vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER, K_OFFSET_BUFFER]
//    };
//
//    UnionType {
//      ty: Ty::Union,
//      buffer_layout: buffer_layout,
//      name: String::from("union"),
//      mode: mode,
//      fields: Box::new(fields),
//      type_codes: type_codes
//    }
//  }
//
//  pub fn mode(&self) -> &UnionMode {
//    &self.mode
//  }
//
//  pub fn type_codes(&self) -> &Vec<u8> {
//    &self.type_codes
//  }
//}
//
//impl_arrow_type!(UnionType);
//impl_nested_type!(UnionType);
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct DictionaryType {
//  ty: Ty,
//  buffer_layout: Vec<&'static BufferDesc>,
//  name: String,
//  index_type: Box<IntegerType>,
//  dictionary: Box<array::ArrayType>,
//  ordered: bool
//}
//
//impl DictionaryType {
//  pub fn new(index_type: IntegerType, dictionary: array::ArrayType, ordered: bool) -> DictionaryType {
//    DictionaryType {
//      ty: Ty::Dictionary,
//      buffer_layout: vec![K_VALIDITY_BUFFER, get_data_buffer_desc(index_type.bit_width)],
//      name: String::from("dictionary"),
//      index_type: Box::new(index_type),
//      dictionary: Box::new(dictionary),
//      ordered: ordered
//    }
//  }
//
//  pub fn index_type(&self) -> &IntegerType {
//    &self.index_type
//  }
//
//  pub fn dictionary(&self) -> &array::ArrayType {
//    &self.dictionary
//  }
//
//  pub fn ordered(&self) -> bool {
//    self.ordered
//  }
//}
//
//impl_arrow_type!(DictionaryType);
//
//impl FixedWidthType for DictionaryType {
//  fn get_bit_width(&self) -> i32 {
//    self.index_type.get_bit_width()
//  }
//}

//fn clone_data_type(data_type: &Box<DataType>) -> Box<DataType> {
//  let clone = unsafe {
//    let size = std::mem::size_of_val(data_type.as_ref());
//    let p = std::libc::malloc(size);
//    std::libc::memcpy(p, std::mem::transmute::<&DataType, *const std::libc::c_void>(data_type.as_ref()), size)
//  };
//
//  Box::new(clone)
//}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Field<T: DataType> {
  name: String,
  ty: T,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl <T: DataType> Field<T> {
  pub fn new(name: String, ty: T) -> Field<T> {
    Field::create(name, ty, true, Option::None)
  }

  pub fn non_nullable(name: String, ty: T) -> Field<T> {
    Field::create(name, ty, false, Option::None)
  }

  pub fn with_metadata(name: String, ty: T, metadata: KeyValueMetadata) -> Field<T> {
    Field::create(name, ty, true, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, ty: T, metadata: KeyValueMetadata) -> Field<T> {
    Field::create(name, ty, false, Option::from(metadata))
  }

  fn create(name: String, ty: T, nullable: bool, metadata: Option<KeyValueMetadata>) -> Field<T> {
    Field {
      name,
      ty,
      nullable,
      metadata
    }
  }

  pub fn get_name(&self) -> &String {
    &self.name
  }

  pub fn get_type(&self) -> &T {
    &self.ty
  }

  pub fn nullable(&self) -> bool {
    self.nullable
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Field<T> {
    Field::create(self.name.clone(), self.ty.clone(), self.nullable, Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> Field<T> {
    Field::create(self.name.clone(), self.ty.clone(), self.nullable, Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl <T: DataType> ToString for Field<T> {
  fn to_string(&self) -> String {
    let str = self.name.clone() + ": " + self.ty.to_string().as_str();
    if self.nullable {
      str + " not null"
    } else {
      str
    }
  }
}