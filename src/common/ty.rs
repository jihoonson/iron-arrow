use std::ops::Index;

use array;

#[macro_use]
use std;

/// Data types in this library are all *logical*. They can be expressed as
/// either a primitive physical type (bytes or bits of some fixed size), a
/// nested type consisting of other data types, or another data type (e.g. a
/// timestamp encoded as an int64)
#[derive(Debug, Eq, PartialEq)]
pub enum Ty {
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

  // YEAR_MONTH or DAY_TIME interval in SQL style
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

#[derive(Debug, Eq, PartialEq)]
pub enum TimeUnit {
  Second,
  Milli,
  Micro,
  Nano
}

#[derive(Debug, Eq, PartialEq)]
pub enum BufferType {
  Data,
  Offset,
  Type,
  Validity
}

#[derive(Debug, Eq, PartialEq)]
pub struct BufferDesc {
  ty: BufferType,
  bit_width: i32
}

// TODO: pub?
pub static K_VALIDITY_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::Validity,
  bit_width: 1
};

pub static K_OFFSET_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::Offset,
  bit_width: 32
};

pub static K_TYPE_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::Type,
  bit_width: 32
};

pub static K_VALUES_1: &'static BufferDesc = &BufferDesc {
  ty: BufferType::Data,
  bit_width: 1
};

macro_rules! define_buffer_desc {
  ($name: ident, $width: expr) => (
    pub static $name: &'static BufferDesc = &BufferDesc{
      ty: BufferType::Data,
      bit_width: $width
    };
  );
}

define_buffer_desc!(K_VALUES_8, 8);
define_buffer_desc!(K_VALUES_16, 16);
define_buffer_desc!(K_VALUES_32, 32);
define_buffer_desc!(K_VALUES_64, 64);

fn get_data_buffer_desc(bit_width: i32) -> &'static BufferDesc {
  match bit_width {
    1 => K_VALUES_1,
    8 => K_VALUES_8,
    16 => K_VALUES_16,
    32 => K_VALUES_32,
    64 => K_VALUES_64,
    _ => panic!()
  }
}

// Required to implement this trait for every data types
pub trait ArrowType {
  fn get_type(&self) -> &Ty;
  fn get_buffer_layout(&self) -> &Vec<&BufferDesc>;
  fn get_name(&self) -> &String;
}

// Required to implement this trait for structured data types
pub trait NestedType {
  fn child(&self, i: usize) -> &Field;
  fn get_children(&self) -> &Vec<Field>;
  fn num_children(&self) -> i32;
}

// Required to implement this trait for fixed-size data types
pub trait FixedWidthType {
  fn get_bit_width(&self) -> i32;
}

#[derive(Debug, Eq, PartialEq)]
pub enum DataType {
  NullType { type_info: NullType },

  BooleanType { type_info: BooleanType },

  Int8Type { type_info: IntegerType },
  Int16Type { type_info: IntegerType },
  Int32Type { type_info: IntegerType },
  Int64Type { type_info: IntegerType },
  UInt8Type { type_info: IntegerType },
  UInt16Type { type_info: IntegerType },
  UInt32Type { type_info: IntegerType },
  UInt64Type { type_info: IntegerType },

  HalfFloatType { type_info: FloatingPointType },
  FloatType { type_info: FloatingPointType },
  DoubleType { type_info: FloatingPointType },

  BinaryType { type_info: BinaryType },
  StringType { type_info: StringType },

  Date32Type { type_info: Date32Type },
  Date64Type { type_info: Date64Type },
  Time32Type { type_info: Time32Type },
  Time64Type { type_info: Time64Type },
  TimestampType { type_info: TimestampType },
  IntervalType { type_info: IntervalType },

  DecimalType { type_info: DecimalType },
  ListType { type_info: ListType },
  StructType { type_info: StructType },
  UnionType { type_info: UnionType },
  DictionaryType { type_info: DictionaryType }
}

macro_rules! as_type_info {
    ($method_name: ident, $type_info: ty, $data_type: path) => {
      pub fn $method_name(&self) -> &$type_info {
        match self {
          &$data_type { type_info: ref type_info } => type_info,
          _ => panic!()
        }
      }
    };
}

impl DataType {
  pub fn null() -> DataType {
    DataType::NullType { type_info: NullType::new() }
  }

  pub fn boolean() -> DataType {
    DataType::BooleanType { type_info: BooleanType::new() }
  }

  pub fn int8() -> DataType {
    DataType::Int8Type { type_info: IntegerType::int8() }
  }

  pub fn int16() -> DataType {
    DataType::Int16Type { type_info: IntegerType::int16() }
  }

  pub fn int32() -> DataType {
    DataType::Int32Type { type_info: IntegerType::int32() }
  }

  pub fn int64() -> DataType {
    DataType::Int64Type { type_info: IntegerType::int64() }
  }

  pub fn uint8() -> DataType {
    DataType::UInt8Type { type_info: IntegerType::uint8() }
  }

  pub fn uint16() -> DataType {
    DataType::UInt16Type { type_info: IntegerType::uint16() }
  }

  pub fn uint32() -> DataType {
    DataType::UInt32Type { type_info: IntegerType::uint32() }
  }

  pub fn uint64() -> DataType {
    DataType::UInt64Type { type_info: IntegerType::uint64() }
  }

  pub fn half_float() -> DataType {
    DataType::HalfFloatType { type_info: FloatingPointType::half_float() }
  }

  pub fn float() -> DataType {
    DataType::FloatType { type_info: FloatingPointType::float() }
  }

  pub fn double() -> DataType {
    DataType::DoubleType { type_info: FloatingPointType::double() }
  }

  pub fn binary() -> DataType {
    DataType::BinaryType { type_info: BinaryType::new() }
  }

  pub fn string() -> DataType {
    DataType::StringType { type_info: StringType::new() }
  }

  pub fn date32() -> DataType {
    DataType::Date32Type { type_info: Date32Type::new() }
  }

  pub fn date64() -> DataType {
    DataType::Date64Type { type_info: Date64Type::new() }
  }

  pub fn time32() -> DataType {
    DataType::Time32Type { type_info: Time32Type::new(TimeUnit::Milli) }
  }

  pub fn time32_with(unit: TimeUnit) -> DataType {
    DataType::Time32Type { type_info: Time32Type::new(unit) }
  }

  pub fn time64() -> DataType {
    DataType::Time64Type { type_info: Time64Type::new(TimeUnit::Milli) }
  }

  pub fn time64_with(unit: TimeUnit) -> DataType {
    DataType::Time64Type { type_info: Time64Type::new(unit) }
  }

  pub fn timestamp() -> DataType {
    DataType::TimestampType { type_info: TimestampType::new(TimeUnit::Milli, "")}
  }

  pub fn timestamp_with(unit: TimeUnit) -> DataType {
    DataType::TimestampType { type_info: TimestampType::new(unit, "") }
  }

  pub fn timestamp_for(unit: TimeUnit, timezone: &'static str) -> DataType {
    DataType::TimestampType { type_info: TimestampType::new(unit, timezone)}
  }

  pub fn interval() -> DataType {
    DataType::IntervalType { type_info: IntervalType::new(IntervalUnit::YEAR_MONTH) }
  }

  pub fn interval_with(unit: IntervalUnit) -> DataType {
    DataType::IntervalType { type_info: IntervalType::new(unit) }
  }

  pub fn decimal(precision: i32, scale: i32) -> DataType {
    DataType::DecimalType { type_info: DecimalType::new(precision, scale) }
  }

  pub fn list(ty: DataType) -> DataType {
    DataType::ListType {type_info: ListType::new(Field::basic("item", ty))}
  }

  pub fn list_with(value_field: Field) -> DataType {
    DataType::ListType { type_info: ListType::new(value_field) }
  }

  pub fn struc(fields: Vec<Field>) -> DataType {
    DataType::StructType { type_info: StructType::new(fields) }
  }

  pub fn sparse_union(fields: Vec<Field>, type_codes: Vec<u8>) -> DataType {
    DataType::UnionType { type_info: UnionType::new(UnionMode::SPARSE, fields, type_codes) }
  }

  pub fn dense_union(fields: Vec<Field>, type_codes: Vec<u8>) -> DataType {
    DataType::UnionType { type_info: UnionType::new(UnionMode::DENSE, fields, type_codes) }
  }

  pub fn dictionary(index_type: IntegerType, dictionary: array::Array) -> DataType {
    DataType::DictionaryType { type_info: DictionaryType::new(index_type, dictionary, false) }
  }

  pub fn dictionary_with(index_type: IntegerType, dictionary: array::Array, ordered: bool) -> DataType {
    DataType::DictionaryType { type_info: DictionaryType::new(index_type, dictionary, ordered) }
  }

  // TODO: compile time check
  as_type_info!(as_null_info, NullType, DataType::NullType);

  as_type_info!(as_bool_info, BooleanType, DataType::BooleanType);

  as_type_info!(as_int8_info, IntegerType, DataType::Int8Type);
  as_type_info!(as_int16_info, IntegerType, DataType::Int16Type);
  as_type_info!(as_int32_info, IntegerType, DataType::Int32Type);
  as_type_info!(as_int64_info, IntegerType, DataType::Int64Type);
  as_type_info!(as_uint8_info, IntegerType, DataType::UInt8Type);
  as_type_info!(as_uint16_info, IntegerType, DataType::UInt16Type);
  as_type_info!(as_uint32_info, IntegerType, DataType::UInt32Type);
  as_type_info!(as_uint64_info, IntegerType, DataType::UInt64Type);

  as_type_info!(as_half_float_info, FloatingPointType, DataType::HalfFloatType);
  as_type_info!(as_float_info, FloatingPointType, DataType::FloatType);
  as_type_info!(as_double_info, FloatingPointType, DataType::DoubleType);

  as_type_info!(as_binary_info, BinaryType, DataType::BinaryType);
  as_type_info!(as_string_info, StringType, DataType::StringType);

  as_type_info!(as_date64_info, Date64Type, DataType::Date64Type);
  as_type_info!(as_date32_info, Date32Type, DataType::Date32Type);
  as_type_info!(as_timestamp_info, TimestampType, DataType::TimestampType);
  as_type_info!(as_time32_info, Time32Type, DataType::Time32Type);
  as_type_info!(as_time64_info, Time64Type, DataType::Time64Type);
  as_type_info!(as_interval_info, IntervalType, DataType::IntervalType);

  as_type_info!(as_decimal_info, DecimalType, DataType::DecimalType);
  as_type_info!(as_list_info, ListType, DataType::ListType);
  as_type_info!(as_struct_info, StructType, DataType::StructType);
  as_type_info!(as_union_info, UnionType, DataType::UnionType);
  as_type_info!(as_dictionary_info, DictionaryType, DataType::DictionaryType);

  pub fn is_integer(ty: &Ty) -> bool {
    match ty {
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

  pub fn is_float(ty: &Ty) -> bool {
    match ty {
      &Ty::HalfFloat => true,
      &Ty::Float => true,
      &Ty::Double => true,
      _ => false
    }
  }

  pub fn is_primitive(ty: &Ty) -> bool {
    match ty {
      &Ty::Null => true,
      &Ty::Bool => true,
      &Ty::Int8 => true,
      &Ty::Int16 => true,
      &Ty::Int32 => true,
      &Ty::Int64 => true,
      &Ty::UInt8 => true,
      &Ty::UInt16 => true,
      &Ty::UInt32 => true,
      &Ty::UInt64 => true,
      &Ty::HalfFloat => true,
      &Ty::Float => true,
      &Ty::Double => true,
      &Ty::Date32 => true,
      &Ty::Date64 => true,
      &Ty::Time32 => true,
      &Ty::Time64 => true,
      &Ty::Timestamp => true,
      &Ty::Interval => true,
      _ => false
    }
  }

  pub fn is_binary_like(ty: &Ty) -> bool {
    match ty {
      &Ty::Binary => true,
      &Ty::String => true,
      _ => false
    }
  }
}

impl ToString for DataType {
  fn to_string(&self) -> String {
    match self {
      &DataType::NullType { type_info: ref type_info } => type_info.to_string(),
      &DataType::BooleanType { type_info: ref type_info } => type_info.to_string(),
      &DataType::Int8Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Int16Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Int32Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Int64Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::UInt8Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::UInt16Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::UInt32Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::UInt64Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::HalfFloatType { type_info: ref type_info } => type_info.to_string(),
      &DataType::FloatType { type_info: ref type_info } => type_info.to_string(),
      &DataType::DoubleType { type_info: ref type_info } => type_info.to_string(),
      &DataType::BinaryType { type_info: ref type_info } => type_info.to_string(),
      &DataType::StringType { type_info: ref type_info } => type_info.to_string(),
      &DataType::Date32Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Date64Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Time32Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::Time64Type { type_info: ref type_info } => type_info.to_string(),
      &DataType::TimestampType { type_info: ref type_info } => type_info.to_string(),
      &DataType::IntervalType { type_info: ref type_info } => type_info.to_string(),
      &DataType::DecimalType { type_info: ref type_info } => type_info.to_string(),
      &DataType::ListType { type_info: ref type_info } => type_info.to_string(),
      &DataType::StructType { type_info: ref type_info } => type_info.to_string(),
      &DataType::UnionType { type_info: ref type_info } => type_info.to_string(),
      &DataType::DictionaryType { type_info: ref type_info } => type_info.to_string(),
      _ => panic!()
    }
  }
}

macro_rules! define_basic_type {
    ($type_name: ident, $ty: expr, $buffer_layout: expr, $name: expr) => {
      #[derive(Debug, Eq, PartialEq)]
      pub struct $type_name {
        ty: Ty,
        buffer_layout: Vec<&'static BufferDesc>,
        name: String
      }

      impl $type_name {
        pub fn new() -> $type_name {
          $type_name {
            ty: $ty,
            buffer_layout: $buffer_layout,
            name: String::from($name)
          }
        }
      }
    };
}

macro_rules! define_fixed_width_type {
    ($type_name: ident, $ty: expr, $buffer_layout: expr, $name: expr, $bit_width: expr) => {
      #[derive(Debug, Eq, PartialEq)]
      pub struct $type_name {
        ty: Ty,
        buffer_layout: Vec<&'static BufferDesc>,
        name: String,
        bit_width: i32
      }

      impl $type_name {
        pub fn new() -> $type_name {
          $type_name {
            ty: $ty,
            buffer_layout: $buffer_layout,
            name: String::from($name),
            bit_width: $bit_width
          }
        }
      }
    };
}

macro_rules! impl_arrow_type {
    ($type_name: ident) => {
      impl ArrowType for $type_name {
        fn get_type(&self) -> &Ty {
          &self.ty
        }

        fn get_buffer_layout(&self) -> &Vec<&BufferDesc> {
          &self.buffer_layout
        }

        fn get_name(&self) -> &String {
          &self.name
        }
      }

      impl ToString for $type_name {
        fn to_string(&self) -> String {
          self.get_name().clone()
        }
      }
    };
}

macro_rules! impl_fixed_width_type {
    ($type_name: ident) => {
      impl FixedWidthType for $type_name {
        fn get_bit_width(&self) -> i32 {
          self.bit_width
        }
      }
    };
}

macro_rules! impl_nested_type {
    ($type_name: ident) => {
      impl NestedType for $type_name {
        #[inline]
        fn child(&self, i: usize) -> &Field {
          &self.fields[i]
        }

        #[inline]
        fn get_children(&self) -> &Vec<Field> {
          &self.fields
        }

        #[inline]
        fn num_children(&self) -> i32 {
          self.fields.len() as i32
        }
      }

      impl Index<usize> for $type_name {
        type Output = Field;

        #[inline]
        fn index(&self, index: usize) -> &Field {
          &self.fields[index]
        }
      }
    };
}

define_basic_type!(NullType, Ty::Null, vec![], "null");
impl_arrow_type!(NullType);
define_basic_type!(BooleanType, Ty::Bool, vec![K_VALIDITY_BUFFER, K_VALUES_1], "bool");
impl_arrow_type!(BooleanType);

#[derive(Debug, Eq, PartialEq)]
pub struct IntegerType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  is_signed: bool
}

impl IntegerType {
  pub fn int8() -> IntegerType {
    IntegerType::new(Ty::Int8, vec![K_VALIDITY_BUFFER, K_VALUES_8], "int8", 8, true)
  }

  pub fn int16() -> IntegerType {
    IntegerType::new(Ty::Int16, vec![K_VALIDITY_BUFFER, K_VALUES_16], "int16", 16, true)
  }

  pub fn int32() -> IntegerType {
    IntegerType::new(Ty::Int32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "int32", 32, true)
  }

  pub fn int64() -> IntegerType {
    IntegerType::new(Ty::Int64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "int64", 64, true)
  }

  pub fn uint8() -> IntegerType {
    IntegerType::new(Ty::UInt8, vec![K_VALIDITY_BUFFER, K_VALUES_8], "uint8", 8, false)
  }

  pub fn uint16() -> IntegerType {
    IntegerType::new(Ty::UInt16, vec![K_VALIDITY_BUFFER, K_VALUES_16], "uint16", 16, false)
  }

  pub fn uint32() -> IntegerType {
    IntegerType::new(Ty::UInt32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "uint32", 32, false)
  }

  pub fn uint64() -> IntegerType {
    IntegerType::new(Ty::UInt64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "uint64", 64, false)
  }

  fn new(ty: Ty, buffer_layout: Vec<&'static BufferDesc>, name: &'static str, bit_width: i32, is_signed: bool) -> IntegerType {
    IntegerType {
      ty: ty,
      buffer_layout: buffer_layout,
      name: String::from(name),
      bit_width: bit_width,
      is_signed: is_signed
    }
  }

  pub fn is_signed(&self) -> bool {
    self.is_signed
  }
}

impl_arrow_type!(IntegerType);
impl_fixed_width_type!(IntegerType);

#[derive(Debug, Eq, PartialEq)]
pub enum Precision {
  Half,
  Single,
  Double
}

#[derive(Debug, Eq, PartialEq)]
pub struct FloatingPointType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  precision: Precision
}

impl FloatingPointType {
  pub fn half_float() -> FloatingPointType {
    FloatingPointType::new(Ty::HalfFloat, vec![K_VALIDITY_BUFFER, K_VALUES_16], "halffloat", 16, Precision::Half)
  }

  pub fn float() -> FloatingPointType {
    FloatingPointType::new(Ty::Float, vec![K_VALIDITY_BUFFER, K_VALUES_32], "float", 32, Precision::Single)
  }

  pub fn double() -> FloatingPointType {
    FloatingPointType::new(Ty::Double, vec![K_VALIDITY_BUFFER, K_VALUES_64], "double", 64, Precision::Double)
  }

  fn new(ty: Ty, buffer_layout: Vec<&'static BufferDesc>, name: &'static str, bit_width: i32, precision: Precision) -> FloatingPointType {
    FloatingPointType {
      ty: ty,
      buffer_layout: buffer_layout,
      name: String::from(name),
      bit_width: bit_width,
      precision: precision
    }
  }

  pub fn precision(&self) -> &Precision {
    &self.precision
  }
}

impl_arrow_type!(FloatingPointType);
impl_fixed_width_type!(FloatingPointType);

define_basic_type!(StringType, Ty::String, vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], "utf8");
impl_arrow_type!(StringType);
define_basic_type!(BinaryType, Ty::Binary, vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], "binary");
impl_arrow_type!(BinaryType);

define_fixed_width_type!(Date32Type, Ty::Date32, vec![K_VALIDITY_BUFFER, K_VALUES_32], "date32", 32);
impl_arrow_type!(Date32Type);
impl_fixed_width_type!(Date32Type);
define_fixed_width_type!(Date64Type, Ty::Date64, vec![K_VALIDITY_BUFFER, K_VALUES_64], "date64", 64);
impl_arrow_type!(Date64Type);
impl_fixed_width_type!(Date64Type);

#[derive(Debug, Eq, PartialEq)]
pub struct Time32Type {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  unit: TimeUnit
}

impl Time32Type {
  pub fn new(unit: TimeUnit) -> Time32Type {
    Time32Type {
      ty: Ty::Time32,
      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_32],
      name: String::from("time32"),
      bit_width: 32,
      unit: unit
    }
  }

  pub fn unit(&self) -> &TimeUnit {
    &self.unit
  }
}

impl_arrow_type!(Time32Type);
impl_fixed_width_type!(Time32Type);

#[derive(Debug, Eq, PartialEq)]
pub struct Time64Type {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  unit: TimeUnit
}

impl Time64Type {
  pub fn new(unit: TimeUnit) -> Time64Type {
    Time64Type {
      ty: Ty::Time64,
      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
      name: String::from("time64"),
      bit_width: 64,
      unit: unit
    }
  }

  pub fn unit(&self) -> &TimeUnit {
    &self.unit
  }
}

impl_arrow_type!(Time64Type);
impl_fixed_width_type!(Time64Type);

#[derive(Debug, Eq, PartialEq)]
pub struct TimestampType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  unit: TimeUnit,
  timezone: String
}

impl TimestampType {
  pub fn new(unit: TimeUnit, timezone: &'static str) -> TimestampType {
    TimestampType {
      ty: Ty::Timestamp,
      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
      name: String::from("timestamp"),
      bit_width: 64,
      unit: unit,
      timezone: String::from(timezone)
    }
  }

  pub fn unit(&self) -> &TimeUnit {
    &self.unit
  }

  pub fn timezone(&self) -> &String {
    &self.timezone
  }
}

impl_arrow_type!(TimestampType);
impl_fixed_width_type!(TimestampType);

#[derive(Debug, Eq, PartialEq)]
pub enum IntervalUnit {
  YEAR_MONTH,
  DAY_TIME
}

#[derive(Debug, Eq, PartialEq)]
pub struct IntervalType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  bit_width: i32,
  unit: IntervalUnit
}

impl IntervalType {
  pub fn new(unit: IntervalUnit) -> IntervalType {
    IntervalType {
      ty: Ty::Interval,
      buffer_layout: vec![K_VALIDITY_BUFFER, K_VALUES_64],
      name: String::from("interval"),
      bit_width: 64,
      unit: unit
    }
  }

  pub fn unit(&self) -> &IntervalUnit {
    &self.unit
  }
}

impl_arrow_type!(IntervalType);
impl_fixed_width_type!(IntervalType);

#[derive(Debug, Eq, PartialEq)]
pub struct DecimalType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  precision: i32,
  scale: i32
}

impl DecimalType {
  pub fn new(precision: i32, scale: i32) -> DecimalType {
    DecimalType {
      ty: Ty::Decimal,
      buffer_layout: vec![], // TODO
      name: String::from("decimal"),
      precision: precision,
      scale: scale
    }
  }

  pub fn precision(&self) -> i32 {
    self.precision
  }

  pub fn scale(&self) -> i32 {
    self.precision
  }
}

impl_arrow_type!(DecimalType);

#[derive(Debug, Eq, PartialEq)]
pub struct ListType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  value_field: Box<Field>
}

impl ListType {
  pub fn new(value_field: Field) -> ListType {
    ListType {
      ty: Ty::List,
      buffer_layout: vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER],
      name: String::from("list"),
      value_field: Box::new(value_field)
    }
  }

  pub fn value_type(&self) -> &DataType {
    self.value_field.get_type()
  }

  pub fn value_field(&self) -> &Field {
    &self.value_field
  }
}

impl_arrow_type!(ListType);

#[derive(Debug, Eq, PartialEq)]
pub struct StructType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  fields: Box<Vec<Field>>
}

impl StructType {
  pub fn new(fields: Vec<Field>) -> StructType {
    StructType {
      ty: Ty::Struct,
      buffer_layout: vec![K_VALIDITY_BUFFER],
      name: String::from("struct"),
      fields: Box::new(fields)
    }
  }
}

impl_arrow_type!(StructType);
impl_nested_type!(StructType);

#[derive(Debug, Eq, PartialEq)]
pub enum UnionMode {
  SPARSE,
  DENSE
}

#[derive(Debug, Eq, PartialEq)]
pub struct UnionType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  mode: UnionMode,
  fields: Box<Vec<Field>>,
  type_codes: Vec<u8>
}

impl UnionType {
  pub fn new(mode: UnionMode, fields: Vec<Field>, type_codes: Vec<u8>) -> UnionType {
    let buffer_layout = if mode == UnionMode::SPARSE {
      vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER]
    } else {
      vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER, K_OFFSET_BUFFER]
    };

    UnionType {
      ty: Ty::Union,
      buffer_layout: buffer_layout,
      name: String::from("union"),
      mode: mode,
      fields: Box::new(fields),
      type_codes: type_codes
    }
  }

  pub fn mode(&self) -> &UnionMode {
    &self.mode
  }

  pub fn type_codes(&self) -> &Vec<u8> {
    &self.type_codes
  }
}

impl_arrow_type!(UnionType);
impl_nested_type!(UnionType);

#[derive(Debug, Eq, PartialEq)]
pub struct DictionaryType {
  ty: Ty,
  buffer_layout: Vec<&'static BufferDesc>,
  name: String,
  index_type: Box<IntegerType>,
  dictionary: Box<array::Array>,
  ordered: bool
}

impl DictionaryType {
  pub fn new(index_type: IntegerType, dictionary: array::Array, ordered: bool) -> DictionaryType {
    DictionaryType {
      ty: Ty::Dictionary,
      buffer_layout: vec![K_VALIDITY_BUFFER, get_data_buffer_desc(index_type.bit_width)],
      name: String::from("dictionary"),
      index_type: Box::new(index_type),
      dictionary: Box::new(dictionary),
      ordered: ordered
    }
  }

  pub fn index_type(&self) -> &IntegerType {
    &self.index_type
  }

  pub fn dictionary(&self) -> &array::Array {
    &self.dictionary
  }

  pub fn ordered(&self) -> bool {
    self.ordered
  }
}

impl_arrow_type!(DictionaryType);

impl FixedWidthType for DictionaryType {
  fn get_bit_width(&self) -> i32 {
    self.index_type.get_bit_width()
  }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Field {
  name: String,
  ty: Box<DataType>,
  nullable: bool,
  // optional dictionary id if the field is dictionary encoded
  // 0 means it's not dictionary encoded
  dictionary: i64
}

impl Field {
  pub fn basic(name: &'static str, ty: DataType) -> Field {
    Field::new(name, ty, true, 0)
  }

  pub fn non_null(name: &'static str, ty: DataType) -> Field {
    Field::new(name, ty, false, 0)
  }

  pub fn with_dic(name: &'static str, ty: DataType, dictionary: i64) -> Field {
    Field::new(name, ty, true, dictionary)
  }

  pub fn non_null_with_dic(name: &'static str, ty: DataType, dictionary: i64) -> Field {
    Field::new(name, ty, false, dictionary)
  }

  fn new(name: &'static str, ty: DataType, nullable: bool, dictionary: i64) -> Field {
    Field {
      name: String::from(name),
      ty: Box::new(ty),
      nullable: nullable,
      dictionary: dictionary
    }
  }

  pub fn get_name(&self) -> &String {
    &self.name
  }

  pub fn get_type(&self) -> &DataType {
    &self.ty
  }

  pub fn is_nullable(&self) -> bool {
    self.nullable
  }

  pub fn get_dictionary(&self) -> i64 {
    self.dictionary
  }
}

impl ToString for Field {
  fn to_string(&self) -> String {
    let str = self.name.clone() + ": " + self.ty.to_string().as_str();
    if self.nullable {
      str + " not null"
    } else {
      str
    }
  }
}