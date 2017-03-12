use std::ops::Index;
use std::mem;

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
  Uint8,
  Int8,
  Uint16,
  Int16,
  Uint32,
  Int32,
  Uint64,
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
  Date,

  // int32_t days since the UNIX epoch
  Date32,

  // Exact timestamp encoded with int64 since UNIX epoch
  // Default unit millisecond
  Timestamp { unit: TimeUnit },

  // Exact time encoded with int64, default unit millisecond
  Time { unit: TimeUnit },

  // YEAR_MONTH or DAY_TIME interval in SQL style
  Interval { unit: TimeUnit },

  // Precision- and scale-based decimal type. Storage type depends on the
  // parameters.
  Decimal { precision: i32, scale: i32 },

  // A list of some logical data type
  List { field: Box<Field> },

  // Struct of logical types
  Struct { children: Vec<Field> },

  // Unions of logical types
  Union { mode: UnionMode, children: Vec<Box<Field>> },

  // Dictionary aka Category type
  Dictionary { index_type: Box<Ty>, dictionary: array::Array }
}

impl Ty {
  pub fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
    match self {
      &Ty::Null => vec![],
      &Ty::Bool => vec![K_VALIDITY_BUFFER, K_VALUES_1],
      &Ty::Uint8 => vec![K_VALIDITY_BUFFER, K_VALUES_8],
      &Ty::Int8 => vec![K_VALIDITY_BUFFER, K_VALUES_8],
      &Ty::Uint16 => vec![K_VALIDITY_BUFFER, K_VALUES_16],
      &Ty::Int16 => vec![K_VALIDITY_BUFFER, K_VALUES_16],
      &Ty::Uint32 => vec![K_VALIDITY_BUFFER, K_VALUES_32],
      &Ty::Int32 => vec![K_VALIDITY_BUFFER, K_VALUES_32],
      &Ty::Uint64 => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::Int64 => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::HalfFloat => vec![K_VALIDITY_BUFFER, K_VALUES_16],
      &Ty::Float => vec![K_VALIDITY_BUFFER, K_VALUES_32],
      &Ty::Double => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::String => vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8],
      &Ty::Binary => vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8],
      &Ty::Date => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::Date32 => vec![K_VALIDITY_BUFFER, K_VALUES_32],
      &Ty::Timestamp { unit: ref unit } => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::Time { unit: ref unit } => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::Interval { unit: ref unit } => vec![K_VALIDITY_BUFFER, K_VALUES_64],
      &Ty::Decimal { precision: precision, scale: scale } => vec![], // TODO
      &Ty::List { field: ref field } => vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER],
      &Ty::Struct { children: ref children } => vec![K_VALIDITY_BUFFER],
      &Ty::Union { mode: ref mode, children: ref children } => {
        match mode {
          &UnionMode::SPARSE => vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER],
          &UnionMode::DENSE => vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER, K_OFFSET_BUFFER],
          _ => panic!()
        }
      },
      &Ty::Dictionary { index_type: ref index_type, dictionary: ref dictionary } => vec![K_VALIDITY_BUFFER, Ty::get_buffer_desc(index_type.bit_width()) ],
      _ => panic!() // TODO: unknown type error
    }
  }

  fn get_buffer_desc(bit_width: i32) -> &'static BufferDesc {
    match bit_width {
      1 => K_VALUES_1,
      8 => K_VALUES_8,
      16 => K_VALUES_16,
      32 => K_VALUES_32,
      64 => K_VALUES_64,
      _ => panic!()
    }
  }

  pub fn get_name(&self) -> &'static str {
    match self {
      &Ty::Null => "null",
      &Ty::Bool => "bool",
      &Ty::Uint8 => "uint8",
      &Ty::Int8 => "int8",
      &Ty::Uint16 => "uint16",
      &Ty::Int16 => "int16",
      &Ty::Uint32 => "uint32",
      &Ty::Int32 => "int32",
      &Ty::Uint64 => "uint64",
      &Ty::Int64 => "int64",
      &Ty::HalfFloat => "halffloat",
      &Ty::Float => "float",
      &Ty::Double => "double",
      &Ty::String => "utf8",
      &Ty::Binary => "binary",
      &Ty::Date => "date",
      &Ty::Date32 => "date32",
      &Ty::Timestamp { unit: ref unit } => "timestamp",
      &Ty::Time { unit: ref unit } => "time",
      &Ty::Interval { unit: ref unit } => "interval",
      &Ty::Decimal { precision: precision, scale: scale } => "decimal",
      &Ty::List { field: ref field } => "list",
      &Ty::Struct { children: ref children } => "struct",
      &Ty::Union { mode: ref mode, children: ref children } => "union",
      &Ty::Dictionary { index_type: ref index_type, dictionary: ref dictionary } => "dictionary",
      _ => panic!()
    }
  }

  pub fn bit_width(&self) -> i32 {
    match self {
      &Ty::Bool => 1,
      &Ty::Uint8 => 8,
      &Ty::Int8 => 8,
      &Ty::Uint16 => 16,
      &Ty::Int16 => 16,
      &Ty::Uint32 => 32,
      &Ty::Int32 => 32,
      &Ty::Uint64 => 64,
      &Ty::Int64 => 64,
      &Ty::HalfFloat => 16,
      &Ty::Float => 32,
      &Ty::Double => 64,
      &Ty::Date => 64,
      &Ty::Date32 => 32,
      &Ty::Timestamp { unit: ref unit } => 64,
      &Ty::Time { unit: ref unit } => 64,
      &Ty::Interval { unit: ref unit } => 64,
      &Ty::Dictionary { index_type: ref index_type, dictionary: ref dictionary } => index_type.bit_width(),
      _ => panic!() // TODO: not supported error
    }
  }

  pub fn is_fixed_type(&self) -> bool {
    match self {
      &Ty::Bool => true,
      &Ty::Uint8 => true,
      &Ty::Int8 => true,
      &Ty::Uint16 => true,
      &Ty::Int16 => true,
      &Ty::Uint32 => true,
      &Ty::Int32 => true,
      &Ty::Uint64 => true,
      &Ty::Int64 => true,
      &Ty::HalfFloat => true,
      &Ty::Float => true,
      &Ty::Double => true,
      &Ty::Date => true,
      &Ty::Date32 => true,
      &Ty::Timestamp { unit: ref unit } => true,
      &Ty::Time { unit: ref unit } => true,
      &Ty::Interval { unit: ref unit } => true,
      &Ty::Dictionary { index_type: ref index_type, dictionary: ref dictionary } => true,
      _ => false // TODO: not supported error
    }
  }

  pub fn is_nested_type(&self) -> bool {
    match self {
      &Ty::Struct { children: ref children } => true,
      &Ty::Union { mode: ref mode, children: ref children } => true,
      _ => false // TODO: not supported error
    }
  }

  pub fn child(&self, i: usize) -> &Field {
    unimplemented!()
  }

  pub fn get_children(&self) -> &Vec<Field> {
    unimplemented!()
  }

  pub fn num_children(&self) -> i32 {
    unimplemented!()
  }

  pub fn is_signed(&self) -> bool {
    unimplemented!()
  }

  pub fn precision(&self) -> Precision {
    unimplemented!()
  }

  pub fn value_field_from_list_type(&self) -> &Field {
    match self {
      &Ty::List { field: ref field } => field,
      _ => panic!()
    }
  }

  pub fn value_type_from_list_type(&self) -> &Ty {
    match self {
      &Ty::List { field: ref field } => field.get_type(),
      _ => panic!()
    }
  }
}

impl ToString for Ty {
  fn to_string(&self) -> String {
    String::from(self.get_name())
  }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TimeUnit {
  SECOND,
  MILLI,
  MICRO,
  NANO
}

#[derive(Debug, Eq, PartialEq)]
pub enum BufferType {
  DATA,
  OFFSET,
  TYPE,
  VALIDITY
}

#[derive(Debug, Eq, PartialEq)]
pub struct BufferDesc {
  ty: BufferType,
  bit_width: i32
}

// TODO: pub?
pub static K_VALIDITY_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::VALIDITY,
  bit_width: 1
};

pub static K_OFFSET_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::OFFSET,
  bit_width: 32
};

pub static K_TYPE_BUFFER: &'static BufferDesc = &BufferDesc {
  ty: BufferType::TYPE,
  bit_width: 32
};

pub static K_VALUES_1: &'static BufferDesc = &BufferDesc {
  ty: BufferType::DATA,
  bit_width: 1
};

macro_rules! define_buffer_desc {
  ($name: ident, $width: expr) => (
    pub static $name: &'static BufferDesc = &BufferDesc{
      ty: BufferType::DATA,
      bit_width: $width
    };
  );
}

define_buffer_desc!(K_VALUES_8, 8);
define_buffer_desc!(K_VALUES_16, 16);
define_buffer_desc!(K_VALUES_32, 32);
define_buffer_desc!(K_VALUES_64, 64);

//pub trait VisitType {
//  // TODO
//}

//pub trait AcceptVisitor {
//  fn accept(&self, visit_type: &VisitType);
//}

// Required to implement this trait for every data types
pub trait Typed {
  fn get_type(&self) -> Ty;
  fn get_buffer_layout(&self) -> Vec<&BufferDesc>;
  fn get_name(&self) -> &'static str;
}

// Required to implement this trait for structured data types
pub trait NestedTyped {
  fn child(&self, i: usize) -> &Field;
  fn get_children(&self) -> &Vec<Field>;
  fn num_children(&self) -> i32;
}

// Required to implement this trait for fixed-size data types
pub trait FixedWidth {
  fn get_bit_width(&self) -> i32;
}

// Required to implement this trait for integer data types
pub trait IntegerTyped {
  fn is_signed(&self) -> bool;
}

#[derive(Debug, Eq, PartialEq)]
pub enum Precision {
  HALF,
  SINGLE,
  DOUBLE
}

pub trait FloatTyped {
  fn precision(&self) -> Precision;
}

#[derive(Debug, Eq, PartialEq)]
pub struct Field {
  name: String,
  ty: Box<Ty>,
  nullable: bool,
  // optional dictionary id if the field is dictionary encoded
  // 0 means it's not dictionary encoded
  dictionary: i64
}

impl Field {
  pub fn basic(name: &'static str, ty: Ty) -> Field {
    Field::new(name, ty, true, 0)
  }

  pub fn non_null(name: &'static str, ty: Ty) -> Field {
    Field::new(name, ty, false, 0)
  }

  pub fn with_dic(name: &'static str, ty: Ty, dictionary: i64) -> Field {
    Field::new(name, ty, true, dictionary)
  }

  pub fn non_null_with_dic(name: &'static str, ty: Ty, dictionary: i64) -> Field {
    Field::new(name, ty, false, dictionary)
  }

  fn new(name: &'static str, ty: Ty, nullable: bool, dictionary: i64) -> Field {
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

  pub fn get_type(&self) -> &Ty {
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
    let str = self.name.clone() + ": " + self.ty.get_name();
    if self.nullable {
      str + " not null"
    } else {
      str
    }
  }
}

// TODO: singleton?
//#[derive(Debug, Eq, PartialEq)]
//pub struct NullType {}
//
//impl NullType {
//  pub fn new() -> NullType {
//    NullType {}
//  }
//}
//
//impl Typed for NullType {
//  fn get_type(&self) -> RawType {
//    RawType::NULL
//  }
//
//  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
//    vec![]
//  }
//
//  fn get_name(&self) -> &'static str {
//    "null"
//  }
//}
//
//#[derive(Debug, Eq, PartialEq)]
//pub struct BooleanType {}
//
//impl BooleanType {
//  pub fn new() -> BooleanType {
//    BooleanType {}
//  }
//}
//
//impl Typed for BooleanType {
//  fn get_type(&self) -> RawType {
//    RawType::BOOL
//  }
//
//  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
//    vec![K_VALIDITY_BUFFER, K_VALUES_1]
//  }
//
//  fn get_name(&self) -> &'static str {
//    "bool"
//  }
//}
//
//impl FixedWidth for BooleanType {
//  fn get_bit_width(&self) -> i32 {
//    1
//  }
//}
//
////impl AcceptVisitor for NullType {
////  fn accept(&self, visit_type: &VisitType) {
////    // TODO
////  }
////}
//
//macro_rules! define_primitive_type {
//  ($type_name: ident, $str_name: expr, $raw_type: expr, $rust_type: ty, $buffer_desc: expr) => {
//    #[derive(Debug, Eq, PartialEq)]
//    pub struct $type_name {}
//
//    impl $type_name {
//      pub fn new() -> $type_name {
//        $type_name {}
//      }
//    }
//
//    impl Typed for $type_name {
//      fn get_type(&self) -> RawType {
//        $raw_type
//      }
//
//      fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
//        vec![K_VALIDITY_BUFFER, $buffer_desc] // TODO?
//      }
//
//      fn get_name(&self) -> &'static str {
//        $str_name
//      }
//    }
//
//    impl FixedWidth for $type_name {
//      fn get_bit_width(&self) -> i32 {
//        mem::size_of::<$rust_type>() as i32
//      }
//    }
//  }
//}
//
//macro_rules! impl_int_type {
//  ($type_name: ident, $is_signed: expr) => (
//    impl IntegerTyped for $type_name {
//      fn is_signed(&self) -> bool {
//        $is_signed
//      }
//    }
//  );
//}
//
//macro_rules! impl_float_type {
//  ($type_name: ident, $precision: expr) => (
//    impl FloatTyped for $type_name {
//      fn precision(&self) -> Precision {
//        $precision
//      }
//    }
//  );
//}
//
////macro_rules! impl_nested_type {
////  ($type_name: ident) => (
////    impl<T: Typed> NestedTyped<T> for $type_name<T> {
////      #[inline]
////      fn child(&self, i: usize) -> &Field<T> {
////        &self.children[i]
////      }
////
////      #[inline]
////      fn get_children(&self) -> &Vec<Field<T>> {
////        &self.children
////      }
////
////      #[inline]
////      fn num_children(&self) -> i32 {
////        self.children.len() as i32
////      }
////    }
////
////    impl<T: Typed> Index<usize> for $type_name<T> {
////      type Output = Field<T>;
////
////      #[inline]
////      fn index(&self, index: usize) -> &Field<T> {
////        &self.children[index]
////      }
////    }
////  );
////}
//
//define_primitive_type!(UInt8Type, "uint8", RawType::UINT8, u8, K_VALUES_8);
//define_primitive_type!(UInt16Type, "uint16", RawType::UINT16, u16, K_VALUES_16);
//define_primitive_type!(UInt32Type, "uint32", RawType::UINT32, u32, K_VALUES_32);
//define_primitive_type!(UInt64Type, "uint64", RawType::UINT64, u64, K_VALUES_64);
//define_primitive_type!(Int8Type, "int8", RawType::INT8, i8, K_VALUES_8);
//define_primitive_type!(Int16Type, "int16", RawType::INT16, i16, K_VALUES_16);
//define_primitive_type!(Int32Type, "int32", RawType::INT32, i32, K_VALUES_32);
//define_primitive_type!(Int64Type, "int64", RawType::INT64, i64, K_VALUES_64);
//impl_int_type!(UInt8Type, false);
//impl_int_type!(UInt16Type, false);
//impl_int_type!(UInt32Type, false);
//impl_int_type!(UInt64Type, false);
//impl_int_type!(Int8Type, true);
//impl_int_type!(Int16Type, true);
//impl_int_type!(Int32Type, true);
//impl_int_type!(Int64Type, true);
//
//define_primitive_type!(HalfFloatType, "halffloat", RawType::HALF_FLOAT, u16, K_VALUES_16);
//define_primitive_type!(FloatType, "float", RawType::FLOAT, f32, K_VALUES_32);
//define_primitive_type!(DoubleType, "double", RawType::DOUBLE, f64, K_VALUES_64);
//impl_float_type!(HalfFloatType, Precision::HALF);
//impl_float_type!(FloatType, Precision::SINGLE);
//impl_float_type!(DoubleType, Precision::DOUBLE);
//
////impl AcceptVisitor for UInt8Type {
////  fn accept(&self, visit_type: &VisitType) {
////    unimplemented!()
////  }
////}
//
////#[derive(Debug, Eq, PartialEq)]
////pub struct ListType<T: Typed> {
////  children: Vec<Field<T>>
////}
////
////impl<T: Typed> ListType<T> {
////  pub fn with_value_type(value_type: T) -> ListType<T> {
////    ListType::with_field(Field::basic("item", value_type))
////  }
////
////  pub fn with_field(field: Field<T>) -> ListType<T> {
////    ListType {
////      children: vec![field]
////    }
////  }
////
////  pub fn value_field(&self) -> &Field<T> {
////    &self.children[0]
////  }
////
////  pub fn value_type(&self) -> &T {
////    self.children[0].get_type()
////  }
////}
////
////impl<T: Typed> Typed for ListType<T> {
////  fn get_type(&self) -> RawType {
////    RawType::LIST
////  }
////
////  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
////    vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER]
////  }
////
////  fn get_name(&self) -> &'static str {
////    "list"
////  }
////}
////
////impl_nested_type!(ListType);
////
////#[derive(Debug, Eq, PartialEq)]
////pub struct BinaryType {}
////
////impl BinaryType {
////  pub fn new() -> BinaryType {
////    BinaryType {}
////  }
////}
////
////impl Typed for BinaryType {
////  fn get_type(&self) -> RawType {
////    RawType::BINARY
////  }
////
////  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
////    vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8]
////  }
////
////  fn get_name(&self) -> &'static str {
////    "binary"
////  }
////}
////
////#[derive(Debug, Eq, PartialEq)]
////pub struct StringType {}
////
////impl StringType {
////  pub fn new() -> StringType {
////    StringType {}
////  }
////}
////
////impl Typed for StringType {
////  fn get_type(&self) -> RawType {
////    RawType::STRING
////  }
////
////  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
////    vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8]
////  }
////
////  fn get_name(&self) -> &'static str {
////    "utf8"
////  }
////}
////
////#[derive(Debug, Eq, PartialEq)]
////pub struct StructType<T: Typed> {
////  children: Vec<Field<T>>
////}
////
////impl<T: Typed> StructType<T> {
////  pub fn new(fields: Vec<Field<T>>) -> StructType<T> {
////    StructType {
////      children: fields
////    }
////  }
////}
////
////impl<T:Typed> Typed for StructType<T> {
////  fn get_type(&self) -> RawType {
////    RawType::STRUCT
////  }
////
////  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
////    vec![K_VALIDITY_BUFFER]
////  }
////
////  fn get_name(&self) -> &'static str {
////    "struct"
////  }
////}
////
////impl_nested_type!(StructType);
////
////#[derive(Debug, Eq, PartialEq)]
////pub struct DecimalType {
////  precision: i32,
////  scale: i32
////}
////
////impl DecimalType {
////  pub fn new(precision: i32, scale: i32) -> DecimalType {
////    DecimalType {
////      precision: precision,
////      scale: scale
////    }
////  }
////}
////
////impl Typed for DecimalType {
////  fn get_type(&self) -> RawType {
////    RawType::DECIMAL
////  }
////
////  fn get_buffer_layout(&self) -> Vec<&BufferDesc> {
////    // TODO
////    vec![]
////  }
////
////  fn get_name(&self) -> &'static str {
////    unimplemented!()
////  }
////}

#[derive(Debug, Eq, PartialEq)]
pub enum UnionMode {
  SPARSE,
  DENSE
}

////#[derive(Debug, Eq, PartialEq)]
////pub struct UnionType<T: Typed> {
////  children: Vec<Field<T>>,
////  type_codes: Vec<u8>,
////  mode: UnionMode,
////}
////
////impl<T:Typed> UnionType<T> {
////  pub fn new_sparse_type(fields: Vec<Field<T>>, type_codes: Vec<u8>) -> UnionType<T> {
////    UnionType {
////      children: fields,
////      type_codes: type_codes,
////      mode: UnionMode::SPARSE
////    }
////  }
////
////  pub fn new_dense_type(fields: Vec<Field<T>>, type_codes: Vec<u8>) -> UnionType<T> {
////    UnionType {
////      children: fields,
////      type_codes: type_codes,
////      mode: UnionMode::DENSE
////    }
////  }
////}
////
////impl_nested_type!(UnionType);
//
//// DateType
//
//// TimeType
//
//// TimestampType
//
//// IntervalType
//
//// DictionaryType