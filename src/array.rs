use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::{Ty, DataType};
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};

use std::ptr;
use std::mem;

use std::fmt::{Debug, Formatter, Error};

#[derive(Eq, PartialEq)]
pub struct ArrayData {
  ty: DataType,
  length: i64,
  null_count: i64,
  offset: i64,
  null_bitmap: Option<PoolBuffer>,
  values: Option<PoolBuffer>,
  children: Vec<ArrayData>
}

impl ArrayData {
  pub fn new(ty: DataType, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: PoolBuffer) -> ArrayData {
    ArrayData::create(ty, length, ArrayData::compute_null_count(&null_bitmap, offset, length), offset, null_bitmap, Some(values), Vec::new())
  }

  pub fn null(length: i64, offset: i64) -> ArrayData {
    unsafe { ArrayData::create(DataType::null(), length, length, offset, None, None, Vec::new()) }
  }

  fn create(ty: DataType, length: i64, null_count: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: Option<PoolBuffer>, children: Vec<ArrayData>) -> ArrayData {
    ArrayData {
      ty,
      length,
      null_count,
      offset,
      null_bitmap,
      values,
      children
    }
  }

  #[inline]
  pub fn data_type(&self) -> &DataType {
    &self.ty
  }

  #[inline]
  pub fn len(&self) -> i64 {
    self.length
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
    self.null_count
  }

  #[inline]
  pub fn offset(&self) -> i64 {
    self.offset
  }

  #[inline]
  pub fn null_bitmap_buffer(&self) -> &Option<PoolBuffer> {
    &self.null_bitmap
  }

  #[inline]
  pub fn values_buffer(&self) -> &Option<PoolBuffer> {
    &self.values
  }

//  #[inline]
//  pub fn raw_null_bitmap(&self) -> *const u8 {
//    self.null_bitmap.data()
//  }

//  #[inline]
//  pub fn raw_values(&self) -> *const u8 {
//    self.values.data()
//  }

  #[inline]
  pub fn children(&self) -> &Vec<ArrayData> {
    &self.children
  }

  #[inline]
  pub fn child(&self, i: usize) -> &ArrayData {
    &self.children[i]
  }

  #[inline]
  pub fn num_children(&self) -> i32 {
    self.children.len() as i32
  }

  #[inline]
  fn compute_null_count(null_bitmap: &Option<PoolBuffer>, offset: i64, length: i64) -> i64 {
    match null_bitmap {
      &Some(ref buffer) => {
        let null_bitmap_data = buffer.data();
        if !null_bitmap_data.is_null() {
          length - bit_util::count_set_bits(null_bitmap_data, offset, length)
        } else {
          0
        }
      },
      &None => 0
    }

  }
}

#[derive(Eq, PartialEq)]
pub struct Array {
  data: ArrayData,
  meta: ArrayMeta
}

#[derive(Eq, PartialEq)]
pub enum ArrayMeta {
  Null,
  Bool,

  UInt8,
  Int8,
  UInt16,
  Int16,
  UInt32,
  Int32,
  UInt64,
  Int64,

  HalfFloat,
  Float,
  Double,

  Binary {
    value_offsets: *const i32
  },
  String {
    value_offsets: *const i32
  },
  FixedSizedBinary,

  Date64,
  Date32,
  Timestamp,
  Time32,
  Time64,
  Interval,

  Decimal,

  List,
  Struct,
  Union,

  Dictionary
}

impl Array {
  pub fn primitive(ty: DataType, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: PoolBuffer) -> Array {
    Array::fixed_width(ArrayData::new(ty, length, offset, null_bitmap, values))
  }

  pub fn null(length: i64, offset: i64) -> Array {
    Array::fixed_width(ArrayData::null(length, offset))
  }

  fn fixed_width(data: ArrayData) -> Array {
    let meta = match data.data_type().ty() {
      Ty::NA => ArrayMeta::Null,
      Ty::Bool => ArrayMeta::Bool,

      Ty::Int8 => ArrayMeta::Int8,
      Ty::Int16 => ArrayMeta::Int16,
      Ty::Int32 => ArrayMeta::Int32,
      Ty::Int64 => ArrayMeta::Int64,
      Ty::UInt8 => ArrayMeta::UInt8,
      Ty::UInt16 => ArrayMeta::UInt16,
      Ty::UInt32 => ArrayMeta::UInt32,
      Ty::UInt64 => ArrayMeta::UInt64,

      Ty::HalfFloat => ArrayMeta::HalfFloat,
      Ty::Float => ArrayMeta::Float,
      Ty::Double => ArrayMeta::Double,

      _ => panic!("[{:?}] is not supported type", data.data_type().ty())
    };

    Array {
      data,
      meta,
    }
  }

  pub fn variable_width(ty: DataType, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: PoolBuffer, value_offsets: PoolBuffer) -> Array {
    let data = ArrayData::new(ty, length, offset, null_bitmap, values);
    let meta = match data.data_type().ty() {
      Ty::Binary => ArrayMeta::Binary {
        value_offsets: unsafe { mem::transmute::<*const u8, *const i32>(value_offsets.data()) }
      },
      _ => panic!()
    };

    Array {
      data,
      meta
    }
  }

  pub fn is_null(&self, i: i64) -> bool {
    match self.ty() {
      Ty::NA => true,
      _ => match self.null_bitmap_buffer() {
        &Some(ref bitmap) => bit_util::bit_not_set(bitmap.data(), i + self.offset()),
        &None => false
      }
    }
  }

  pub fn is_valid(&self, i: i64) -> bool {
    match self.ty() {
      Ty::NA => false,
      _ => match self.null_bitmap_buffer() {
        &Some(ref bitmap) => bit_util::get_bit(bitmap.data(), i + self.offset()),
        &None => true
      }
    }
  }

  #[inline]
  pub fn len(&self) -> i64 {
    self.data.len()
  }

  #[inline]
  pub fn offset(&self) -> i64 {
    self.data.offset()
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
    self.data.null_count()
  }

  #[inline]
  pub fn data_type(&self) -> &DataType {
    self.data.data_type()
  }

  #[inline]
  pub fn ty(&self) -> Ty {
    self.data.data_type().ty()
  }

  #[inline]
  pub fn null_bitmap_buffer(&self) -> &Option<PoolBuffer> {
    self.data.null_bitmap_buffer()
  }

  #[inline]
  pub fn value_buffer(&self) -> &Option<PoolBuffer> {
    self.data.values_buffer()
  }

  #[inline]
  pub fn num_fields(&self) -> i32 {
    self.data.num_children()
  }

  #[inline]
  pub fn data(&self) -> &ArrayData {
    &self.data
  }
}

impl Debug for Box<Array> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl Clone for Box<Array> {
  fn clone(&self) -> Self {
    unimplemented!()
  }
}

// TODO: maybe need cast?
pub trait FixedWidthArray<T> {
  fn value(&self, i: i64) -> T;

  fn values(&self) -> *const T;
}

impl FixedWidthArray<bool> for Array {
  fn value(&self, i: i64) -> bool {
    match self.ty() {
      Ty::Bool => match self.value_buffer() {
        &Some(ref buffer) => bit_util::get_bit(buffer.data(), i + self.data.offset()),
        &None => panic!("value buffer doesn't exist")
      },
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }

  fn values(&self) -> *const bool {
    match self.ty() {
      Ty::Bool => match self.value_buffer() {
        &Some(ref buffer) => {
          unsafe {
            use std::ptr;
            mem::transmute::<*const u8, *const bool>(buffer.data().offset(self.data.offset() as isize))
          }
        },
        &None => panic!("value buffer doesn't exist")
      },
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }
}

impl FixedWidthArray<u16> for Array {
  fn value(&self, i: i64) -> u16 {
    unsafe { *self.values().offset(i as isize) }
  }

  fn values(&self) -> *const u16 {
    match self.ty() {
      Ty::UInt16 | Ty::HalfFloat => match self.value_buffer() {
        &Some(ref buffer) => {
          unsafe {
            use std::ptr;
            mem::transmute::<*const u8, *const u16>(buffer.data().offset(self.data.offset() as isize))
          }
        },
        &None => panic!("value buffer doesn't exist")
      },
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }
}

macro_rules! impl_fixed_width_array {
    ($ty: path, $prim_ty: ident) => {
      impl FixedWidthArray<$prim_ty> for Array {
        fn value(&self, i: i64) -> $prim_ty {
          unsafe { *self.values().offset(i as isize) }
        }

        fn values(&self) -> *const $prim_ty {
          match self.ty() {
            $ty => match self.value_buffer() {
              &Some(ref buffer) => {
                unsafe {
                  use std::ptr;
                  mem::transmute::<*const u8, *const $prim_ty>(buffer.data().offset(self.data.offset() as isize))
                }
              },
              &None => panic!("value buffer doesn't exist")
            },
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
      }
    };
}

impl_fixed_width_array!(Ty::Int8, i8);
impl_fixed_width_array!(Ty::Int16, i16);
impl_fixed_width_array!(Ty::Int32, i32);
impl_fixed_width_array!(Ty::Int64, i64);
impl_fixed_width_array!(Ty::UInt8, u8);
impl_fixed_width_array!(Ty::UInt32, u32);
impl_fixed_width_array!(Ty::UInt64, u64);

impl_fixed_width_array!(Ty::Float, f32);
impl_fixed_width_array!(Ty::Double, f64);

pub struct VariableWidthElem {
  p: *const u8,
  len: i32
}

pub trait VariableWidthArray {
  fn value(&self, i: i64) -> VariableWidthElem;

  fn value_offset(&self, i: i64) -> i32;

  fn value_len(&self, i: i64) -> i32;
}

impl VariableWidthArray for Array {
  fn value(&self, i: i64) -> VariableWidthElem {
    match self.meta {
      ArrayMeta::Binary { ref value_offsets } | ArrayMeta::String { ref value_offsets } => {
        let offset = i + self.data.offset();
        unsafe {
          let pos = *value_offsets.offset(i as isize);
          let value_len = *value_offsets.offset((i + 1) as isize) - pos;
          match self.data.values_buffer() {
            &Some(ref buffer) => VariableWidthElem {
              p: buffer.data().offset(pos as isize),
              len: value_len
            },
            &None => panic!()
          }
        }
      },
      _ => panic!()
    }
  }

  fn value_offset(&self, i: i64) -> i32 {
    match self.meta {
      ArrayMeta::Binary { ref value_offsets } | ArrayMeta::String { ref value_offsets } => {
        let offset = i + self.data.offset();
        unsafe { *value_offsets.offset(i as isize) }
      },
      _ => panic!()
    }
  }

  fn value_len(&self, i: i64) -> i32 {
    match self.meta {
      ArrayMeta::Binary { ref value_offsets } | ArrayMeta::String { ref value_offsets } => {
        let offset = i + self.data.offset();
        unsafe {
          let pos = *value_offsets.offset(i as isize);
          *value_offsets.offset((i + 1) as isize) - pos
        }
      },
      _ => panic!()
    }
  }
}

pub trait StringArray {
  fn string(&self, i: i64) -> String;
}

impl <T> StringArray for T where T: VariableWidthArray {
  fn string(&self, i: i64) -> String {
    let elem = self.value(i);
    unsafe { String::from_raw_parts(mem::transmute::<*const u8, *mut u8>(elem.p), elem.len as usize, elem.len as usize) }
  }
}


//pub trait BaseArray {
//  fn is_null(&self, i: i64) -> bool {
//    !self.null_bitmap_data().is_null() &&
//      bit_util::bit_not_set(self.null_bitmap_data(), i + self.offset())
//  }
//
//  fn is_valid(&self, i: i64) -> bool {
//    !self.null_bitmap_data().is_null() &&
//      bit_util::get_bit(self.null_bitmap_data(), i + self.offset())
//  }
//
//  fn len(&self) -> i64 {
//    self.data().len()
//  }
//
//  fn offset(&self) -> i64 {
//    self.data().offset()
//  }
//
//  fn null_count(&self) -> i64 {
//    self.data().null_count()
//  }
//
//  fn data_type(&self) -> &Box<DataType> {
//    self.data().data_type()
//  }
//
//  fn ty(&self) -> Ty {
//    self.data_type().ty()
//  }
//
//  fn null_bitmap(&self) -> &Box<PoolBuffer> {
//    self.data().null_bitmap()
//  }
//
//  fn null_bitmap_data(&self) -> *const u8 {
//    self.data().raw_null_bitmap()
//  }
//
//  fn values(&self) -> &Box<PoolBuffer> {
//    self.data().values()
//  }
//
//  fn num_fields(&self) -> i32 {
//    self.data().num_children()
//  }
//
//  fn data(&self) -> &ArrayData;
//}

pub trait Cast {
//  fn as_null(&self) -> &NullArray {
//    unimplemented!("Cannot cast to null")
//  }

  fn as_bool(&self) -> &FixedWidthArray<bool> {
    unimplemented!("Cannot cast to boolean")
  }

  fn as_int8(&self) -> &FixedWidthArray<i8> {
    unimplemented!("Cannot cast to int8")
  }

//  fn as_int16(&self) -> &Int16Array {
//    unimplemented!("Cannot cast to int16")
//  }
//
//  fn as_int32(&self) -> &Int32Array {
//    unimplemented!("Cannot cast to int32")
//  }
//
//  fn as_int64(&self) -> &Int64Array {
//    unimplemented!("Cannot cast to int64")
//  }
//
//  fn as_uint8(&self) -> &UInt8Array {
//    unimplemented!("Cannot cast to uint8")
//  }
//
//  fn as_uint16(&self) -> &UInt16Array {
//    unimplemented!("Cannot cast to uint16")
//  }
//
//  fn as_uint32(&self) -> &UInt32Array {
//    unimplemented!("Cannot cast to uint32")
//  }
//
//  fn as_uint64(&self) -> &UInt64Array {
//    unimplemented!("Cannot cast to uint64")
//  }
//
//  fn as_float(&self) -> &FloatArray {
//    unimplemented!("Cannot cast to float")
//  }
//
//  fn as_double(&self) -> &DoubleArray {
//    unimplemented!("Cannot cast to double")
//  }
//
//  fn as_halffloat(&self) -> &HalfFloatArray {
//    unimplemented!("Cannot cast to halffloat")
//  }
}

//pub trait Array : BaseArray + Cast {}
//
//pub fn array_eq(a1: &Array, a2: &Array) -> bool {
//  unimplemented!()
//}
//
//impl PartialEq for Box<Array> {
//  fn eq(&self, other: &Self) -> bool {
//    unimplemented!()
//  }
//}
//
//impl Eq for Box<Array> {
//
//}
//
//impl Debug for Box<Array> {
//  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//    unimplemented!()
//  }
//}
//
//impl Clone for Box<Array> {
//  fn clone(&self) -> Self {
//    unimplemented!()
//  }
//}
//
//
//#[derive(PartialEq, Eq)]
//pub struct NullArray {
//  data: ArrayData
//}
//
//impl NullArray {
//  pub fn new(len: i64) -> Self {
//    NullArray {
//      data: ArrayData::null_data(Box::new(ty::NullType::new()), len, 0)
//    }
//  }
//
//  pub fn from_data(data: ArrayData) -> Self {
//    NullArray {
//      data
//    }
//  }
//}
//
//impl BaseArray for NullArray {
//  fn is_null(&self, i: i64) -> bool {
//    true
//  }
//
//  fn data(&self) -> &ArrayData {
//    &self.data
//  }
//}
//
//impl Cast for NullArray {
//  fn as_null(&self) -> &NullArray {
//    &self
//  }
//}
//
//trait NumericArray<T> : Array {
//  fn value(&self, i: i64) -> T;
//}
//
//#[derive(PartialEq, Eq)]
//pub struct BooleanArray {
//  data: ArrayData,
//  raw_values: *const u8
//}
//
//impl BooleanArray {
//  pub fn from_data(data: ArrayData) -> Self {
//    let raw_values = data.raw_values();
//    BooleanArray {
//      data,
//      raw_values
//    }
//  }
//
//  pub fn raw_values(&self) -> *const u8 {
//    self.raw_values
//  }
//
//  pub fn value(&self, i: i64) -> bool {
//    bit_util::get_bit(self.raw_values, i + self.data.offset())
//  }
//}
//
//impl BaseArray for BooleanArray {
//  fn data(&self) -> &ArrayData {
//    &self.data
//  }
//}
//
//impl Cast for BooleanArray {
//  fn as_bool(&self) -> &BooleanArray {
//    &self
//  }
//}
//
//macro_rules! define_numeric_array {
//    ($name: ident, $cast_fn: ident) => {
//      pub struct $name {
//        data: ArrayData,
//        raw_values: *const u8
//      }
//
//      impl $name {
//        pub fn from_data(data: ArrayData) -> Self {
//          let raw_values = data.raw_values();
//          $name {
//            data,
//            raw_values
//          }
//        }
//
//        pub fn raw_values(&self) -> *const u8 {
//          self.raw_values
//        }
//
//        pub fn value(&self, i: i64) -> bool {
//          bit_util::get_bit(self.raw_values, i + self.data.offset())
//        }
//      }
//
//      impl BaseArray for $name {
//        fn data(&self) -> &ArrayData {
//          &self.data
//        }
//      }
//
//      impl Cast for $name {
//        fn $cast_fn(&self) -> &$name {
//          &self
//        }
//      }
//    };
//}
//
//define_numeric_array!(Int8Array, as_int8);
//define_numeric_array!(Int16Array, as_int16);
//define_numeric_array!(Int32Array, as_int32);
//define_numeric_array!(Int64Array, as_int64);
//define_numeric_array!(UInt8Array, as_uint8);
//define_numeric_array!(UInt16Array, as_uint16);
//define_numeric_array!(UInt32Array, as_uint32);
//define_numeric_array!(UInt64Array, as_uint64);
//define_numeric_array!(FloatArray, as_float);
//define_numeric_array!(DoubleArray, as_double);
//define_numeric_array!(HalfFloatArray, as_halffloat);




//#[derive(Debug, Eq, PartialEq)]
//pub enum ArrayType {
//
//}
//
//macro_rules! define_base_array {
//    ($name: ident) => {
//      #[derive(Eq, PartialEq)]
//      pub struct $name<'a> {
//        data: ArrayData<'a>,
//        null_bitmap_data: *const u8
//      }
//
//      impl<'a> $name<'a> {
//        pub fn is_null(&self, i: i64) -> bool {
//          self.null_bitmap_data.is_null() || bit_util::bit_not_set(self.null_bitmap_data, i + self.data.offset())
//        }
//
//        pub fn len(&self) -> i64 {
//          self.data.len()
//        }
//
//        pub fn offset(&self) -> i64 {
//          self.data.offset()
//        }
//
//        pub fn data(&self) -> &ArrayData {
//          &self.data
//        }
//
//        pub fn null_bitmap_data(&self) -> &*const u8 {
//          &self.null_bitmap_data
//        }
//      }
//    };
//}
//
//define_base_array!(NullArray);
//
//impl <'a> NullArray<'a> {
//  pub fn with_data(array_data: ArrayData<'a>) -> NullArray<'a> {
//    let null_data = ArrayData {
//      ty: array_data.ty,
//      length: array_data.length,
//      null_count: array_data.length,
//      offset: 0,
//      buffers: array_data.buffers,
//      children: array_data.children
//    };
//    NullArray {
//      data: null_data,
//      null_bitmap_data: ptr::null()
//    }
//  }
//}