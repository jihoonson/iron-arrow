use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::{Ty, DataType};
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};

use std::ptr;
use std::mem;

use std::fmt::{Debug, Formatter, Error};

pub struct ArrayData {
  ty: Box<DataType>, // TODO: box? or ref?
  length: i64,
  null_count: i64,
  offset: i64,
  null_bitmap: Box<PoolBuffer>,
  values: Box<PoolBuffer>,
  children: Vec<ArrayData> // TODO: box? or mut ref?
}

impl ArrayData {
  pub fn new(ty: Box<DataType>, length: i64, offset: i64, null_bitmap: Box<PoolBuffer>, values: Box<PoolBuffer>) -> ArrayData {
    ArrayData::create(ty, length, ArrayData::compute_null_count(&null_bitmap, offset, length), offset, null_bitmap, values, Vec::new())
  }

  pub fn null_data(ty: Box<DataType>, length: i64, offset: i64) -> ArrayData {
    unsafe { ArrayData::create(ty, length, length, offset, Box::from_raw(mem::uninitialized()), Box::from_raw(mem::uninitialized()), Vec::new()) }
  }

  fn create(ty: Box<DataType>, length: i64, null_count: i64, offset: i64, null_bitmap: Box<PoolBuffer>, values: Box<PoolBuffer>, children: Vec<ArrayData>) -> ArrayData {
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
  pub fn data_type(&self) -> &Box<DataType> {
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
  pub fn null_bitmap(&self) -> &Box<PoolBuffer> {
    &self.null_bitmap
  }

  #[inline]
  pub fn values(&self) -> &Box<PoolBuffer> {
    &self.values
  }

  #[inline]
  pub fn raw_null_bitmap(&self) -> *const u8 {
    self.null_bitmap.data()
  }

  #[inline]
  pub fn raw_values(&self) -> *const u8 {
    self.values.data()
  }

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
  fn compute_null_count(null_bitmap: &Box<PoolBuffer>, offset: i64, length: i64) -> i64 {
    let null_bitmap_data = null_bitmap.data();
    if !null_bitmap_data.is_null() {
      length - bit_util::count_set_bits(null_bitmap_data, offset, length)
    } else {
      0
    }
  }
}

impl PartialEq for ArrayData {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for ArrayData {

}

pub trait BaseArray {
  fn is_null(&self, i: i64) -> bool {
    !self.null_bitmap_data().is_null() &&
      bit_util::bit_not_set(self.null_bitmap_data(), i + self.offset())
  }

  fn is_valid(&self, i: i64) -> bool {
    !self.null_bitmap_data().is_null() &&
      bit_util::get_bit(self.null_bitmap_data(), i + self.offset())
  }

  fn len(&self) -> i64 {
    self.data().len()
  }

  fn offset(&self) -> i64 {
    self.data().offset()
  }

  fn null_count(&self) -> i64 {
    self.data().null_count()
  }

  fn data_type(&self) -> &Box<DataType> {
    self.data().data_type()
  }

  fn ty(&self) -> Ty {
    self.data_type().ty()
  }

  fn null_bitmap(&self) -> &Box<PoolBuffer> {
    self.data().null_bitmap()
  }

  fn null_bitmap_data(&self) -> *const u8 {
    self.data().raw_null_bitmap()
  }

  fn values(&self) -> &Box<PoolBuffer> {
    self.data().values()
  }

  fn num_fields(&self) -> i32 {
    self.data().num_children()
  }

  fn data(&self) -> &ArrayData;
}

pub trait Cast {
  fn as_null(&self) -> &NullArray {
    unimplemented!("Cannot cast to null")
  }

  fn as_bool(&self) -> &BooleanArray {
    unimplemented!("Cannot cast to boolean")
  }

  fn as_int8(&self) -> &Int8Array {
    unimplemented!("Cannot cast to int8")
  }

  fn as_int16(&self) -> &Int16Array {
    unimplemented!("Cannot cast to int16")
  }

  fn as_int32(&self) -> &Int32Array {
    unimplemented!("Cannot cast to int32")
  }

  fn as_int64(&self) -> &Int64Array {
    unimplemented!("Cannot cast to int64")
  }

  fn as_uint8(&self) -> &UInt8Array {
    unimplemented!("Cannot cast to uint8")
  }

  fn as_uint16(&self) -> &UInt16Array {
    unimplemented!("Cannot cast to uint16")
  }

  fn as_uint32(&self) -> &UInt32Array {
    unimplemented!("Cannot cast to uint32")
  }

  fn as_uint64(&self) -> &UInt64Array {
    unimplemented!("Cannot cast to uint64")
  }

  fn as_float(&self) -> &FloatArray {
    unimplemented!("Cannot cast to float")
  }

  fn as_double(&self) -> &DoubleArray {
    unimplemented!("Cannot cast to double")
  }

  fn as_halffloat(&self) -> &HalfFloatArray {
    unimplemented!("Cannot cast to halffloat")
  }
}

pub trait Array : BaseArray + Cast {}

pub fn array_eq(a1: &Array, a2: &Array) -> bool {
  unimplemented!()
}

impl PartialEq for Box<Array> {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for Box<Array> {

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


#[derive(PartialEq, Eq)]
pub struct NullArray {
  data: ArrayData
}

impl NullArray {
  pub fn new(len: i64) -> Self {
    NullArray {
      data: ArrayData::null_data(Box::new(ty::NullType::new()), len, 0)
    }
  }

  pub fn from_data(data: ArrayData) -> Self {
    NullArray {
      data
    }
  }
}

impl BaseArray for NullArray {
  fn is_null(&self, i: i64) -> bool {
    true
  }

  fn data(&self) -> &ArrayData {
    &self.data
  }
}

impl Cast for NullArray {
  fn as_null(&self) -> &NullArray {
    &self
  }
}

trait NumericArray<T> : Array {
  fn value(&self, i: i64) -> T;
}

#[derive(PartialEq, Eq)]
pub struct BooleanArray {
  data: ArrayData,
  raw_values: *const u8
}

impl BooleanArray {
  pub fn from_data(data: ArrayData) -> Self {
    let raw_values = data.raw_values();
    BooleanArray {
      data,
      raw_values
    }
  }

  pub fn raw_values(&self) -> *const u8 {
    self.raw_values
  }

  pub fn value(&self, i: i64) -> bool {
    bit_util::get_bit(self.raw_values, i + self.data.offset())
  }
}

impl BaseArray for BooleanArray {
  fn data(&self) -> &ArrayData {
    &self.data
  }
}

impl Cast for BooleanArray {
  fn as_bool(&self) -> &BooleanArray {
    &self
  }
}

macro_rules! define_numeric_array {
    ($name: ident, $cast_fn: ident) => {
      pub struct $name {
        data: ArrayData,
        raw_values: *const u8
      }

      impl $name {
        pub fn from_data(data: ArrayData) -> Self {
          let raw_values = data.raw_values();
          $name {
            data,
            raw_values
          }
        }

        pub fn raw_values(&self) -> *const u8 {
          self.raw_values
        }

        pub fn value(&self, i: i64) -> bool {
          bit_util::get_bit(self.raw_values, i + self.data.offset())
        }
      }

      impl BaseArray for $name {
        fn data(&self) -> &ArrayData {
          &self.data
        }
      }

      impl Cast for $name {
        fn $cast_fn(&self) -> &$name {
          &self
        }
      }
    };
}

define_numeric_array!(Int8Array, as_int8);
define_numeric_array!(Int16Array, as_int16);
define_numeric_array!(Int32Array, as_int32);
define_numeric_array!(Int64Array, as_int64);
define_numeric_array!(UInt8Array, as_uint8);
define_numeric_array!(UInt16Array, as_uint16);
define_numeric_array!(UInt32Array, as_uint32);
define_numeric_array!(UInt64Array, as_uint64);
define_numeric_array!(FloatArray, as_float);
define_numeric_array!(DoubleArray, as_double);
define_numeric_array!(HalfFloatArray, as_halffloat);




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