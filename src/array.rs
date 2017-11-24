use common::status::ArrowError;
use common::bit_util;
use memory_pool::MemoryPool;
use common::ty::{Ty, DataType};
use buffer::{Buffer, PoolBuffer};

use std::ptr;

use std::fmt::{Debug, Formatter, Error};

pub struct ArrayData {
  ty: Box<DataType>, // TODO: box? or ref?
  length: i64,
  null_count: i64,
  offset: i64,
  buffers: Vec<Box<PoolBuffer>>, // TODO: box? or mut ref? or rc?
  children: Vec<ArrayData> // TODO: box? or mut ref?
}

impl ArrayData {
  pub fn new(ty: Box<DataType>, length: i64, null_count: i64, offset: i64) -> ArrayData {
    ArrayData {
      ty,
      length,
      null_count,
      offset,
      buffers: Vec::new(),
      children: Vec::new()
    }
  }

  pub fn with_buffer(ty: Box<DataType>, length: i64, null_count: i64, offset: i64, buffer: Vec<Box<PoolBuffer>>) -> ArrayData {
    ArrayData {
      ty,
      length,
      null_count,
      offset,
      buffers: buffer,
      children: Vec::new()
    }
  }

//  pub fn from(other: &ArrayData) -> ArrayData {
//    ArrayData {
//      ty: other.ty.clone(),
//      length: other.length,
//      null_count: other.null_count,
//      offset: other.offset,
//      buffers: other.buffers.clone(),
//      children: other.children.clone()
//    }
//  }

  pub fn data_type(&self) -> &Box<DataType> {
    &self.ty
  }

  pub fn len(&self) -> i64 {
    self.length
  }

  pub fn null_count(&self) -> i64 {
    self.null_count
  }

  pub fn offset(&self) -> i64 {
    self.offset
  }

  pub fn buffers(&self) -> &Vec<Box<PoolBuffer>> {
    &self.buffers
  }

  pub fn children(&self) -> &Vec<ArrayData> {
    &self.children
  }

  pub fn child(&self, i: usize) -> &ArrayData {
    &self.children[i]
  }

  pub fn num_children(&self) -> i32 {
    self.children.len() as i32
  }
}

impl PartialEq for ArrayData {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for ArrayData {

}

trait BaseArray {
  fn is_null(&self, i: i64) -> bool {
    self.null_bitmap_data().is_some() &&
      bit_util::bit_not_set(self.null_bitmap_data().unwrap(), i + self.offset())
  }

  fn is_valid(&self, i: i64) -> bool {
    self.null_bitmap_data().is_some() &&
      bit_util::get_bit(self.null_bitmap_data().unwrap(), i + self.offset())
  }

  fn len(&self) -> i64 {
    self.data().len()
  }

  fn offset(&self) -> i64 {
    self.data().offset()
  }

  fn null_count(&self) -> i64;

  fn data_type(&self) -> &Box<DataType> {
    self.data().data_type()
  }

  fn ty(&self) -> Ty {
    self.data_type().ty()
  }

  fn null_bitmap(&self) -> &Vec<Box<PoolBuffer>> {
    self.data().buffers()
  }

  fn null_bitmap_data(&self) -> Option<*const u8>;

  fn data(&self) -> &ArrayData;

  fn num_fields(&self) -> i32 {
    self.data().num_children()
  }
}

pub fn array_eq(a1: &BaseArray, a2: &BaseArray) -> bool {
  unimplemented!()
}

impl PartialEq for Box<BaseArray> {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for Box<BaseArray> {

}

impl Debug for Box<BaseArray> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl Clone for Box<BaseArray> {
  fn clone(&self) -> Self {
    unimplemented!()
  }
}

trait Cast {
  fn as_null(&self) -> &NullArray {
    unimplemented!("Cannot cast to null")
  }
}

pub trait Array : BaseArray + Cast {}

#[derive(PartialEq, Eq)]
pub struct NullArray {
  data: ArrayData
}

impl BaseArray for NullArray {
  fn is_null(&self, i: i64) -> bool {
    true
  }

  fn null_count(&self) -> i64 {
    self.data.len()
  }

  fn null_bitmap_data(&self) -> Option<*const u8> {
    Option::None
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

pub struct BooleanArray {
  data: ArrayData
}

impl BaseArray for BooleanArray {
  fn null_count(&self) -> i64 {
    unimplemented!()
  }

  fn null_bitmap_data(&self) -> Option<*const u8> {
    unimplemented!()
  }

  fn data(&self) -> &ArrayData {
    unimplemented!()
  }
}


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