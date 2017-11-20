//use common::status::ArrowError;
//use common::bit_util;
//use memory_pool::MemoryPool;
//use common::ty::DataType;
//use buffer::{Buffer, PoolBuffer};
//
//use std::ptr;

use std::fmt::{Debug, Formatter, Error};

pub trait Array {

}

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



//
//#[derive(Eq, PartialEq)]
//pub struct ArrayData<'a> {
//  ty: &'a DataType, // TODO: box? or ref?
//  length: i64,
//  null_count: i64,
//  offset: i64,
//  buffers: Vec<Box<PoolBuffer>>, // TODO: box? or mut ref?
//  children: Vec<Box<ArrayData<'a>>> // TODO: box? or mut ref?
//}
//
//impl<'a> ArrayData<'a> {
//  pub fn new(ty: &'a DataType, length: i64, null_count: i64, offset: i64) -> ArrayData<'a> {
//    ArrayData {
//      ty,
//      length,
//      null_count,
//      offset,
//      buffers: Vec::new(),
//      children: Vec::new()
//    }
//  }
//
//  pub fn with_buffer(ty: &'a DataType, length: i64, null_count: i64, offset: i64, buffer: Vec<Box<PoolBuffer>>) -> ArrayData<'a> {
//    ArrayData {
//      ty,
//      length,
//      null_count,
//      offset,
//      buffers: buffer,
//      children: Vec::new()
//    }
//  }
//
////  pub fn from(other: &'a ArrayData) -> ArrayData<'a> {
////    ArrayData {
////      ty: other.ty,
////      length: other.length,
////      null_count: other.null_count,
////      offset: other.offset,
////      buffers: other.buffers,
////      children: other.children
////    }
////  }
//
//  pub fn ty(&self) -> &'a DataType {
//    self.ty
//  }
//
//  pub fn len(&self) -> i64 {
//    self.length
//  }
//
//  pub fn null_count(&self) -> i64 {
//    self.null_count
//  }
//
//  pub fn offset(&self) -> i64 {
//    self.offset
//  }
//}
//
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