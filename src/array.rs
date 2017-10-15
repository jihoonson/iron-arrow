use common::status::ArrowError;
use common::bit_util;
use memory_pool::MemoryPool;
use common::ty::DataType;
use buffer::{Buffer, PoolBuffer};

pub struct ArrayData<'a> {
  ty: &'a DataType, // TODO: box? or ref?
  length: i64,
  null_count: i64,
  offset: i64,
  buffers: Vec<Box<PoolBuffer>>, // TODO: box? or mut ref?
  children: Vec<Box<ArrayData<'a>>> // TODO: box? or mut ref?
}

impl<'a> ArrayData<'a> {
  pub fn new(ty: &'a DataType, length: i64, null_count: i64, offset: i64) -> ArrayData<'a> {
    ArrayData {
      ty,
      length,
      null_count,
      offset,
      buffers: Vec::new(),
      children: Vec::new()
    }
  }

  pub fn with_buffer(ty: &'a DataType, length: i64, null_count: i64, offset: i64, buffer: Vec<Box<PoolBuffer>>) -> ArrayData<'a> {
    ArrayData {
      ty,
      length,
      null_count,
      offset,
      buffers: buffer,
      children: Vec::new()
    }
  }

//  pub fn from(other: &'a ArrayData) -> ArrayData<'a> {
//    ArrayData {
//      ty: other.ty,
//      length: other.length,
//      null_count: other.null_count,
//      offset: other.offset,
//      buffers: other.buffers,
//      children: other.children
//    }
//  }

  pub fn ty(&self) -> &'a DataType {
    self.ty
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
}

#[derive(Debug, Eq, PartialEq)]
pub enum ArrayType {

}
