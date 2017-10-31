use common::status::ArrowError;
use common::bit_util;
use memory_pool::MemoryPool;
use common::ty::{DataType, FixedWidthType};
use buffer::{Buffer, PoolBuffer};

use std::ptr;

#[derive(Eq, PartialEq)]
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

  pub fn to_array<T: Array<'a>>(self) -> Box<T> {
    Box::new(T::from_data(self))
  }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ArrayType {

}

pub trait Array<'a> {
  fn from_data<T: Array<'a>>(data: ArrayData<'a>) -> T;
}

macro_rules! define_base_array {
    ($name: ident) => {
      #[derive(Eq, PartialEq)]
      pub struct $name<'a> {
        data: ArrayData<'a>,
        null_bitmap_data: *const u8
      }

      impl<'a> $name<'a> {
        pub fn is_null(&self, i: i64) -> bool {
          self.null_bitmap_data.is_null() || bit_util::bit_not_set(self.null_bitmap_data, i + self.data.offset())
        }

        pub fn len(&self) -> i64 {
          self.data.len()
        }

        pub fn offset(&self) -> i64 {
          self.data.offset()
        }

        pub fn ty(&self) -> &DataType {
          self.data.ty()
        }

        pub fn data(&self) -> &ArrayData {
          &self.data
        }

        pub fn null_bitmap_data(&self) -> &*const u8 {
          &self.null_bitmap_data
        }
      }
    };
}

define_base_array!(NullArray);

impl <'a> NullArray<'a> {
  pub fn with_data(array_data: ArrayData<'a>) -> NullArray<'a> {
    let null_data = ArrayData {
      ty: array_data.ty,
      length: array_data.length,
      null_count: array_data.length,
      offset: 0,
      buffers: array_data.buffers,
      children: array_data.children
    };
    NullArray {
      data: null_data,
      null_bitmap_data: ptr::null()
    }
  }
}

impl <'a> Array<'a> for NullArray<'a> {
  fn from_data<T: Array<'a>>(data: ArrayData<'a>) -> T {
    NullArray::with_data(data)
  }
}

macro_rules! impl_primitive_array {
    ($name: ident) => {
      impl<'a> $name<'a> {
        pub fn with_data(array_data: ArrayData<'a>) -> $name<'a> {
          let value_buffer = &array_data.buffers[1];
          $name {
            data: array_data,
            null_bitmap_data: if array_data.buffers.len() > 0 && !value_buffer.is_null() {
              value_buffer.data()
            } else {
              ptr::null()
            }
          }
        }

        pub fn values(&self) -> &Box<PoolBuffer> {
          &self.data.buffers[1]
        }

        pub fn raw_values(&self) -> *const u8 {
          // raw_values = data->buffers[1]->data()
          if Box::into_raw(*self.value_buffer()).is_null() {
            ptr::null()
          } else {
            let offset = self.offset() * self.ty().as_fixed_width_type_info().get_bit_width() as i64 / 8;
            self.data.buffers[1].data().offset(offset as isize)
          }
        }

        fn value_buffer(&self) -> &Box<PoolBuffer> {
          &self.data.buffers[1]
        }
      }
    }
}

define_base_array!(BoolArray);
//impl_primitive_array!(BoolArray);

//impl <'a> BoolArray<'a> {
//  pub fn value(&self, i: i64) -> bool {
//    let raw_value_buffer = Box::into_raw(*self.value_buffer());
//    if raw_value_buffer.is_null() {
//      panic!()
//    } else {
//      bit_util::get_bit((*raw_value_buffer).data(), i + self.data.offset())
//    }
//  }
//}