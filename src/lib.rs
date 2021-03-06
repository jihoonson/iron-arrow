#![feature(integer_atomics)]
#![feature(box_syntax, box_patterns)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(const_size_of)]

extern crate libc;
extern crate num;
extern crate rand;

pub mod common;
pub mod array;
pub mod memory_pool;
pub mod buffer;
pub mod builder;

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use std::cell::RefCell;
  use common::status::ArrowError;
  use memory_pool::{DefaultMemoryPool, MemoryPool};

  #[test]
  fn test_drop_empty_pool_buffer() {
    use buffer::PoolBuffer;
    let mut buffer = PoolBuffer::new(Arc::new(RefCell::new(DefaultMemoryPool::new())));
  }

  #[test]
  fn test_pool_buffer() {
    use buffer::{Buffer, MutableBuffer, ResizableBuffer, PoolBuffer};

    let mut buffer = PoolBuffer::new(Arc::new(RefCell::new(DefaultMemoryPool::new())));
    buffer.reserve(100).unwrap();
    assert_eq!(128, buffer.capacity());
    assert_eq!(0, buffer.size());

    buffer.resize(10).unwrap();
    assert_eq!(128, buffer.capacity());
    assert_eq!(10, buffer.size());
  }

  #[test]
  fn test_buffer_builder() {
    use buffer::{Buffer, MutableBuffer, ResizableBuffer, PoolBuffer, BufferBuilder, TypedBufferBuilder};

    let mut buffer_builder = BufferBuilder::new(Arc::new(RefCell::new(DefaultMemoryPool::new())));
    for i in 0..100 {
      buffer_builder.append_typed_val(i + 10);
    }

    let buffer = buffer_builder.finish();
    assert_eq!(400, buffer.size());
    assert_eq!(512, buffer.capacity());
  }

//  #[test]
//  fn test_array_data() {
//    use common::ty::DataType;
//    use array::ArrayData;
//    use buffer::PoolBuffer;
//
//    let mut pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
//    let data = ArrayData::new(DataType::int32(), 100, 0, PoolBuffer::new(pool.clone()), PoolBuffer::new(pool.clone()));
//
//    assert_eq!(&DataType::int32(), data.data_type());
//    assert_eq!(100, data.len());
//  }
}
