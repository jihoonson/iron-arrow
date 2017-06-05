#![feature(integer_atomics)]
#![feature(box_syntax, box_patterns)]
extern crate libc;
extern crate num;

pub mod common;
pub mod array;
pub mod memory_pool;
pub mod buffer;

#[cfg(test)]
mod tests {
  use memory_pool;
  use common::status::ArrowError;
  use memory_pool::{DefaultMemoryPool, MemoryPool};

  #[test]
  fn test_allocate() {
    let mut pool = DefaultMemoryPool::new();
    match pool.allocate(100) {
      Ok(page) => {
        assert_eq!(100, pool.bytes_allocated());
        assert_eq!(100, pool.max_memory());

        pool.free(page, 100);
        assert_eq!(0, pool.bytes_allocated());
        assert_eq!(100, pool.max_memory());
      },
      Err(e) => panic!("{}", e.message())
    }
  }

  #[test]
  fn test_reallocate() {
    let mut pool = DefaultMemoryPool::new();
    let page = match pool.allocate(100) {
      Ok(page) => page,
      Err(e) => panic!("{}", e.message())
    };
    assert_eq!(100, pool.bytes_allocated());
    assert_eq!(100, pool.max_memory());

    let page = match pool.reallocate(100, 200, page) {
      Ok(page) => page,
      Err(e) => panic!("{}", e.message())
    };
    assert_eq!(200, pool.bytes_allocated());
    assert_eq!(200, pool.max_memory());

    let page = match pool.reallocate(200, 50, page) {
      Ok(page) => page,
      Err(e) => panic!("{}", e.message())
    };
    assert_eq!(50, pool.bytes_allocated());
    assert_eq!(200, pool.max_memory());

    pool.free(page, 50);
    assert_eq!(0, pool.bytes_allocated());
    assert_eq!(200, pool.max_memory());
  }

  #[test]
  fn test_drop_empty_pool_buffer() {
    use buffer::PoolBuffer;
    let mut buffer = PoolBuffer::new(Box::new(DefaultMemoryPool::new()));
  }

  #[test]
  fn test_pool_buffer() {
    use buffer::{Buffer, MutableBuffer, ResizableBuffer, PoolBuffer, BufferBuilder};

    let mut pool = DefaultMemoryPool::new();
    let mut buffer = PoolBuffer::new(Box::new(pool));
    let mut buffer = buffer.reserve(10).unwrap();
  }
}
