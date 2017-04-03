#![feature(integer_atomics)]
extern crate libc;

pub mod common;
pub mod array;
pub mod memory_pool;

#[cfg(test)]
mod tests {
  use memory_pool;
  use common::status::ArrowError;

  #[test]
  fn test_allocate() {
    use memory_pool::{DefaultMemoryPool, MemoryPool};

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
    use memory_pool::{DefaultMemoryPool, MemoryPool};

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
}
