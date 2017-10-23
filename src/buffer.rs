use common::status::ArrowError;
use common::bit_util;
use memory_pool::MemoryPool;

use std::mem;

use libc;
use num::Num;

pub trait Buffer {
  fn capacity(&self) -> i64;

  fn size(&self) -> i64;

  fn data(&self) -> *const u8;

  fn parent(&self) -> Option<&Buffer>;
}

pub trait MutableBuffer {
  fn data_as_mut(&mut self) -> *mut u8;
}

pub trait ResizableBuffer<T> {
  fn resize(&mut self, new_size: i64) -> Result<&mut T, ArrowError>;

  fn reserve(&mut self, new_capacity: i64) -> Result<&mut T, ArrowError>;
}

fn resize(pool: &mut Box<MemoryPool>, page: *const u8, size: i64, capacity: i64, new_size: i64) -> Result<(*const u8, i64, i64), ArrowError> {
  if new_size > size {
    match reserve(pool, page, capacity, new_size) {
      Ok((new_page, new_capacity)) => Ok((new_page, new_size, new_capacity)),
      Err(e) => Err(e)
    }
  } else {
    let new_capacity = bit_util::round_up_to_multiple_of_64(new_size);
    if capacity != new_capacity {
      if new_size == 0 {
        pool.free(page, capacity);
        Ok((unsafe { mem::uninitialized() }, 0, 0))
      } else {
        match pool.reallocate(capacity, new_capacity, page) {
          Ok(new_page) => {
            Ok((new_page, new_size, new_capacity))
          },
          Err(e) => Err(e)
        }
      }
    } else {
      Ok((page, new_size, capacity))
    }
  }
}

fn reserve(pool: &mut Box<MemoryPool>, page: *const u8, capacity: i64, new_capacity: i64) -> Result<(*const u8, i64), ArrowError> {
  if new_capacity > capacity {
    let new_capacity = bit_util::round_up_to_multiple_of_64(new_capacity);
    match pool.reallocate(capacity, new_capacity, page) {
      Ok(new_page) => {
        Ok((new_page, new_capacity))
      },
      Err(e) => Err(e)
    }
  } else {
    Ok((page, capacity))
  }
}

fn as_mut<T>(p: *const u8) -> *mut T {
  unsafe { mem::transmute::<*const u8, *mut T>(p) }
}

// Eq, PartialEq
// Copy?

pub struct PoolBuffer {
  pool: Box<MemoryPool>,
  page: *const u8,
  size: i64,
  capacity: i64
//  parent: Option<Box<Buffer>>
}

impl PoolBuffer {
  pub fn new(pool: Box<MemoryPool>) -> PoolBuffer {
    PoolBuffer {
      pool,
      page: unsafe { mem::uninitialized() },
      size: 0,
      capacity: 0,
//      parent: None
    }
  }

  pub fn from(pool: Box<MemoryPool>, page: *const u8, size: i64, capacity: i64) -> PoolBuffer {
    PoolBuffer {
      pool: pool,
      page: page,
      size: size,
      capacity: capacity,
//      parent: None
    }
  }

  pub fn capacity(&self) -> i64 {
    self.capacity
  }

  pub fn size(&self) -> i64 {
    self.size
  }

  pub fn parent(&self) -> Option<&Buffer> {
    None
  }

  pub fn data(&self) -> *const u8 {
    self.page
  }
}

impl PartialEq for PoolBuffer {
  fn eq(&self, other: &PoolBuffer) -> bool {
    self.size == other.size &&
      (unsafe { self.page == other.page ||
        libc::memcmp(
          mem::transmute::<*const u8, *const libc::c_void>(self.page),
          mem::transmute::<*const u8, *const libc::c_void>(other.page),
          self.size as usize
        ) == 0})
  }
}

impl Eq for PoolBuffer {}

impl MutableBuffer for PoolBuffer {
  #[inline]
  fn data_as_mut(&mut self) -> *mut u8 {
    as_mut(self.page)
  }
}

impl ResizableBuffer<PoolBuffer> for PoolBuffer {
  fn resize(&mut self, new_size: i64) -> Result<&mut PoolBuffer, ArrowError> {
    match resize(&mut self.pool, self.page, self.size, self.capacity, new_size) {
      Ok((new_page, new_size, new_capacity)) => {
        self.page = new_page;
        self.size = new_size;
        self.capacity = new_capacity;
        Ok(self)
      },
      Err(e) => Err(e)
    }
  }

  fn reserve(&mut self, new_capacity: i64) -> Result<&mut PoolBuffer, ArrowError> {
    match reserve(&mut self.pool, self.page, self.capacity, new_capacity) {
      Ok((new_page, new_capacity)) => {
        self.page = new_page;
        self.capacity = new_capacity;
        Ok(self)
      },
      Err(e) => Err(e)
    }
  }
}

impl Drop for PoolBuffer {
  fn drop(&mut self) {
    if self.capacity > 0 {
      self.pool.free(self.page, self.capacity);
    }
  }
}

pub trait TypedBufferBuilder<T> {
  fn append_typed_val(&mut self, val: T) -> Result<&mut TypedBufferBuilder<T>, ArrowError>;

  fn append_typed_vals(&mut self, vals: *const T, num_vals: i64) -> Result<&mut TypedBufferBuilder<T>, ArrowError>;

  fn unsafe_append_typed_val(&mut self, val: T);

  fn unsafe_append_typed_vals(&mut self, vals: *const T, num_vals: i64);
}

pub struct BufferBuilder {
  pool: Box<MemoryPool>,
  page: *const u8,
  size: i64,
  capacity: i64
}

impl BufferBuilder {
  pub fn new(pool: Box<MemoryPool>) -> BufferBuilder {
    BufferBuilder {
      pool: pool,
      page: unsafe { mem::uninitialized() },
      size: 0,
      capacity: 0
    }
  }

  pub fn resize(&mut self, elements: i64) -> Result<&mut BufferBuilder, ArrowError> {
    if elements == 0 {
      Ok(self)
    } else {
      let old_capacity = self.capacity;
      match resize(&mut self.pool, self.page, self.size, self.capacity, elements) {
        Ok((new_page, _, new_capacity)) => {
          self.page = new_page;
          self.capacity = new_capacity;
          if new_capacity > old_capacity {
            unsafe {
              libc::memset(as_mut(self.page), 0, (new_capacity - old_capacity) as usize);
            }
          }
          Ok(self)
        },
        Err(e) => Err(e)
      }
    }
  }

  fn unsafe_append(&mut self, data: *const u8, len: i64) {
    // Unsafe methods don't check existing size
    unsafe {
      libc::memcpy(
        as_mut(self.page.offset(self.size as isize)),
        as_mut(data),
        len as usize
      );
      self.size += len;
    }
  }

  pub fn append(&mut self, data: *const u8, len: i64) -> Result<&mut BufferBuilder, ArrowError> {
    if self.capacity < len + self.size {
      let new_capacity = bit_util::next_power_2(len + self.size);
      match self.resize(new_capacity) {
        Ok(buffer_builder) => {
          buffer_builder.unsafe_append(data, len);
          Ok(buffer_builder)
        },
        Err(e) => Err(e)
      }
    } else {
      self.unsafe_append(data, len);
      Ok(self)
    }
  }

  pub fn advance(&mut self, len: i64) -> Result<&mut BufferBuilder, ArrowError> {
    if self.capacity < len + self.size {
      match resize(&mut self.pool, self.page, self.size, self.capacity, self.size + len) {
        Ok((new_page, new_size, new_capacity)) => {
          self.page = new_page;
          self.size = new_size;
          self.capacity = new_capacity;
          unsafe {
            libc::memset(
              as_mut(self.page.offset(self.size as isize)),
              0,
              len as libc::size_t
            );
          }
          self.size += len;
          Ok(self)
        },
        Err(e) => Err(e)
      }
    } else {
      unsafe {
        libc::memset(
          as_mut(self.page.offset(self.size as isize)),
          0,
          len as libc::size_t
        );
      }
      self.size += len;
      Ok(self)
    }
  }

  pub fn finish(self) -> PoolBuffer {
    PoolBuffer::from(self.pool, self.page, self.size, self.capacity)
  }
}

impl<T> TypedBufferBuilder<T> for BufferBuilder {

  fn append_typed_val(&mut self, val: T) -> Result<&mut TypedBufferBuilder<T>, ArrowError> {
    self.append_typed_vals(&val, 1)
  }

  fn append_typed_vals(&mut self, vals: *const T, num_vals: i64) -> Result<&mut TypedBufferBuilder<T>, ArrowError> {
    match self.append(
      unsafe { mem::transmute::<*const T, *const u8>(vals) },
      num_vals * mem::size_of::<T>() as i64
    ) {
      Ok(buffer_builder) => Ok(buffer_builder),
      Err(e) => Err(e)
    }
  }

  fn unsafe_append_typed_val(&mut self, val: T) {
    self.unsafe_append_typed_vals(&val, 1)
  }

  fn unsafe_append_typed_vals(&mut self, vals: *const T, num_vals: i64) {
    self.unsafe_append(
      unsafe { mem::transmute::<*const T, *const u8>(vals) },
      num_vals * mem::size_of::<T>() as i64
    )
  }
}