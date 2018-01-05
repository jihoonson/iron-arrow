use common::status::{ArrowError, StatusCode};

use std::cmp;
use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};
use libc;

pub trait MemoryPool {
  fn allocate(&mut self, size: i64) -> Result<*const u8, ArrowError>;

  fn reallocate(&mut self, old_size: i64, new_size: i64, page: *const u8) -> Result<*const u8, ArrowError>;

  fn free(&mut self, page: *const u8, size: i64);

  fn bytes_allocated(&self) -> i64;

  fn max_memory(&self) -> i64;
}

#[derive(Debug)]
pub struct DefaultMemoryPool {
  lock: Mutex<bool>,
  bytes_allocated: AtomicI64,
  max_memory: AtomicI64
}

impl DefaultMemoryPool {
  pub fn new() -> DefaultMemoryPool {
    DefaultMemoryPool {
      lock: Mutex::new(true),
      bytes_allocated: AtomicI64::new(0),
      max_memory: AtomicI64::new(0)
    }
  }
}

impl MemoryPool for DefaultMemoryPool {
  fn allocate(&mut self, size: i64) -> Result<*const u8, ArrowError> {
    match allocate_aligned(size) {
      Ok(page) => {
        println!("allocated memory of {} at {:?}", size, page);
        self.bytes_allocated.fetch_add(size, Ordering::Relaxed);

        let _locked = self.lock.lock().unwrap();
        let cur_max = self.max_memory.get_mut();
        let cur_alloc = self.bytes_allocated.load(Ordering::Relaxed);

        if *cur_max < cur_alloc {
          *cur_max = cur_alloc;
        }

        Ok(page)
      },
      Err(e) => Err(e)
    }
  }

  fn reallocate(&mut self, old_size: i64, new_size: i64, page: *const u8) -> Result<*const u8, ArrowError> {
    match allocate_aligned(new_size) {
      Ok(new_page) => {
        unsafe {
          let p_new_page = mem::transmute::<*const u8, *mut libc::c_void>(new_page);
          let p_old_page = mem::transmute::<*const u8, *mut libc::c_void>(page);
          if old_size > 0 {
            let copy_len = cmp::min(new_size, old_size) as usize;
            libc::memcpy(p_new_page, p_old_page, copy_len);
            if new_size > old_size {
              libc::memset(p_new_page.offset(old_size as isize), 0, (new_size - old_size) as usize);
            }
            libc::free(p_old_page);
          }
          self.bytes_allocated.fetch_add(new_size - old_size, Ordering::Relaxed);

          {
            let _locked = self.lock.lock().unwrap();
            let cur_max = self.max_memory.get_mut();
            let cur_alloc = self.bytes_allocated.load(Ordering::Relaxed);

            if *cur_max < cur_alloc {
              *cur_max = cur_alloc;
            }
          }

          Ok(new_page)
        }
      },
      Err(e) => Err(e)
    }
  }

  fn free(&mut self, page: *const u8, size: i64) {
    // TODO
    if self.bytes_allocated() < size {
      panic!("allocated bytes[{}] is less than free size[{}]", self.bytes_allocated(), size);
    } else {
//      println!("try freeing memory of {} from {:?}", size, page);
      unsafe {
        libc::free(mem::transmute::<*const u8, *mut libc::c_void>(page));
        self.bytes_allocated.fetch_sub(size, Ordering::Relaxed);
      }
    }
  }

  fn bytes_allocated(&self) -> i64 {
    self.bytes_allocated.load(Ordering::Relaxed)
  }

  fn max_memory(&self) -> i64 {
    self.max_memory.load(Ordering::Relaxed)
  }
}

const ALIGNMENT: usize = 64;

fn allocate_aligned(size: i64) -> Result<*const u8, ArrowError> {
  unsafe {
    let mut page: *mut libc::c_void = mem::uninitialized();
    let result = libc::posix_memalign(&mut page, ALIGNMENT, size as usize);
//    println!("allocated aligned memory of {} at {:?}", size, page);
    match result {
      libc::ENOMEM => Err(ArrowError::out_of_memory(format!("malloc of size {} failed", size))),
      libc::EINVAL => Err(ArrowError::invalid(format!("invalid alignment parameter: {}", ALIGNMENT))),
      _ => Ok(mem::transmute::<*mut libc::c_void, *const u8>(page))
    }
  }
}