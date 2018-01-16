use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer, ResizableBuffer, MutableBuffer};
use array::{Array, Blob};

use std::ptr;
use std::mem;

const MIN_BUILDER_CAPACITY: i64 = 1 << 5;

// TODO: make ArrayData and ues different interfaces for building an array and reading from it
#[derive(Eq, PartialEq)]
pub struct ArrayBuilder<'a> {
  ty: Ty<'a>,
  null_count: i64,
  length: i64,
  capacity: i64,
  data: BuilderData
}

impl <'a> ArrayBuilder<'a> {
  pub fn null(len: i64) -> ArrayBuilder<'a> {
    ArrayBuilder {
      ty: Ty::NA,
      null_count: 0,
      length: len,
      capacity: 0,
      data: BuilderData::Null
    }
  }

  pub fn binary(null_bitmap: PoolBuffer, lengths_and_data: PoolBuffer) -> ArrayBuilder<'a> {
    ArrayBuilder {
      ty: Ty::Binary,
      null_count: 0,
      length: 0,
      capacity: 0,
      data: BuilderData::Binary {
        null_bitmap,
        lengths_and_data,
        cur_offset: 0
      }
    }
  }

  pub fn new_fixed_width(ty: Ty<'a>, null_bitmap: PoolBuffer, data: PoolBuffer) -> ArrayBuilder<'a> {
    let builder_data = match ty {
      Ty::Bool => BuilderData::Bool { null_bitmap, data },

      Ty::Int8 => BuilderData::Int8 { null_bitmap, data },
      Ty::Int16 => BuilderData::Int16 { null_bitmap, data },
      Ty::Int32 => BuilderData::Int32 { null_bitmap, data },
      Ty::Int64 => BuilderData::Int64 { null_bitmap, data },
      Ty::UInt8 => BuilderData::UInt8 { null_bitmap, data },
      Ty::UInt16 => BuilderData::UInt16 { null_bitmap, data },
      Ty::UInt32 => BuilderData::UInt32 { null_bitmap, data },
      Ty::UInt64 => BuilderData::UInt64 { null_bitmap, data },

      Ty::HalfFloat => BuilderData::HalfFloat { null_bitmap, data },
      Ty::Float => BuilderData::Float { null_bitmap, data },
      Ty::Double => BuilderData::Double { null_bitmap, data },

      Ty::Date64 { unit: ref _unit } => BuilderData::Date64 { null_bitmap, data },
      Ty::Date32 { unit: ref _unit } => BuilderData::Date32 { null_bitmap, data },
      Ty::Time64 { unit: ref _unit } => BuilderData::Time64 { null_bitmap, data },
      Ty::Time32 { unit: ref _unit } => BuilderData::Time32 { null_bitmap, data },
      Ty::Timestamp { unit: ref _unit, timezone: ref _timezone } => BuilderData::Timestamp { null_bitmap, data },
      Ty::Interval { unit: ref _unit } => BuilderData::Interval { null_bitmap, data },

      Ty::FixedSizeBinary { byte_width } => BuilderData::FixedSizeBinary { null_bitmap, data },

      _ => panic!("[{:?}] is not supported type", ty)
    };

    ArrayBuilder {
      ty,
      null_count: 0,
      length: 0,
      capacity: 0,
      data: builder_data,
    }
  }

  #[inline]
  pub fn ty(&self) -> &Ty {
    &self.ty
  }

  #[inline]
  pub fn null_bitmap(&self) ->Option<&PoolBuffer> {
    self.data.null_bitmap()
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
    self.null_count
  }

  #[inline]
  pub fn len(&self) -> i64 {
    self.length
  }

  #[inline]
  pub fn capacity(&self) -> i64 {
    self.capacity
  }

  #[inline]
  pub fn data(&self) -> &BuilderData {
    &self.data
  }

  fn get_capacity_for_type(ty: &Ty, req_capacity: i64) -> i64 {
    match ty {
      &Ty::Bool => i64::min(req_capacity, MIN_BUILDER_CAPACITY),
      _ => req_capacity
    }
  }

  fn reserve_null_bitmap(&mut self, len: i64) -> Result<(), ArrowError> {
    let new_len = self.length + len;
    if new_len > self.capacity {
      match self.force_resize_null_bitmap(new_len) {
        Ok(new_capacity) => {
          self.capacity = new_capacity;
          Ok(())
        },
        Err(e) => Err(e)
      }
    } else {
      Ok(())
    }
  }

  fn force_resize_null_bitmap(&mut self, new_len: i64) -> Result<i64, ArrowError> {
    let new_capacity = bit_util::next_power_2(new_len);
    // the capacity of null bitmap is always same with the array capacity
    match self.data.resize_null_bitmap(new_capacity) {
      Ok(_) => Ok(new_capacity),
      Err(e) => Err(e)
    }
  }

  fn reserve_bool(&mut self) -> Result<(), ArrowError> {
    match self.reserve_null_bitmap(MIN_BUILDER_CAPACITY) {
      Ok(_) => {
        let new_bits = self.length - self.null_count + 1;
        let new_bytes = bit_util::bytes_for_bits(new_bits);
        self.data.resize_data(new_bytes)
      },
      Err(e) => Err(e)
    }
  }

  fn reserve_fixed_width_type<T: Size>(&mut self, item: T) -> Result<(), ArrowError> {
    let new_length = self.length + 1;
    let null_bitmap_prepare_result = if new_length > self.capacity {
      match self.force_resize_null_bitmap(new_length) {
        Ok(new_capacity) => {
          self.capacity = new_capacity;
          Ok(())
        },
        Err(e) => Err(e)
      }
    } else {
      Ok(())
    };

    match null_bitmap_prepare_result {
      Ok(_) => self.data.resize_data(self.capacity * item.len()),
      Err(e) => Err(e)
    }
  }

  fn reserve_blob<T: Size>(&mut self, item: &T) -> Result<(), ArrowError> {
    let new_length = self.length + 1;
    let null_bitmap_prepare_result = if new_length > self.capacity {
      match self.force_resize_null_bitmap(new_length) {
        Ok(new_capacity) => {
          self.capacity = new_capacity;
          Ok(())
        },
        Err(e) => Err(e)
      }
    } else {
      Ok(())
    };

    match null_bitmap_prepare_result {
      Ok(_) => self.data.reserve_data(item.len()),
      Err(e) => Err(e)
    }
  }

  pub fn advance(&mut self, elem: i64) -> Result<(), ArrowError> {
    if self.length + elem > self.capacity {
      Err(ArrowError::invalid(String::from("Builder must be expanded")))
    } else {
      self.length = self.length + elem;
      Ok(())
    }
  }

  // append methods

  pub fn append_null(&mut self) -> Result<(), ArrowError> {
    match self.data {
      BuilderData::Null => {
        self.null_count = self.null_count + 1;
        self.length = self.length + 1;
        if self.length > self.capacity {
          self.capacity = bit_util::next_power_2(self.length);
        }
        Ok(())
      },
      _ => {
        match self.reserve_null_bitmap(1) {
          Ok(_) => {
            self.null_count = self.null_count + 1;
            self.length = self.length + 1;
            Ok(())
          },
          Err(e) => Err(e)
        }
      }
    }
  }
}

pub trait Size {
  fn len(&self) -> i64;
}

macro_rules! impl_size_for_primitive_types {
    ($ty: ty) => {
      impl Size for $ty {
        fn len(&self) -> i64 {
          mem::size_of::<$ty>() as i64
        }
      }
    };
}

impl_size_for_primitive_types!(u8);
impl_size_for_primitive_types!(i8);
impl_size_for_primitive_types!(u16);
impl_size_for_primitive_types!(i16);
impl_size_for_primitive_types!(u32);
impl_size_for_primitive_types!(i32);
impl_size_for_primitive_types!(u64);
impl_size_for_primitive_types!(i64);

pub trait Append<T> {
  fn append(&mut self, val: T) -> Result<(), ArrowError>;
}

impl <'a> Append<bool> for ArrayBuilder<'a> {
  fn append(&mut self, val: bool) -> Result<(), ArrowError> {
    match self.reserve_bool() {
      Ok(_) => {
        match self.data {
          BuilderData::Bool { ref mut null_bitmap, ref mut data } => {
            bit_util::set_bit(null_bitmap.data_as_mut(), self.length);
            if val {
              bit_util::set_bit(data.data_as_mut(), self.length);
            } else {
              bit_util::clear_bit(data.data_as_mut(), self.length);
            }
            self.length = self.length + 1;
            Ok(())
          },
          _ => panic!()
        }
      },
      Err(e) => Err(e)
    }
  }
}

macro_rules! impl_append_for_primitive_type {
    ($ty: ty, $builder_data: path) => {
      impl <'a> Append<$ty> for ArrayBuilder<'a> {
        fn append(&mut self, val: $ty) -> Result<(), ArrowError> {
          match self.reserve_fixed_width_type(val) {
            Ok(_) => {
              match self.data {
                $builder_data { ref mut null_bitmap, ref mut data } => {
                  bit_util::set_bit(null_bitmap.data_as_mut(), self.length);
                  unsafe { *(mem::transmute::<*mut u8, *mut $ty>(data.data_as_mut()).offset(self.length as isize)) = val }
                  self.length = self.length + 1;
                  Ok(())
                },
                _ => panic!()
              }
            },
            Err(e) => Err(e)
          }
        }
      }
    };
}

impl_append_for_primitive_type!(u8, BuilderData::UInt8);
impl_append_for_primitive_type!(i8, BuilderData::Int8);
impl_append_for_primitive_type!(u16, BuilderData::UInt16);
impl_append_for_primitive_type!(i16, BuilderData::Int16);
impl_append_for_primitive_type!(u32, BuilderData::UInt32);
impl_append_for_primitive_type!(i32, BuilderData::Int32);
impl_append_for_primitive_type!(u64, BuilderData::UInt64);
impl_append_for_primitive_type!(i64, BuilderData::Int64);

impl <'a> Append<Blob> for ArrayBuilder<'a> {
  fn append(&mut self, val: Blob) -> Result<(), ArrowError> {
    let reserve_result = self.reserve_blob(&val);
    match reserve_result {
      Ok(_) => {
        match self.data {
          BuilderData::Binary { ref mut null_bitmap, ref mut lengths_and_data, ref mut cur_offset } => {
            bit_util::set_bit(null_bitmap.data_as_mut(), self.length);
            unsafe {
              use std::intrinsics;
              use libc;
              // write offset
              *(mem::transmute::<*mut u8, *mut i32>(lengths_and_data.data_as_mut().offset(*cur_offset))) = val.len() as i32;
              // write data
              intrinsics::copy(val.p(), lengths_and_data.data_as_mut().offset(*cur_offset + mem::size_of::<i32>() as isize), val.len() as usize);
//              libc::memcpy(
//                mem::transmute::<*mut u8, *mut li bc::c_void>(lengths_and_data.data_as_mut().offset(*cur_offset + mem::size_of::<i32>() as isize)),
//                mem::transmute::<*const u8, *const libc::c_void>(val.p()),
//                val.len() as libc::size_t
//              );
            }
            *cur_offset = *cur_offset + mem::size_of::<i32>() as isize + val.len() as isize;
            self.length = self.length + 1;
            Ok(())
          },
          _ => panic!()
        }
      },
      Err(e) => Err(e)
    }
  }
}

#[derive(Clone, Eq, PartialEq)]
pub enum BuilderData {
  Null,
  Bool {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  UInt8 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Int8 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  UInt16 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Int16 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  UInt32 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Int32 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  UInt64 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Int64 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  HalfFloat {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Float {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Double {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  Binary {
    null_bitmap: PoolBuffer,
    lengths_and_data: PoolBuffer,
    cur_offset: isize
  },
  String {

  },
  FixedSizeBinary {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  Date64 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Date32 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Timestamp {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Time32 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Time64 {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },
  Interval {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  Decimal {
    null_bitmap: PoolBuffer,
    data: PoolBuffer
  },

  List {

  },
  Struct {

  },
  Union {

  },

  Dictionary {

  }
}

impl BuilderData {
  fn resize_null_bitmap(&mut self, new_capacity: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Null => Ok(()),
      &mut BuilderData::Bool { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::Int8 { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::UInt8 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::Int16 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt16 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int32 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt32 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int64 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt64 { ref mut null_bitmap, ref mut data } => {
        let new_bytes = bit_util::bytes_for_bits(new_capacity);
        if null_bitmap.size() != new_bytes {
          null_bitmap.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      &mut BuilderData::Binary { ref mut null_bitmap, ref mut lengths_and_data, cur_offset } => {
        let new_bytes = bit_util::bytes_for_bits(new_capacity);
        if null_bitmap.size() != new_bytes {
          null_bitmap.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      _ => panic!()
    }
  }

  fn reserve_data(&mut self, reserve_bytes: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Null => Ok(()),
      &mut BuilderData::Bool { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::Int8 { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::UInt8 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::Int16 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt16 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int32 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt32 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int64 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt64 { ref mut null_bitmap, ref mut data } => {
        if reserve_bytes > 0 {
          let new_bytes = reserve_bytes + data.size();
          data.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      &mut BuilderData::Binary { ref mut null_bitmap, ref mut lengths_and_data, cur_offset } => {
        if reserve_bytes > 0 {
          let new_bytes = mem::size_of::<i32>() as i64 + reserve_bytes + lengths_and_data.size();
          lengths_and_data.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      _ => panic!()
    }
  }

  fn resize_data(&mut self, new_bytes: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Null => Ok(()),
      &mut BuilderData::Bool { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::Int8 { ref mut null_bitmap, ref mut data }   |
      &mut BuilderData::UInt8 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::Int16 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt16 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int32 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt32 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::Int64 { ref mut null_bitmap, ref mut data }  |
      &mut BuilderData::UInt64 { ref mut null_bitmap, ref mut data } => {
        if data.size() != new_bytes {
          data.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      &mut BuilderData::Binary { ref mut null_bitmap, ref mut lengths_and_data, cur_offset } => {
        if lengths_and_data.size() != new_bytes {
          lengths_and_data.resize(new_bytes)
        } else {
          Ok(())
        }
      },
      _ => panic!()
    }
  }

  fn null_bitmap(&self) -> Option<&PoolBuffer> {
    match self {
      &BuilderData::Bool { ref null_bitmap, ref data } |
      &BuilderData::Int8 { ref null_bitmap, ref data } |
      &BuilderData::UInt8 { ref null_bitmap, ref data } |
      &BuilderData::Int16 { ref null_bitmap, ref data } |
      &BuilderData::UInt16 { ref null_bitmap, ref data } |
      &BuilderData::Int32 { ref null_bitmap, ref data } |
      &BuilderData::UInt32 { ref null_bitmap, ref data } |
      &BuilderData::Int64 { ref null_bitmap, ref data } |
      &BuilderData::UInt64 { ref null_bitmap, ref data } => Some(null_bitmap),
      &BuilderData::Binary { ref null_bitmap, ref lengths_and_data, cur_offset } => Some(null_bitmap),
      _ => None
    }
  }
}

fn init_buffer(buffer: &mut PoolBuffer, new_bits: i64) -> Result<(), ArrowError> {
  let nbytes = bit_util::bytes_for_bits(new_bits);
  buffer.resize(nbytes)
}

fn resize_buffer(buffer: &mut PoolBuffer, new_bits: i64) -> Result<(), ArrowError> {
  let old_bytes = buffer.size();
  let new_bytes = bit_util::bytes_for_bits(new_bits);

  if old_bytes == new_bytes {
    Ok(())
  } else {
    buffer.resize(new_bytes)
  }
}

#[cfg(test)]
mod tests {
  use memory_pool::DefaultMemoryPool;
  use buffer::{PoolBuffer, ResizableBuffer, MutableBuffer};
  use common::ty::Ty;
  use std::sync::Arc;
  use std::cell::RefCell;
  use builder::{ArrayBuilder, Append};
  use array::{Array, ArrowSlice};
  use rand;

  #[test]
  fn test_null_builder() {
    let mut builder = ArrayBuilder::null(0);
    for i in 0..100 {
      builder.append_null();
    }

    assert_eq!(100, builder.len());
    assert_eq!(100, builder.null_count());
    assert_eq!(128, builder.capacity());

    let array = Array::from(builder);

    assert_eq!(&Ty::NA, array.ty());
    assert_eq!(100, array.null_count());
    assert_eq!(100, array.len());
//    assert_eq!(0, array.offset());
  }

  #[test]
  fn test_bool_builder() {
    let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
    let null_bitmap = PoolBuffer::new(pool.clone());
    let data = PoolBuffer::new(pool.clone());

    let mut builder = ArrayBuilder::new_fixed_width(Ty::Bool, null_bitmap, data);
    let mut expected: Vec<bool> = Vec::new();
    for i in 0..100 {
      let val = rand::random::<bool>();
      builder.append(val);
      expected.push(val);
    }

    assert_eq!(100, builder.len());
    assert_eq!(256, builder.capacity());
    assert_eq!(0, builder.null_count());

    let array = Array::from(builder);

    assert_eq!(&Ty::Bool, array.ty());
    assert_eq!(100, array.len());
    assert_eq!(0, array.null_count());
//    assert_eq!(0, array.offset());

    for i in 0..100 {
      assert_eq!(expected[i], array.value(i as i64));
    }
  }

  // TODO: test boolean with null

  macro_rules! test_primitive_type_builder {
      ($test_name: ident, $ty: path, $prim_ty: ty, $expected_capacity: expr) => {
        #[test]
        fn $test_name() {
          let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
          let null_bitmap = PoolBuffer::new(pool.clone());
          let data = PoolBuffer::new(pool.clone());

          let mut builder = ArrayBuilder::new_fixed_width($ty, null_bitmap, data);
          let mut expected: Vec<$prim_ty> = Vec::new();
          for i in 0..100 {
            let val = rand::random::<$prim_ty>();
            builder.append(val);
            expected.push(val);
          }

          assert_eq!(100, builder.len());
          assert_eq!($expected_capacity, builder.capacity());
          assert_eq!(0, builder.null_count());

          let array = Array::from(builder);

          assert_eq!(&$ty, array.ty());
          assert_eq!(100, array.len());
          assert_eq!(0, array.null_count());
//          assert_eq!(0, array.offset());

          for i in 0..100 {
            assert_eq!(expected[i], array.value(i as i64));
          }

          assert_eq!(expected, array.values());
        }
      };
  }

  test_primitive_type_builder!(test_i8_builder, Ty::Int8, i8, 128);
  test_primitive_type_builder!(test_u8_builder, Ty::UInt8, u8, 128);
  test_primitive_type_builder!(test_i16_builder, Ty::Int16, i16, 128);
  test_primitive_type_builder!(test_u16_builder, Ty::UInt16, u16, 128);
  test_primitive_type_builder!(test_i32_builder, Ty::Int32, i32, 128);
  test_primitive_type_builder!(test_u32_builder, Ty::UInt32, u32, 128);
  test_primitive_type_builder!(test_i64_builder, Ty::Int64, i64, 128);
  test_primitive_type_builder!(test_u64_builder, Ty::UInt64, u64, 128);

  #[test]
  fn test_binary_builder() {
    use memory_pool::MemoryPool;
    use builder::Size;
    use array::Blob;
    use array::ArrayIterator;

    let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
    let null_bitmap = PoolBuffer::new(pool.clone());
    let data = PoolBuffer::new(pool.clone());

    let mut builder = ArrayBuilder::binary(null_bitmap, data);
    let mut expected: Vec<Blob> = Vec::new();
    let generator = pool.clone();
    let mut next_len = 10;
    for i in 0..100 {
      let len = next_len;
      next_len = next_len + 2;
      if next_len > 50 {
        next_len = 10;
      }
      let p = generator.borrow_mut().allocate(len).unwrap();
      unsafe {
        use std::mem;
        use libc;
        let dst = mem::transmute::<*const u8, *mut libc::c_void>(p);
        libc::memset(dst, 0, len as usize);
        libc::memcpy(dst, mem::transmute::<*const i64, *const libc::c_void>(&len), mem::size_of::<i64>());
      }
      let val = Blob::new(p, len as i32);
      builder.append(val);
      expected.push(val);
    }

    assert_eq!(100, builder.len());
    assert_eq!(128, builder.capacity());
    assert_eq!(0, builder.null_count());

    let array = Array::from(builder);

    assert_eq!(&Ty::Binary, array.ty());
    assert_eq!(100, array.len());
    assert_eq!(0, array.null_count());
//    assert_eq!(0, array.offset());

    let mut iter = ArrayIterator::new(array);

    for i in 0..100 {
      assert_eq!(expected[i], iter.next().unwrap());
    }

    assert!(iter.next().is_none());

    for blob in expected {
      pool.borrow_mut().free(blob.p(), blob.len())
    }
  }
}
