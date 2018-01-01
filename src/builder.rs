use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer, ResizableBuffer, MutableBuffer};
use array::Array;

use std::ptr;
use std::mem;

const MIN_BUILDER_CAPACITY: i64 = 1 << 5;

pub struct ArrayBuilder {
  init: bool,
  ty: Ty,
  null_bitmap: PoolBuffer,
  null_count: i64,
  length: i64,
  capacity: i64,
  data: BuilderData
}

impl ArrayBuilder {
  pub fn new(ty: Ty, null_bitmap: PoolBuffer, data: PoolBuffer) -> ArrayBuilder {
    let builder_data = match ty {
      Ty::NA => BuilderData::Null,
      Ty::Bool => BuilderData::Bool { data },

      Ty::Int8 => BuilderData::Int8 { data },
      Ty::Int16 => BuilderData::Int16 { data },
      Ty::Int32 => BuilderData::Int32 { data },
      Ty::Int64 => BuilderData::Int64 { data },
      Ty::UInt8 => BuilderData::UInt8 { data },
      Ty::UInt16 => BuilderData::UInt16 { data },
      Ty::UInt32 => BuilderData::UInt32 { data },
      Ty::UInt64 => BuilderData::UInt64 { data },

      Ty::HalfFloat => BuilderData::HalfFloat { data },
      Ty::Float => BuilderData::Float { data },
      Ty::Double => BuilderData::Double { data },

      Ty::Date64 { unit: ref _unit } => BuilderData::Date64 { data },
      Ty::Date32 { unit: ref _unit } => BuilderData::Date32 { data },
      Ty::Time64 { unit: ref _unit } => BuilderData::Time64 { data },
      Ty::Time32 { unit: ref _unit } => BuilderData::Time32 { data },
      Ty::Timestamp { unit: ref _unit, timezone: ref _timezone } => BuilderData::Timestamp { data },
      Ty::Interval { unit: ref _unit } => BuilderData::Interval { data },

      Ty::FixedSizeBinary { byte_width } => BuilderData::FixedSizeBinary { data },

      _ => panic!("[{:?}] is not supported type", ty)
    };

    ArrayBuilder {
      init: false,
      ty,
      null_bitmap,
      null_count: 0,
      length: 0,
      capacity: 0,
      data: builder_data,
    }
  }

  pub fn ty(&self) -> &Ty {
    &self.ty
  }

  pub fn null_bitmap(&self) ->&PoolBuffer {
    &self.null_bitmap
  }

  pub fn null_count(&self) -> i64 {
    self.null_count
  }

  pub fn len(&self) -> i64 {
    self.length
  }

  pub fn capacity(&self) -> i64 {
    self.capacity
  }

  pub fn init(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match init_buffer(&mut self.null_bitmap, capacity) {
      Ok(_) => {
        self.capacity = capacity;
        match self.data.init(capacity) {
          Ok(_) => {
            self.init = true;
            Ok(())
          },
          Err(e) => Err(e)
        }
      },
      Err(e) => Err(e)
    }
  }

  pub fn resize(&mut self, req_capacity: i64) -> Result<(), ArrowError> {
    let new_capacity = ArrayBuilder::get_capacity_for_type(self.ty(), req_capacity);

    if !self.init {
      return self.init(new_capacity);
    }

    match resize_buffer(&mut self.null_bitmap, new_capacity) {
      Ok(_) => {
        self.capacity = new_capacity;
        self.data.resize(new_capacity)
      },
      Err(e) => Err(e)
    }
  }

  fn get_capacity_for_type(ty: &Ty, req_capacity: i64) -> i64 {
    match ty {
      &Ty::Bool => i64::min(req_capacity, MIN_BUILDER_CAPACITY),
      _ => req_capacity
    }
  }

  pub fn reserve(&mut self, elem: i64) -> Result<(), ArrowError> {
    if self.length + elem > self.capacity {
      let new_capacity = bit_util::next_power_2(self.length + elem);
      self.resize(new_capacity)
    } else {
      Ok(())
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
    let required_elem = match self.data {
      BuilderData::Null => 0,
      BuilderData::Bool { ref data } => 1,
      _ => panic!()
    };

    match self.reserve(required_elem) {
      Ok(_) => {
        self.unsafe_append_to_bitmap(false);
        Ok(())
      },
      Err(e) => Err(e)
    }
  }

  pub fn append_bool(&mut self, val: bool) -> Result<(), ArrowError> {
    match self.data {
      BuilderData::Bool { ref mut data } => {

      },
      _ => panic!()
    };

    match self.reserve(1) {
      Ok(_) => {
        match self.data {
          BuilderData::Bool { ref mut data } => {
            bit_util::set_bit(self.null_bitmap.data_as_mut(), self.length);
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

  fn unsafe_append_to_bitmap(&mut self, is_valid: bool) {
    if is_valid {
      bit_util::set_bit(self.null_bitmap.data_as_mut(), self.length);
    } else {
      self.null_count = self.null_count + 1;
    }
    self.length = self.length + 1;
  }

  pub fn finish(&self) -> Result<Array, ArrowError> {
    let array = match self.data {
      BuilderData::Null => Array::null(self.length, 0),

      // primitive types
      // TODO: null_bitmap should be able to be passed without clone
      BuilderData::Bool      { ref data } |
      BuilderData::Int8      { ref data } |
      BuilderData::Int16     { ref data } |
      BuilderData::Int32     { ref data } |
      BuilderData::Int64     { ref data } |
      BuilderData::UInt8     { ref data } |
      BuilderData::UInt16    { ref data } |
      BuilderData::UInt32    { ref data } |
      BuilderData::UInt64    { ref data } |
      BuilderData::HalfFloat { ref data } |
      BuilderData::Float     { ref data } |
      BuilderData::Double    { ref data } |
      BuilderData::Date64    { ref data } |
      BuilderData::Date32    { ref data } |
      BuilderData::Time64    { ref data } |
      BuilderData::Time32    { ref data } |
      BuilderData::Timestamp { ref data } |
      BuilderData::Interval  { ref data } => Array::primitive(
        self.ty.clone(),
        self.length,
        0,
        Some(self.null_bitmap.clone()),
        data
      ),

      // fixed size binary types
      BuilderData::FixedSizeBinary { ref data } |
      BuilderData::Decimal         { ref data } => Array::fixed_width(
        self.ty.clone(),
        self.length,
        0,
        Some(self.null_bitmap.clone()),
        data
      ),

      _ => panic!()
    };

    Ok(array)
  }
}

#[derive(Clone)]
enum BuilderData {
  Null,
  Bool {
    data: PoolBuffer
  },

  UInt8 {
    data: PoolBuffer
  },
  Int8 {
    data: PoolBuffer
  },
  UInt16 {
    data: PoolBuffer
  },
  Int16 {
    data: PoolBuffer
  },
  UInt32 {
    data: PoolBuffer
  },
  Int32 {
    data: PoolBuffer
  },
  UInt64 {
    data: PoolBuffer
  },
  Int64 {
    data: PoolBuffer
  },

  HalfFloat {
    data: PoolBuffer
  },
  Float {
    data: PoolBuffer
  },
  Double {
    data: PoolBuffer
  },

  Binary {

  },
  String {

  },
  FixedSizeBinary {
    data: PoolBuffer
  },

  Date64 {
    data: PoolBuffer
  },
  Date32 {
    data: PoolBuffer
  },
  Timestamp {
    data: PoolBuffer
  },
  Time32 {
    data: PoolBuffer
  },
  Time64 {
    data: PoolBuffer
  },
  Interval {
    data: PoolBuffer
  },

  Decimal {
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
  fn init(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Bool { ref mut data } => {
        init_buffer(data, capacity)
      },
      _ => Ok(())
    }
  }

  fn resize(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Bool { ref mut data } => {
        resize_buffer(data, capacity)
      },
      _ => Ok(())
    }
  }
}

fn init_buffer(buffer: &mut PoolBuffer, capacity: i64) -> Result<(), ArrowError> {
  let nbytes = bit_util::bytes_for_bits(capacity);
  match buffer.resize(nbytes) {
    Ok(_) => {
      buffer.clear(0, nbytes);
      Ok(())
    },
    Err(e) => Err(e)
  }
}

fn resize_buffer(buffer: &mut PoolBuffer, capacity: i64) -> Result<(), ArrowError> {
  let old_bytes = buffer.size();
  let new_bytes = bit_util::bytes_for_bits(capacity);

  if old_bytes == new_bytes {
    Ok(())
  } else {
    match buffer.resize(new_bytes) {
      Ok(_) => {
        let new_capacity = buffer.capacity();
        if old_bytes < new_bytes {
          buffer.clear(old_bytes, new_capacity - old_bytes);
        }
        Ok(())
      },
      Err(e) => Err(e)
    }
  }
}

#[cfg(test)]
mod tests {
  use memory_pool::DefaultMemoryPool;
  use buffer::{PoolBuffer, ResizableBuffer, MutableBuffer};
  use common::ty::Ty;
  use std::sync::Arc;
  use std::cell::RefCell;
  use builder::ArrayBuilder;
  use array::Array;

  #[test]
  fn test_null_builder() {
    let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
    let null_bitmap = PoolBuffer::new(pool.clone());
    let data = PoolBuffer::new(pool.clone());

    let mut builder = ArrayBuilder::new(Ty::NA, null_bitmap, data);
    for i in 0..100 {
      builder.append_null();
    }

    assert_eq!(100, builder.len());
    assert_eq!(100, builder.null_count());
    assert_eq!(128, builder.capacity());

    let array = builder.finish().unwrap();

    assert_eq!(&Ty::NA, array.ty());
    assert_eq!(100, array.null_count());
    assert_eq!(100, array.len());
    assert_eq!(0, array.offset());
  }

  #[test]
  fn test_bool_builder() {
    use rand;
    use array::PrimitiveArray;

    let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
    let null_bitmap = PoolBuffer::new(pool.clone());
    let data = PoolBuffer::new(pool.clone());

    let mut builder = ArrayBuilder::new(Ty::Bool, null_bitmap, data);
    let mut expected: Vec<bool> = Vec::new();
    for i in 0..100 {
      let val = rand::random::<bool>();
      builder.append_bool(val);
      expected.push(val);
    }

//    assert_eq!(100, builder.len());
//    assert_eq!(128, builder.capacity());
//    assert_eq!(0, builder.null_count());
//
//    let array = builder.finish().unwrap();
//
//    assert_eq!(&Ty::Bool, array.ty());
//    assert_eq!(100, array.len());
//    assert_eq!(0, array.null_count());
//    assert_eq!(0, array.offset());

//    for i in 0..100 {
//      assert_eq!(expected[i], array.prim_value(i as i64));
//    }
  }
}