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

// TODO: make ArrayData and ues different interfaces for building an array and reading from it
#[derive(Eq, PartialEq)]
pub struct ArrayBuilder<'a> {
  init: bool,
  ty: Ty<'a>,
  null_count: i64,
  length: i64,
  capacity: i64,
  data: BuilderData
}

impl <'a> ArrayBuilder<'a> {
  pub fn null(len: i64) -> ArrayBuilder<'a> {
    ArrayBuilder {
      init: true,
      ty: Ty::NA,
      null_count: 0,
      length: len,
      capacity: 0,
      data: BuilderData::Null
    }
  }

  pub fn new(ty: Ty<'a>, null_bitmap: PoolBuffer, data: PoolBuffer) -> ArrayBuilder<'a> {
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
      init: false,
      ty,
      null_count: 0,
      length: 0,
      capacity: 0,
      data: builder_data,
    }
  }

  pub fn ty(&self) -> &Ty {
    &self.ty
  }

  pub fn null_bitmap(&self) ->Option<&PoolBuffer> {
    self.data.null_bitmap()
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

  pub fn data(&self) -> &BuilderData {
    &self.data
  }

  fn init(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match self.data.init(capacity) {
      Ok(_) => {
        self.init = true;
        Ok(())
      },
      Err(e) => Err(e)
    }
  }

  pub fn resize(&mut self, req_capacity: i64) -> Result<(), ArrowError> {
    let new_capacity = ArrayBuilder::get_capacity_for_type(self.ty(), req_capacity);

    if !self.init {
      return self.init(new_capacity);
    }

    match self.data.resize(new_capacity) {
      Ok(_) => {
        self.capacity = new_capacity;
        Ok(())
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
        match self.reserve(1) {
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

  pub fn append_bool(&mut self, val: bool) -> Result<(), ArrowError> {
    match self.reserve(1) {
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

  pub fn append_int8(&mut self, val: i8) -> Result<(), ArrowError> {
    match self.reserve(1) {
      Ok(_) => {
        match self.data {
          BuilderData::Int8 { ref mut null_bitmap, ref mut data } => {
            bit_util::set_bit(null_bitmap.data_as_mut(), self.length);
            unsafe { *(mem::transmute::<*mut u8, *mut i8>(data.data_as_mut()).offset(self.length as isize)) = val }
            self.length = self.length + 1;
            Ok(())
          },
          _ => panic!()
        }
      },
      Err(e) => Err(e)
    }
  }

  pub fn append_uint8(&mut self, val: u8) -> Result<(), ArrowError> {
    match self.reserve(1) {
      Ok(_) => {
        match self.data {
          BuilderData::UInt8 { ref mut null_bitmap, ref mut data } => {
            bit_util::set_bit(null_bitmap.data_as_mut(), self.length);
            unsafe { *data.data_as_mut().offset(self.length as isize) = val }
            self.length = self.length + 1;
            Ok(())
          },
          _ => panic!()
        }
      },
      Err(e) => Err(e)
    }
  }

//  pub fn build(&self) -> Result<Array, ArrowError> {
//    let array = match self.data {
//      BuilderData::Null => Array::null(self.length, 0),
//
//      // primitive types
//      // TODO: null_bitmap should be able to be passed without clone because PoolBuffer.clone() is deep copy
//      BuilderData::Bool            { ref data } |
//      BuilderData::Int8            { ref data } |
//      BuilderData::Int16           { ref data } |
//      BuilderData::Int32           { ref data } |
//      BuilderData::Int64           { ref data } |
//      BuilderData::UInt16          { ref data } |
//      BuilderData::UInt32          { ref data } |
//      BuilderData::UInt64          { ref data } |
//      BuilderData::HalfFloat       { ref data } |
//      BuilderData::Float           { ref data } |
//      BuilderData::Double          { ref data } |
//      BuilderData::Date64          { ref data } |
//      BuilderData::Date32          { ref data } |
//      BuilderData::Time64          { ref data } |
//      BuilderData::Time32          { ref data } |
//      BuilderData::Timestamp       { ref data } |
//      BuilderData::Interval        { ref data } |
//      // fixed size binary types
//      BuilderData::FixedSizeBinary { ref data } |
//      BuilderData::Decimal         { ref data } => Array::fixed_width(
//        self.ty.clone(),
//        self.length,
//        0,
//        Some(self.null_bitmap.clone()),
//        data
//      ),
//
//      BuilderData::UInt8           { ref data, ref slice } => Array::fixed_width(
//        self.ty.clone(),
//        self.length,
//        0,
//        Some(self.null_bitmap.clone()),
//        data
//      ),
//
//      _ => panic!()
//    };
//
//    Ok(array)
//  }
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
  fn new(ty: Ty, null_bitmap: PoolBuffer, data: PoolBuffer) -> BuilderData {
    match ty {
      Ty::NA => BuilderData::Null,
      Ty::Bool => BuilderData::Bool { null_bitmap, data },
      Ty::Int8 => BuilderData::Int8 { null_bitmap, data },
      Ty::UInt8 => BuilderData::UInt8 { null_bitmap, data },
      _ => panic!()
    }
  }

  fn init(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Null => Ok(()),
      &mut BuilderData::Bool { ref mut null_bitmap, ref mut data } => {
        match init_buffer(null_bitmap, capacity) {
          Ok(_) => init_buffer(data, capacity),
          Err(e) => Err(e)
        }
      },
      &mut BuilderData::Int8 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::UInt8 { ref mut null_bitmap, ref mut data } => {
        match init_buffer(null_bitmap, capacity) {
          Ok(_) => init_buffer(data, capacity * 8),
          Err(e) => Err(e)
        }
      },
      &mut BuilderData::Int16 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::UInt16 { ref mut null_bitmap, ref mut data } => {
        match init_buffer(null_bitmap, capacity) {
          Ok(_) => init_buffer(data, capacity * 16),
          Err(e) => Err(e)
        }
      },
      _ => panic!()
    }
  }

  fn resize(&mut self, capacity: i64) -> Result<(), ArrowError> {
    match self {
      &mut BuilderData::Null => Ok(()),
      &mut BuilderData::Bool { ref mut null_bitmap, ref mut data } => {
        match resize_buffer(null_bitmap, capacity) {
          Ok(_) => resize_buffer(data, capacity),
          Err(e) => Err(e)
        }
      },
      &mut BuilderData::Int8 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::UInt8 { ref mut null_bitmap, ref mut data } => {
        match resize_buffer(null_bitmap, capacity) {
          Ok(_) => resize_buffer(data, capacity * 8),
          Err(e) => Err(e)
        }
      },
      &mut BuilderData::Int16 { ref mut null_bitmap, ref mut data } |
      &mut BuilderData::UInt16 { ref mut null_bitmap, ref mut data } => {
        match resize_buffer(null_bitmap, capacity) {
          Ok(_) => resize_buffer(data, capacity * 16),
          Err(e) => Err(e)
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
      &BuilderData::UInt16 { ref null_bitmap, ref data } => Some(null_bitmap),
      _ => None
    }
  }

  fn element_bit_len(&self) -> usize {
    match self {
      &BuilderData::Null => 0,
      &BuilderData::Int8 { ref null_bitmap, ref data } |
      &BuilderData::UInt8 { ref null_bitmap, ref data } => 8,
      &BuilderData::Int16 { ref null_bitmap, ref data } |
      &BuilderData::UInt16 { ref null_bitmap, ref data } => 16,
      _ => panic!()
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
  use builder::ArrayBuilder;
  use array::Array;

  #[test]
  fn test_null_builder() {
    let mut builder = ArrayBuilder::null(0);
    for i in 0..100 {
      builder.append_null();
    }

    assert_eq!(100, builder.len());
    assert_eq!(100, builder.null_count());
    assert_eq!(128, builder.capacity());

    let array = Array::new(builder);

    assert_eq!(&Ty::NA, array.ty());
    assert_eq!(100, array.null_count());
    assert_eq!(100, array.len());
//    assert_eq!(0, array.offset());
  }

  #[test]
  fn test_bool_builder() {
    use rand;
    use array::BooleanArray;

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

    assert_eq!(100, builder.len());
    assert_eq!(32, builder.capacity());
    assert_eq!(0, builder.null_count());

    let array = Array::new(builder);

    assert_eq!(&Ty::Bool, array.ty());
    assert_eq!(100, array.len());
    assert_eq!(0, array.null_count());
//    assert_eq!(0, array.offset());

    for i in 0..100 {
      assert_eq!(expected[i], array.bool_value(i as i64));
    }
  }

  // TODO: test boolean with null

  macro_rules! test_primitive_type_builder {
      ($test_name: ident, $ty: path, $prim_ty: ty, $append_method: ident, $expected_capacity: expr) => {
        #[test]
        fn $test_name() {
          use rand;
          use array::PrimitiveArray;

          let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
          let null_bitmap = PoolBuffer::new(pool.clone());
          let data = PoolBuffer::new(pool.clone());

          let mut builder = ArrayBuilder::new($ty, null_bitmap, data);
          let mut expected: Vec<$prim_ty> = Vec::new();
          for i in 0..100 {
            let val = rand::random::<$prim_ty>();
            builder.$append_method(val);
            expected.push(val);
          }

          assert_eq!(100, builder.len());
          assert_eq!($expected_capacity, builder.capacity());
          assert_eq!(0, builder.null_count());

          let array = Array::new(builder);

          assert_eq!(&$ty, array.ty());
          assert_eq!(100, array.len());
          assert_eq!(0, array.null_count());
          assert_eq!(0, array.offset());

          for i in 0..100 {
            assert_eq!(expected[i], array.prim_value(i as i64));
          }
        }
      };
  }

//  test_primitive_type_builder!(test_int8_builder, Ty::Int8, i8, append_int8, 128);
//  test_primitive_type_builder!(test_uint8_builder, Ty::UInt8, u8, append_uint8, 128);


  #[test]
  fn test_u8_builder() {
    use rand;
    use array::UInt8Array;

    let pool = Arc::new(RefCell::new(DefaultMemoryPool::new()));
    let null_bitmap = PoolBuffer::new(pool.clone());
    let data = PoolBuffer::new(pool.clone());

    let mut builder = ArrayBuilder::new(Ty::UInt8, null_bitmap, data);
    let mut expected: Vec<u8> = Vec::new();
    for i in 0..100 {
      let val = rand::random::<u8>();
      builder.append_uint8(val);
      expected.push(val);
    }

    assert_eq!(100, builder.len());
    assert_eq!(128, builder.capacity());
    assert_eq!(0, builder.null_count());

    let array = Array::new(builder);

    assert_eq!(&Ty::UInt8, array.ty());
    assert_eq!(100, array.len());
    assert_eq!(0, array.null_count());
//    assert_eq!(0, array.offset());

    for i in 0..100 {
      assert_eq!(expected[i], array.u8_value(i as i64));
    }
  }
}
