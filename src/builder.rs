use buffer::ArrayData;
use buffer::PoolBuffer;
use array::Array;
use common::status::ArrowError;
use ty::DataType;
use common::bit_util;

pub trait ArrayBuilder<T> {
  fn child(&self, i: i32) -> &ArrayBuilder<T>;
  fn num_children(&self) -> i32;

  fn ty(&self) -> Box<DataType>;
  fn length(&self) -> i64;
  fn null_count(&self) -> i64;
  fn capacity(&self) -> i64;

  fn null_bitmap(&self) -> Box<PoolBuffer>;

  fn init(&self, capacity: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn resize(&self, new_bits: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn reserve(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn advance(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn finish(&self) -> Result<Box<Array>, ArrowError>;

  fn reset(&self);

  unsafe fn append_to_bitmap(&self, is_valid: bool);
  unsafe fn append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64);
  unsafe fn set_not_null(&self, length: i64);
}

pub fn append_to_bitmap(builder: &mut ArrayBuilder<T>, is_valid: bool) -> Result<&ArrayBuilder<T>, ArrowError> {
  if builder.length() == builder.capacity() {
    let resize_result = builder.resize(bit_util::next_power_2(builder.capacity() + 1));
    if resize_result.is_err() {
      return resize_result;
    }
  }
  unsafe { builder.append_to_bitmap(is_valid); }
  Ok(builder)
}

pub fn append_vector_to_bitmap(builder: &mut ArrayBuilder<T>, valid_bytes: *const u8, length: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
  Ok(builder)
}

pub fn set_not_null(builder: &mut ArrayBuilder<T>, length: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
  Ok(builder)
}

pub struct NullBuilder {
  ty: DataType,
  null_count: i64,
  length: i64
}

impl NullBuilder {
  pub fn new(pool: Box<MemoryPool>) -> NullBuilder {
    NullBuilder {
      ty: DataType::null(),
      null_count: 0,
      length: 0
    }
  }

  pub fn append_null(&mut self) -> Result<&NullBuilder, ArrowError> {
    self.null_count = self.null_count + 1;
    self.length = self.length + 1;
    Ok(self)
  }
}

impl ArrayBuilder<u8> for NullBuilder {
  fn child(&self, i: i32) -> &ArrayBuilder<u8> {
    unimplemented!()
  }

  fn num_children(&self) -> i32 {
    unimplemented!()
  }

  fn ty(&self) -> Box<DataType> {
    self.ty
  }

  fn length(&self) -> i64 {
    self.length
  }

  fn null_count(&self) -> i64 {
    self.null_count
  }

  fn capacity(&self) -> i64 {
    self.length
  }

  fn null_bitmap(&self) -> Box<PoolBuffer> {
    unimplemented!()
  }

  fn init(&self, capacity: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    Ok(self)
  }

  fn resize(&self, new_bits: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    Ok(self)
  }

  fn reserve(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    Ok(self)
  }

  fn advance(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    Ok(self)
  }

  fn finish(&self) -> Result<Box<Array>, ArrowError> {
    unimplemented!()
  }

  fn reset(&self) {

  }

  fn append_to_bitmap(&self, is_valid: bool) {
    unimplemented!()
  }

  fn append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64) {
    unimplemented!()
  }

  fn set_not_null(&self, length: i64) {
    unimplemented!()
  }
}