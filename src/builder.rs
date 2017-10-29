use buffer::ArrayData;
use buffer::PoolBuffer;
use array::Array;
use common::status::ArrowError;
use ty::DataType;

pub trait ArrayBuilder<T> {
  fn child(&self, i: i32) -> &ArrayBuilder<T>;
  fn num_children(&self) -> i32;

  fn ty(&self) -> Box<DataType>;
  fn length(&self) -> i64;
  fn null_count(&self) -> i64;
  fn capacity(&self) -> i64;

  fn null_bitmap(&self) -> Box<PoolBuffer>;

  fn append_to_bitmap(&self, is_valid: bool) -> Result<&ArrayBuilder<T>, ArrowError>;
  fn append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn set_not_null(&self, length: i64) -> Result<&ArrowBuilder<T>, ArrowError>;

  fn init(&self, capacity: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn resize(&self, new_bits: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn reserve(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn advance(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError>;
  fn finish(&self) -> Result<Box<Array>, ArrowError>;

  fn reset(&self);

  unsafe fn unsafe_append_to_bitmap(&self, is_valid: bool);
  unsafe fn unsafe_append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64);
  unsafe fn unsafe_set_not_null(&self, length: i64);
}

pub struct NullBuilder {
  null_count: i64,
  length: i64
}

impl NullBuilder {
  pub fn new(pool: Box<MemoryPool>) -> NullBuilder {
    NullBuilder {
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
    unimplemented!()
  }

  fn length(&self) -> i64 {
    unimplemented!()
  }

  fn null_count(&self) -> i64 {
    unimplemented!()
  }

  fn capacity(&self) -> i64 {
    unimplemented!()
  }

  fn null_bitmap(&self) -> Box<PoolBuffer> {
    unimplemented!()
  }

  fn append_to_bitmap(&self, is_valid: bool) -> Result<&ArrayBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn set_not_null(&self, length: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn init(&self, capacity: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn resize(&self, new_bits: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn reserve(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn advance(&self, elements: i64) -> Result<&ArrowBuilder<T>, ArrowError> {
    unimplemented!()
  }

  fn finish(&self) -> Result<Box<Array>, ArrowError> {
    unimplemented!()
  }

  fn reset(&self) {
    unimplemented!()
  }

  fn unsafe_append_to_bitmap(&self, is_valid: bool) {
    unimplemented!()
  }

  fn unsafe_append_vector_to_bitmap(&self, valid_bytes: *const u8, length: i64) {
    unimplemented!()
  }

  fn unsafe_set_not_null(&self, length: i64) {
    unimplemented!()
  }
}