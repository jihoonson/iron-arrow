pub mod status;
pub mod ty;
pub mod bit_util;
pub mod field;

use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq)]
pub struct KeyValueMetadata {
  keys: Vec<String>,
  values: Vec<String>
}

impl KeyValueMetadata {
  pub fn new() -> KeyValueMetadata {
    KeyValueMetadata {
      keys: Vec::new(),
      values: Vec::new()
    }
  }

  pub fn with_kvs(keys: Vec<String>, values: Vec<String>) -> KeyValueMetadata {
    KeyValueMetadata {
      keys,
      values
    }
  }

  pub fn append(&mut self, key: String, val: String) {
    self.keys.push(key);
    self.values.push(val);
  }

  pub fn reserve(&mut self, n: i64) {
    if n >= 0 {
      let m = n as usize;
      self.keys.reserve(m);
      self.values.reserve(m);
    } else {
      panic!();
    }
  }

  pub fn key(&self, i: i64) -> &String {
    &self.keys[i as usize]
  }

  pub fn value(&self, i: i64) -> &String {
    &self.values[i as usize]
  }

  pub fn len(&self) -> i64 {
    if self.keys.len() == self.values.len() {
      self.keys.len() as i64
    } else {
      panic!();
    }
  }

  pub fn to_unordered_map(&self) -> HashMap<String, String> {
    let len = self.len() as usize;
    let mut map = HashMap::with_capacity(len);

    for i in 0..len {
      map.insert(self.keys[i].clone(), self.values[i].clone());
    }

    map
  }
}

impl Clone for KeyValueMetadata {
  fn clone(&self) -> Self {
    KeyValueMetadata {
      keys: self.keys.clone(),
      values: self.values.clone()
    }
  }
}

#[cfg(test)]
mod tests {
  use common::status::{StatusCode, ArrowError};
  use common::ty::*;
  use common::field::*;

  #[test]
  fn test_arrow_error() {
    let arrow_error = ArrowError::out_of_memory(String::from("out of memory"));
    assert_eq!(StatusCode::OutOfMemory, *arrow_error.code());
    assert_eq!(String::from("out of memory"), *arrow_error.message());

    let arrow_error = ArrowError::key_error(String::from("key error"));
    assert_eq!(StatusCode::KeyError, *arrow_error.code());
    assert_eq!(String::from("key error"), *arrow_error.message());

    let arrow_error = ArrowError::type_error(String::from("type error"));
    assert_eq!(StatusCode::TypeError, *arrow_error.code());
    assert_eq!(String::from("type error"), *arrow_error.message());

    let arrow_error = ArrowError::invalid(String::from("invalid"));
    assert_eq!(StatusCode::Invalid, *arrow_error.code());
    assert_eq!(String::from("invalid"), *arrow_error.message());

    let arrow_error = ArrowError::io_error(String::from("io error"));
    assert_eq!(StatusCode::IOError, *arrow_error.code());
    assert_eq!(String::from("io error"), *arrow_error.message());

    let arrow_error = ArrowError::unknown_error(String::from("unknown error"));
    assert_eq!(StatusCode::UnknownError, *arrow_error.code());
    assert_eq!(String::from("unknown error"), *arrow_error.message());

    let arrow_error = ArrowError::not_implemented(String::from("not implemented"));
    assert_eq!(StatusCode::NotImplemented, *arrow_error.code());
    assert_eq!(String::from("not implemented"), *arrow_error.message());
  }

  #[test]
  fn test_field() {
    use common::KeyValueMetadata;

    let field = Field::new(String::from("f1"), DataType::null());
    assert_eq!("f1", field.name().as_str());
    assert_eq!(Ty::NA, field.data_type().ty());
    assert_eq!(true, field.nullable());
    assert!(field.metadata().is_none());

    let field = Field::non_null(String::from("f2"), DataType::float());
    assert_eq!("f2", field.name().as_str());
    assert_eq!(Ty::Float, field.data_type().ty());
    assert_eq!(false, field.nullable());
    assert!(field.metadata().is_none());

    let mut metadata = KeyValueMetadata::new();
    metadata.append(String::from("k1"), String::from("v1"));
    metadata.append(String::from("k2"), String::from("v2"));
    metadata.append(String::from("k3"), String::from("v3"));

    let expected_metadata = metadata.clone();

    let field = Field::new_with_metadata(String::from("f3"), DataType::int64(), metadata);
    assert_eq!("f3", field.name().as_str());
    assert_eq!(Ty::Int64, field.data_type().ty());
    assert_eq!(true, field.nullable());
    assert_eq!(&Some(expected_metadata), field.metadata());
  }

  #[test]
  fn test_null() {
    let ty = DataType::null();
    assert_eq!(Ty::NA, ty.ty());
    assert_eq!("null", ty.name());
    assert_eq!(Vec::<BufferDesc>::new(), ty.get_buffer_layout());
  }

  #[test]
  fn test_boolean() {
    let ty = DataType::bool();
    assert_eq!(Ty::Bool, ty.ty());
    assert_eq!("bool", ty.name());
    assert_eq!(
      vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 1)],
      ty.get_buffer_layout()
    );
  }

  macro_rules! test_fixed_width_types {
    ($test_name: ident, $type_name: ident, $str_name: expr, $ty: expr, $width: expr, $buffer_layout: expr) => (
      #[test]
      fn $test_name() {
        let ty = DataType::$type_name();
        assert_eq!($ty, ty.ty());
        assert_eq!($str_name, ty.name());
        assert_eq!($width, ty.bit_width());
        assert_eq!($buffer_layout, ty.get_buffer_layout());
      }
    );
  }

  test_fixed_width_types!(test_uint8_fixed_width, uint8, "uint8", Ty::UInt8, 8, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(8)]);
  test_fixed_width_types!(test_uint16_fixed_width, uint16, "uint16", Ty::UInt16, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_fixed_width_types!(test_uint32_fixed_width, uint32, "uint32", Ty::UInt32, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_fixed_width_types!(test_uint64_fixed_width, uint64, "uint64", Ty::UInt64, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);
  test_fixed_width_types!(test_int8_fixed_width, int8, "int8", Ty::Int8, 8, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(8)]);
  test_fixed_width_types!(test_int16_fixed_width, int16, "int16", Ty::Int16, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_fixed_width_types!(test_int32_fixed_width, int32, "int32", Ty::Int32, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_fixed_width_types!(test_int64_fixed_width, int64, "int64", Ty::Int64, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);

  test_fixed_width_types!(test_half_float_fixed_width, halffloat, "halffloat", Ty::HalfFloat, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_fixed_width_types!(test_float_fixed_width, float, "float", Ty::Float, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_fixed_width_types!(test_double_fixed_width, double, "double", Ty::Double, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);

  test_fixed_width_types!(test_timestamp_fixed_width, timestamp, "timestamp", Ty::Timestamp, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);
  test_fixed_width_types!(test_time32_fixed_width, time32, "time32", Ty::Time32, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_fixed_width_types!(test_time64_fixed_width, time64, "time64", Ty::Time64, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);
  test_fixed_width_types!(test_interval_fixed_width, interval, "interval", Ty::Interval, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);

  #[test]
  fn test_integers_signed() {
    assert!(DataType::int8().is_signed());
    assert!(DataType::int16().is_signed());
    assert!(DataType::int32().is_signed());
    assert!(DataType::int64().is_signed());

    assert!(!DataType::uint8().is_signed());
    assert!(!DataType::uint16().is_signed());
    assert!(!DataType::uint32().is_signed());
    assert!(!DataType::uint64().is_signed());
  }

  #[test]
  fn test_floats() {
    assert_eq!(Precision::Half, DataType::halffloat().precision());
    assert_eq!(Precision::Single, DataType::float().precision());
    assert_eq!(Precision::Double, DataType::double().precision());
  }

  #[test]
  fn test_dates() {
    let ty = DataType::date32();
    assert_eq!(Ty::Date32, ty.ty());
    assert_eq!("date32", ty.name());
    assert_eq!(32, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)], ty.get_buffer_layout());
    assert_eq!(&DateUnit::Milli, ty.date_unit());

    let ty = DataType::date64_with_unit(DateUnit::Day);
    assert_eq!(Ty::Date64, ty.ty());
    assert_eq!("date64", ty.name());
    assert_eq!(64, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)], ty.get_buffer_layout());
    assert_eq!(&DateUnit::Day, ty.date_unit());
  }

  #[test]
  fn test_binary() {
    let ty = DataType::binary();
    assert_eq!(Ty::Binary, ty.ty());
    assert_eq!("binary", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::k_data_buffer(8)], ty.get_buffer_layout());
  }

  #[test]
  fn test_string() {
    let ty = DataType::string();
    assert_eq!(Ty::String, ty.ty());
    assert_eq!("utf8", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::k_data_buffer(8)], ty.get_buffer_layout());
  }

  #[test]
  fn test_decimal() {
    let ty = DataType::decimal(5, 2);
    assert_eq!(Ty::Decimal, ty.ty());
    assert_eq!("decimal", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(128)], ty.get_buffer_layout());
    assert_eq!(5, ty.decimal_precision());
    assert_eq!(2, ty.decimal_scale());
  }

  #[test]
  fn test_list() {
    use std;
    let ty = DataType::list(Box::new(DataType::timestamp()));

    assert_eq!(Ty::List, ty.ty());
    assert_eq!("list", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer()], ty.get_buffer_layout());
    assert_eq!(Ty::Timestamp, ty.list_value_type().ty());

    let timestamp_ty = ty.list_value_type();
    assert_eq!(&DataType::timestamp(), timestamp_ty.as_ref());
  }

  #[test]
  fn test_struct() {
    let ty = DataType::struct_type(
      vec![
        Field::new(String::from("f1"), DataType::date32_with_unit(DateUnit::Day)),
        Field::new(String::from("f2"), DataType::int32())
      ]
    );
    assert_eq!(Ty::Struct, ty.ty());
    assert_eq!("struct", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer()], ty.get_buffer_layout());
    assert_eq!(2, ty.num_children());
    assert_eq!(&Field::new(String::from("f1"), DataType::date32_with_unit(DateUnit::Day)), ty.child(0));
    assert_eq!(&Field::new(String::from("f2"), DataType::int32()), ty.child(1));
  }

  #[test]
  fn test_union() {
    let ty = DataType::union(
      vec![
        Field::new(String::from("f1"), DataType::date32_with_unit(DateUnit::Day)),
        Field::new(String::from("f2"), DataType::int32())
      ],
      vec![0, 1, 2]
    );
    assert_eq!(Ty::Union, ty.ty());
    assert_eq!(&String::from("union"), ty.name());
    assert_eq!(&vec![0, 1, 2], ty.union_type_codes());
    assert_eq!(&UnionMode::SPARSE, ty.union_mode());
    assert_eq!(2, ty.num_children());
    assert_eq!(&Field::new(String::from("f1"), DataType::date32_with_unit(DateUnit::Day)), ty.child(0));
    assert_eq!(&Field::new(String::from("f2"), DataType::int32()), ty.child(1));

    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer()], ty.get_buffer_layout());

    let ty = DataType::union_with_mode(
      vec![
        Field::new(String::from("f1"), DataType::date32_with_unit(DateUnit::Day)),
        Field::new(String::from("f2"), DataType::int32())
      ],
      vec![0, 1, 2],
      UnionMode::DENSE
    );
    assert_eq!(&UnionMode::DENSE, ty.union_mode());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer(), BufferDesc::k_offset_buffer()], ty.get_buffer_layout());
  }

//  #[test]
//  fn test_dictionary() {
//    // TODO
//  }

  #[test]
  fn test_is_integer() {
    assert!(Ty::UInt8.is_integer());
    assert!(Ty::UInt16.is_integer());
    assert!(Ty::UInt32.is_integer());
    assert!(Ty::UInt64.is_integer());
    assert!(Ty::Int8.is_integer());
    assert!(Ty::Int16.is_integer());
    assert!(Ty::Int32.is_integer());
    assert!(Ty::Int64.is_integer());

    assert_eq!(false, Ty::NA.is_integer());
    assert_eq!(false, Ty::Bool.is_integer());
    assert_eq!(false, Ty::HalfFloat.is_integer());
    assert_eq!(false, Ty::Float.is_integer());
    assert_eq!(false, Ty::Double.is_integer());
    assert_eq!(false, Ty::String.is_integer());
    assert_eq!(false, Ty::Binary.is_integer());
    assert_eq!(false, Ty::Date64.is_integer());
    assert_eq!(false, Ty::Date32.is_integer());
    assert_eq!(false, Ty::Timestamp.is_integer());
    assert_eq!(false, Ty::Time32.is_integer());
    assert_eq!(false, Ty::Time64.is_integer());
    assert_eq!(false, Ty::Interval.is_integer());
    assert_eq!(false, Ty::Decimal.is_integer());
    assert_eq!(false, Ty::List.is_integer());
    assert_eq!(false, Ty::Struct.is_integer());
    assert_eq!(false, Ty::Union.is_integer());
    assert_eq!(false, Ty::Dictionary.is_integer());
  }

  #[test]
  fn test_is_float() {
    assert!(Ty::HalfFloat.is_float());
    assert!(Ty::Float.is_float());
    assert!(Ty::Double.is_float());

    assert_eq!(false, Ty::NA.is_float());
    assert_eq!(false, Ty::Bool.is_float());
    assert_eq!(false, Ty::UInt8.is_float());
    assert_eq!(false, Ty::UInt16.is_float());
    assert_eq!(false, Ty::UInt32.is_float());
    assert_eq!(false, Ty::UInt64.is_float());
    assert_eq!(false, Ty::Int8.is_float());
    assert_eq!(false, Ty::Int16.is_float());
    assert_eq!(false, Ty::Int32.is_float());
    assert_eq!(false, Ty::Int64.is_float());
    assert_eq!(false, Ty::String.is_float());
    assert_eq!(false, Ty::Binary.is_float());
    assert_eq!(false, Ty::Date64.is_float());
    assert_eq!(false, Ty::Date32.is_float());
    assert_eq!(false, Ty::Timestamp.is_float());
    assert_eq!(false, Ty::Time32.is_float());
    assert_eq!(false, Ty::Time64.is_float());
    assert_eq!(false, Ty::Interval.is_float());
    assert_eq!(false, Ty::Decimal.is_float());
    assert_eq!(false, Ty::List.is_float());
    assert_eq!(false, Ty::Struct.is_float());
    assert_eq!(false, Ty::Union.is_float());
    assert_eq!(false, Ty::Dictionary.is_float());
  }
}
