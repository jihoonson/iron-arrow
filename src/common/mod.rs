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

    let field = Field::new(String::from("f1"), Ty::null());
    assert_eq!("f1", field.name().as_str());
    assert_eq!(&Ty::NA, field.data_type());
    assert_eq!(true, field.nullable());
    assert!(field.metadata().is_none());

    let field = Field::non_null(String::from("f2"), Ty::float());
    assert_eq!("f2", field.name().as_str());
    assert_eq!(&Ty::Float, field.data_type());
    assert_eq!(false, field.nullable());
    assert!(field.metadata().is_none());

    let mut metadata = KeyValueMetadata::new();
    metadata.append(String::from("k1"), String::from("v1"));
    metadata.append(String::from("k2"), String::from("v2"));
    metadata.append(String::from("k3"), String::from("v3"));

    let expected_metadata = metadata.clone();

    let field = Field::new_with_metadata(String::from("f3"), Ty::int64(), metadata);
    assert_eq!("f3", field.name().as_str());
    assert_eq!(&Ty::Int64, field.data_type());
    assert_eq!(true, field.nullable());
    assert_eq!(&Some(expected_metadata), field.metadata());
  }

  #[test]
  fn test_null() {
    let ty = Ty::null();
    assert_eq!(Ty::NA, ty);
    assert_eq!("null", ty.name());
    assert_eq!(Vec::<BufferDesc>::new(), ty.get_buffer_layout());
  }

  #[test]
  fn test_boolean() {
    let ty = Ty::bool();
    assert_eq!(Ty::Bool, ty);
    assert_eq!("bool", ty.name());
    assert_eq!(
      vec![BufferDesc::k_validity_buffer(), BufferDesc::new(BufferType::Data, 1)],
      ty.get_buffer_layout()
    );
  }

  macro_rules! test_primitive_types {
    ($test_name: ident, $type_name: ident, $str_name: expr, $ty: path, $width: expr, $buffer_layout: expr) => (
      #[test]
      fn $test_name() {
        let ty = Ty::$type_name();
        assert_eq!($ty, ty);
        assert_eq!($str_name, ty.name());
        assert_eq!($width, ty.bit_width());
        assert_eq!($buffer_layout, ty.get_buffer_layout());
      }
    );
  }

  test_primitive_types!(test_uint8_fixed_width, uint8, "uint8", Ty::UInt8, 8, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(8)]);
  test_primitive_types!(test_uint16_fixed_width, uint16, "uint16", Ty::UInt16, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_primitive_types!(test_uint32_fixed_width, uint32, "uint32", Ty::UInt32, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_primitive_types!(test_uint64_fixed_width, uint64, "uint64", Ty::UInt64, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);
  test_primitive_types!(test_int8_fixed_width, int8, "int8", Ty::Int8, 8, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(8)]);
  test_primitive_types!(test_int16_fixed_width, int16, "int16", Ty::Int16, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_primitive_types!(test_int32_fixed_width, int32, "int32", Ty::Int32, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_primitive_types!(test_int64_fixed_width, int64, "int64", Ty::Int64, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);

  test_primitive_types!(test_half_float_fixed_width, halffloat, "halffloat", Ty::HalfFloat, 16, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(16)]);
  test_primitive_types!(test_float_fixed_width, float, "float", Ty::Float, 32, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)]);
  test_primitive_types!(test_double_fixed_width, double, "double", Ty::Double, 64, vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)]);

  #[test]
  fn test_integers_signed() {
    assert!(Ty::int8().is_signed());
    assert!(Ty::int16().is_signed());
    assert!(Ty::int32().is_signed());
    assert!(Ty::int64().is_signed());

    assert!(!Ty::uint8().is_signed());
    assert!(!Ty::uint16().is_signed());
    assert!(!Ty::uint32().is_signed());
    assert!(!Ty::uint64().is_signed());
  }

  #[test]
  fn test_floats() {
    assert_eq!(Precision::Half, Ty::halffloat().precision());
    assert_eq!(Precision::Single, Ty::float().precision());
    assert_eq!(Precision::Double, Ty::double().precision());
  }

  #[test]
  fn test_timestamp() {
    let ty = Ty::timestamp();
    assert_eq!(Ty::Timestamp { unit: TimeUnit::Milli, timezone: String::new() }, ty);
    assert_eq!("timestamp", ty.name());
    assert_eq!(64, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)], ty.get_buffer_layout());
    assert_eq!(&TimeUnit::Milli, ty.time_unit());
  }

  #[test]
  fn test_time() {
    let ty = Ty::time64();
    assert_eq!(Ty::Time64 { unit: TimeUnit::Milli }, ty);
    assert_eq!("time64", ty.name());
    assert_eq!(64, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)], ty.get_buffer_layout());
    assert_eq!(&TimeUnit::Milli, ty.time_unit());

    let ty = Ty::time32();
    assert_eq!(Ty::Time32 { unit: TimeUnit::Milli }, ty);
    assert_eq!("time32", ty.name());
    assert_eq!(32, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)], ty.get_buffer_layout());
    assert_eq!(&TimeUnit::Milli, ty.time_unit());
  }

  #[test]
  fn test_interval() {
    let ty = Ty::interval();
    assert_eq!(Ty::Interval { unit: IntervalUnit::YearMonth }, ty);
    assert_eq!("interval", ty.name());
    assert_eq!(64, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)], ty.get_buffer_layout());
    assert_eq!(&IntervalUnit::YearMonth, ty.interval_unit());
  }

  #[test]
  fn test_date() {
    let ty = Ty::date32();
    assert_eq!(Ty::Date32 { unit: DateUnit::Milli }, ty);
    assert_eq!("date32", ty.name());
    assert_eq!(32, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(32)], ty.get_buffer_layout());
    assert_eq!(&DateUnit::Milli, ty.date_unit());

    let ty = Ty::date64_with_unit(DateUnit::Day);
    assert_eq!(Ty::Date64 { unit: DateUnit::Day }, ty);
    assert_eq!("date64", ty.name());
    assert_eq!(64, ty.bit_width());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(64)], ty.get_buffer_layout());
    assert_eq!(&DateUnit::Day, ty.date_unit());
  }

  #[test]
  fn test_binary() {
    let ty = Ty::binary();
    assert_eq!(Ty::Binary, ty);
    assert_eq!("binary", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::k_data_buffer(8)], ty.get_buffer_layout());
  }

  #[test]
  fn test_string() {
    let ty = Ty::string();
    assert_eq!(Ty::String, ty);
    assert_eq!("utf8", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer(), BufferDesc::k_data_buffer(8)], ty.get_buffer_layout());
  }

  #[test]
  fn test_decimal() {
    let ty = Ty::decimal(5, 2);
    assert_eq!(Ty::Decimal { precision: 5, scale: 2 }, ty);
    assert_eq!("decimal", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_data_buffer(128)], ty.get_buffer_layout());
    assert_eq!(5, ty.decimal_precision());
    assert_eq!(2, ty.decimal_scale());
  }

  #[test]
  fn test_list() {
    use std;
    let ty = Ty::list(Box::new(Ty::timestamp()));

    assert_eq!(Ty::List { value_type: Box::new(Ty::timestamp()) }, ty);
    assert_eq!("list", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_offset_buffer()], ty.get_buffer_layout());
    assert_eq!(&Box::new(Ty::timestamp()), ty.list_value_type());

    let timestamp_ty = ty.list_value_type();
    assert_eq!(&Ty::timestamp(), timestamp_ty.as_ref());
  }

  #[test]
  fn test_struct() {
    let fields = vec![
      Field::new(String::from("f1"), Ty::date32_with_unit(DateUnit::Day)),
      Field::new(String::from("f2"), Ty::int32())
    ];
    let ty = Ty::struct_type(
      fields.clone()
    );
    assert_eq!(Ty::Struct { fields: fields.clone() }, ty);
    assert_eq!("struct", ty.name());
    assert_eq!(vec![BufferDesc::k_validity_buffer()], ty.get_buffer_layout());
    assert_eq!(2, ty.num_children());
    assert_eq!(&Field::new(String::from("f1"), Ty::date32_with_unit(DateUnit::Day)), ty.child(0));
    assert_eq!(&Field::new(String::from("f2"), Ty::int32()), ty.child(1));
  }

  #[test]
  fn test_union() {
    let fields = vec![
      Field::new(String::from("f1"), Ty::date32_with_unit(DateUnit::Day)),
      Field::new(String::from("f2"), Ty::int32())
    ];
    let type_codes = vec![0, 1, 2];
    let ty = Ty::union(
      fields.clone(),
      type_codes.clone()
    );
    assert_eq!(Ty::Union { fields: fields.clone(), type_codes: type_codes.clone(), mode: UnionMode::SPARSE }, ty);
    assert_eq!(&String::from("union"), ty.name());
    assert_eq!(&vec![0, 1, 2], ty.union_type_codes());
    assert_eq!(&UnionMode::SPARSE, ty.union_mode());
    assert_eq!(2, ty.num_children());
    assert_eq!(&Field::new(String::from("f1"), Ty::date32_with_unit(DateUnit::Day)), ty.child(0));
    assert_eq!(&Field::new(String::from("f2"), Ty::int32()), ty.child(1));

    assert_eq!(vec![BufferDesc::k_validity_buffer(), BufferDesc::k_type_buffer()], ty.get_buffer_layout());

    let ty = Ty::union_with_mode(
      vec![
        Field::new(String::from("f1"), Ty::date32_with_unit(DateUnit::Day)),
        Field::new(String::from("f2"), Ty::int32())
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
    use array::Array;

    assert!(Ty::uint8().is_integer());
    assert!(Ty::uint16().is_integer());
    assert!(Ty::uint32().is_integer());
    assert!(Ty::uint64().is_integer());
    assert!(Ty::int8().is_integer());
    assert!(Ty::int16().is_integer());
    assert!(Ty::int32().is_integer());
    assert!(Ty::int64().is_integer());

    assert_eq!(false, Ty::null().is_integer());
    assert_eq!(false, Ty::bool().is_integer());
    assert_eq!(false, Ty::halffloat().is_integer());
    assert_eq!(false, Ty::float().is_integer());
    assert_eq!(false, Ty::double().is_integer());
    assert_eq!(false, Ty::string().is_integer());
    assert_eq!(false, Ty::binary().is_integer());
    assert_eq!(false, Ty::date64().is_integer());
    assert_eq!(false, Ty::date32().is_integer());
    assert_eq!(false, Ty::timestamp().is_integer());
    assert_eq!(false, Ty::time32().is_integer());
    assert_eq!(false, Ty::time64().is_integer());
    assert_eq!(false, Ty::interval().is_integer());
    assert_eq!(false, Ty::decimal(5, 2).is_integer());
    assert_eq!(false, Ty::list(Box::new(Ty::int8())).is_integer());
    assert_eq!(false, Ty::struct_type(vec![Field::new(String::from("f1"), Ty::int8())]).is_integer());
    assert_eq!(false, Ty::union(vec![Field::new(String::from("f1"), Ty::int8())], vec![0]).is_integer());
    assert_eq!(false, Ty::dictionary(Box::new(Ty::int8()), Box::new(Array::null(10, 0))).is_integer());
  }

  #[test]
  fn test_is_float() {
    use array::Array;

    assert!(Ty::halffloat().is_float());
    assert!(Ty::float().is_float());
    assert!(Ty::double().is_float());

    assert_eq!(false, Ty::null().is_float());
    assert_eq!(false, Ty::bool().is_float());
    assert_eq!(false, Ty::uint8().is_float());
    assert_eq!(false, Ty::uint16().is_float());
    assert_eq!(false, Ty::uint32().is_float());
    assert_eq!(false, Ty::uint64().is_float());
    assert_eq!(false, Ty::int8().is_float());
    assert_eq!(false, Ty::int16().is_float());
    assert_eq!(false, Ty::int32().is_float());
    assert_eq!(false, Ty::int64().is_float());
    assert_eq!(false, Ty::string().is_float());
    assert_eq!(false, Ty::binary().is_float());
    assert_eq!(false, Ty::date64().is_float());
    assert_eq!(false, Ty::date32().is_float());
    assert_eq!(false, Ty::timestamp().is_float());
    assert_eq!(false, Ty::time32().is_float());
    assert_eq!(false, Ty::time64().is_float());
    assert_eq!(false, Ty::interval().is_float());
    assert_eq!(false, Ty::decimal(5, 2).is_float());
    assert_eq!(false, Ty::list(Box::new(Ty::int8())).is_float());
    assert_eq!(false, Ty::struct_type(vec![Field::new(String::from("f1"), Ty::int8())]).is_float());
    assert_eq!(false, Ty::union(vec![Field::new(String::from("f1"), Ty::int8())], vec![0]).is_float());
    assert_eq!(false, Ty::dictionary(Box::new(Ty::int8()), Box::new(Array::null(10, 0))).is_float());
  }
}
