pub mod status;
pub mod ty;

#[cfg(test)]
mod tests {
  use common::status::{StatusCode, ArrowError};
  use common::ty::*;

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
    let field = Field::basic("f1", DataType::null());
    assert_eq!("f1", field.get_name().as_str());
    assert_eq!(&Ty::Null, field.get_type().as_null_info().get_type());
    assert_eq!(true, field.is_nullable());
    assert_eq!(0, field.get_dictionary());

    let field = Field::non_null("f2", DataType::float());
    assert_eq!("f2", field.get_name().as_str());
    assert_eq!(&Ty::Float, field.get_type().as_float_info().get_type());
    assert_eq!(false, field.is_nullable());
    assert_eq!(0, field.get_dictionary());

    let field = Field::with_dic("f3", DataType::int64(), -1);
    assert_eq!("f3", field.get_name().as_str());
    assert_eq!(&Ty::Int64, field.get_type().as_int64_info().get_type());
    assert_eq!(true, field.is_nullable());
    assert_eq!(-1, field.get_dictionary());
  }

  #[test]
  fn test_null() {
    let ty = DataType::null();
    let info = ty.as_null_info();
    assert_eq!("null", info.get_name());
    let expected_layout: Vec<&BufferDesc> = Vec::new();
    assert_eq!(&expected_layout, info.get_buffer_layout());
  }

  #[test]
  fn test_boolean() {
    let ty = DataType::boolean();
    let info = ty.as_bool_info();
    assert_eq!("bool", info.get_name());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_VALUES_1], info.get_buffer_layout());
  }

  macro_rules! test_primitive {
    ($test_name: ident, $type_name: ident, $type_info: ident, $str_name: expr, $ty: expr, $width: expr, $buffer_layout: expr) => (
      #[test]
      fn $test_name() {
        let ty = DataType::$type_name();
        let info = ty.$type_info();
        assert_eq!(&$ty, info.get_type());
        assert_eq!(&String::from($str_name), info.get_name());
        assert_eq!($width, info.get_bit_width());
        assert_eq!(&$buffer_layout, info.get_buffer_layout());
      }
    );
  }

  macro_rules! test_float {
    ($test_name: ident, $type_name: ident, $type_info: ident, $precision: expr) => (
      #[test]
      fn $test_name() {
        let ty = $DataType::$type_name();
        let info = ty.$type_info();
        assert_eq!($precision, info.precision());
      }
    );
  }

  test_primitive!(test_uint8, uint8, as_uint8_info, "uint8", Ty::UInt8, 8, vec![K_VALIDITY_BUFFER, K_VALUES_8]);
  test_primitive!(test_uint16, uint16, as_uint16_info, "uint16", Ty::UInt16, 16, vec![K_VALIDITY_BUFFER, K_VALUES_16]);
  test_primitive!(test_uint32, uint32, as_uint32_info, "uint32", Ty::UInt32, 32, vec![K_VALIDITY_BUFFER, K_VALUES_32]);
  test_primitive!(test_uint64, uint64, as_uint64_info, "uint64", Ty::UInt64, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);
  test_primitive!(test_int8, int8, as_int8_info, "int8", Ty::Int8, 8, vec![K_VALIDITY_BUFFER, K_VALUES_8]);
  test_primitive!(test_int16, int16, as_int16_info, "int16", Ty::Int16, 16, vec![K_VALIDITY_BUFFER, K_VALUES_16]);
  test_primitive!(test_int32, int32, as_int32_info, "int32", Ty::Int32, 32, vec![K_VALIDITY_BUFFER, K_VALUES_32]);
  test_primitive!(test_int64, int64, as_int64_info, "int64", Ty::Int64, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);

  test_primitive!(test_half_float, half_float, as_half_float_info, "halffloat", Ty::HalfFloat, 16, vec![K_VALIDITY_BUFFER, K_VALUES_16]);
  test_primitive!(test_float, float, as_float_info, "float", Ty::Float, 32, vec![K_VALIDITY_BUFFER, K_VALUES_32]);
  test_primitive!(test_double, double, as_double_info, "double", Ty::Double, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);

  test_primitive!(test_date64, date64, as_date64_info, "date64", Ty::Date64, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);
  test_primitive!(test_date32, date32, as_date32_info, "date32", Ty::Date32, 32, vec![K_VALIDITY_BUFFER, K_VALUES_32]);
  test_primitive!(test_timestamp, timestamp, as_timestamp_info, "timestamp", Ty::Timestamp, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);
  test_primitive!(test_time32, time32, as_time32_info, "time32", Ty::Time32, 32, vec![K_VALIDITY_BUFFER, K_VALUES_32]);
  test_primitive!(test_time64, time64, as_time64_info, "time64", Ty::Time64, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);
  test_primitive!(test_interval, interval, as_interval_info, "interval", Ty::Interval, 64, vec![K_VALIDITY_BUFFER, K_VALUES_64]);

  #[test]
  fn test_integers() {
    assert!(DataType::int8().as_int8_info().is_signed());
    assert!(DataType::int16().as_int16_info().is_signed());
    assert!(DataType::int32().as_int32_info().is_signed());
    assert!(DataType::int64().as_int64_info().is_signed());

    assert!(!DataType::uint8().as_uint8_info().is_signed());
    assert!(!DataType::uint16().as_uint16_info().is_signed());
    assert!(!DataType::uint32().as_uint32_info().is_signed());
    assert!(!DataType::uint64().as_uint64_info().is_signed());
  }

  #[test]
  fn test_floats() {
    assert_eq!(&Precision::Half, DataType::half_float().as_half_float_info().precision());
    assert_eq!(&Precision::Single, DataType::float().as_float_info().precision());
    assert_eq!(&Precision::Double, DataType::double().as_double_info().precision());
  }

  #[test]
  fn test_binary() {
    let ty = DataType::binary();
    let info = ty.as_binary_info();
    assert_eq!(&Ty::Binary, info.get_type());
    assert_eq!("binary", info.get_name());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], info.get_buffer_layout());
  }

  #[test]
  fn test_string_type() {
    let ty = DataType::string();
    let info = ty.as_string_info();
    assert_eq!(&Ty::String, info.get_type());
    assert_eq!("utf8", info.get_name());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], info.get_buffer_layout());
  }

  #[test]
  fn test_decimal() {
    let ty = DataType::decimal(5, 2);
    let info = ty.as_decimal_info();
    assert_eq!(&Ty::Decimal, info.get_type());
    assert_eq!(&String::from("decimal"), info.get_name());
    let expected_layout: Vec<&BufferDesc> = Vec::new();
    assert_eq!(&expected_layout, info.get_buffer_layout());
  }

  #[test]
  fn test_list() {
    let ty = DataType::list_with(Field::basic("f1", DataType::timestamp()));
    let info = ty.as_list_info();
    assert_eq!(&Ty::List, info.get_type());
    assert_eq!(&String::from("list"), info.get_name());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER], info.get_buffer_layout());
    assert_eq!(&DataType::timestamp(), info.value_type());
    assert_eq!(&Field::basic("f1", DataType::timestamp()), info.value_field());
  }

  #[test]
  fn test_struct() {
    let ty = DataType::struc(vec![Field::basic("f1", DataType::date32()), Field::basic("f2", DataType::int32())]);
    let info = ty.as_struct_info();
    assert_eq!(&Ty::Struct, info.get_type());
    assert_eq!(&String::from("struct"), info.get_name());
    assert_eq!(&vec![K_VALIDITY_BUFFER], info.get_buffer_layout());
    assert_eq!(2, info.num_children());
    assert_eq!(&Field::basic("f1", DataType::date32()), info.child(0));
    assert_eq!(&Field::basic("f2", DataType::int32()), info.child(1));
    assert_eq!(&vec![Field::basic("f1", DataType::date32()), Field::basic("f2", DataType::int32())],
     info.get_children());
    assert_eq!(Field::basic("f1", DataType::date32()), info[0]);
    assert_eq!(Field::basic("f2", DataType::int32()), info[1]);
  }

  #[test]
  fn test_union() {
    let ty = DataType::sparse_union(
      vec![Field::basic("f1", DataType::date32()), Field::basic("f2", DataType::int32())],
      vec![0, 1, 2]
    );
    let info = ty.as_union_info();
    assert_eq!(&Ty::Union, info.get_type());
    assert_eq!(&String::from("union"), info.get_name());
    assert_eq!(&vec![0, 1, 2], info.type_codes());
    assert_eq!(2, info.num_children());
    assert_eq!(&Field::basic("f1", DataType::date32()), info.child(0));
    assert_eq!(&Field::basic("f2", DataType::int32()), info.child(1));
    assert_eq!(&vec![Field::basic("f1", DataType::date32()), Field::basic("f2", DataType::int32())],
    info.get_children());
    assert_eq!(Field::basic("f1", DataType::date32()), info[0]);
    assert_eq!(Field::basic("f2", DataType::int32()), info[1]);

    assert_eq!(&UnionMode::SPARSE, info.mode());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER], info.get_buffer_layout());

    let ty = DataType::dense_union(
      vec![Field::basic("f1", DataType::date32()), Field::basic("f2", DataType::int32())],
      vec![0, 1, 2]
    );
    let info = ty.as_union_info();
    assert_eq!(&UnionMode::DENSE, info.mode());
    assert_eq!(&vec![K_VALIDITY_BUFFER, K_TYPE_BUFFER, K_OFFSET_BUFFER], info.get_buffer_layout());
  }

  // TODO: dictionary type test

  #[test]
  fn test_is_integer() {
    assert!(DataType::is_integer(&Ty::UInt8));
    assert!(DataType::is_integer(&Ty::UInt16));
    assert!(DataType::is_integer(&Ty::UInt32));
    assert!(DataType::is_integer(&Ty::UInt64));
    assert!(DataType::is_integer(&Ty::Int8));
    assert!(DataType::is_integer(&Ty::Int16));
    assert!(DataType::is_integer(&Ty::Int32));
    assert!(DataType::is_integer(&Ty::Int64));

    assert_eq!(false, DataType::is_integer(&Ty::Null));
    assert_eq!(false, DataType::is_integer(&Ty::Bool));
    assert_eq!(false, DataType::is_integer(&Ty::HalfFloat));
    assert_eq!(false, DataType::is_integer(&Ty::Float));
    assert_eq!(false, DataType::is_integer(&Ty::Double));
    assert_eq!(false, DataType::is_integer(&Ty::String));
    assert_eq!(false, DataType::is_integer(&Ty::Binary));
    assert_eq!(false, DataType::is_integer(&Ty::Date64));
    assert_eq!(false, DataType::is_integer(&Ty::Date32));
    assert_eq!(false, DataType::is_integer(&Ty::Timestamp));
    assert_eq!(false, DataType::is_integer(&Ty::Time32));
    assert_eq!(false, DataType::is_integer(&Ty::Time64));
    assert_eq!(false, DataType::is_integer(&Ty::Interval));
    assert_eq!(false, DataType::is_integer(&Ty::Decimal));
    assert_eq!(false, DataType::is_integer(&Ty::List));
    assert_eq!(false, DataType::is_integer(&Ty::Struct));
    assert_eq!(false, DataType::is_integer(&Ty::Union));
    assert_eq!(false, DataType::is_integer(&Ty::Dictionary));
  }

  #[test]
  fn test_is_float() {
    assert!(DataType::is_float(&Ty::HalfFloat));
    assert!(DataType::is_float(&Ty::Float));
    assert!(DataType::is_float(&Ty::Double));

    assert_eq!(false, DataType::is_float(&Ty::Null));
    assert_eq!(false, DataType::is_float(&Ty::Bool));
    assert_eq!(false, DataType::is_float(&Ty::UInt8));
    assert_eq!(false, DataType::is_float(&Ty::UInt16));
    assert_eq!(false, DataType::is_float(&Ty::UInt32));
    assert_eq!(false, DataType::is_float(&Ty::UInt64));
    assert_eq!(false, DataType::is_float(&Ty::Int8));
    assert_eq!(false, DataType::is_float(&Ty::Int16));
    assert_eq!(false, DataType::is_float(&Ty::Int32));
    assert_eq!(false, DataType::is_float(&Ty::Int64));
    assert_eq!(false, DataType::is_float(&Ty::String));
    assert_eq!(false, DataType::is_float(&Ty::Binary));
    assert_eq!(false, DataType::is_float(&Ty::Date64));
    assert_eq!(false, DataType::is_float(&Ty::Date32));
    assert_eq!(false, DataType::is_float(&Ty::Timestamp));
    assert_eq!(false, DataType::is_float(&Ty::Time32));
    assert_eq!(false, DataType::is_float(&Ty::Time64));
    assert_eq!(false, DataType::is_float(&Ty::Interval));
    assert_eq!(false, DataType::is_float(&Ty::Decimal));
    assert_eq!(false, DataType::is_float(&Ty::List));
    assert_eq!(false, DataType::is_float(&Ty::Struct));
    assert_eq!(false, DataType::is_float(&Ty::Union));
    assert_eq!(false, DataType::is_float(&Ty::Dictionary));
  }

  #[test]
  fn test_is_primitive() {
    assert!(DataType::is_primitive(&Ty::Null));
    assert!(DataType::is_primitive(&Ty::Bool));
    assert!(DataType::is_primitive(&Ty::UInt8));
    assert!(DataType::is_primitive(&Ty::UInt16));
    assert!(DataType::is_primitive(&Ty::UInt32));
    assert!(DataType::is_primitive(&Ty::UInt64));
    assert!(DataType::is_primitive(&Ty::Int8));
    assert!(DataType::is_primitive(&Ty::Int16));
    assert!(DataType::is_primitive(&Ty::Int32));
    assert!(DataType::is_primitive(&Ty::Int64));
    assert!(DataType::is_primitive(&Ty::HalfFloat));
    assert!(DataType::is_primitive(&Ty::Float));
    assert!(DataType::is_primitive(&Ty::Double));
    assert!(DataType::is_primitive(&Ty::Date64));
    assert!(DataType::is_primitive(&Ty::Date32));
    assert!(DataType::is_primitive(&Ty::Timestamp));
    assert!(DataType::is_primitive(&Ty::Time32));
    assert!(DataType::is_primitive(&Ty::Time64));
    assert!(DataType::is_primitive(&Ty::Interval));

    assert_eq!(false, DataType::is_primitive(&Ty::String));
    assert_eq!(false, DataType::is_primitive(&Ty::Binary));
    assert_eq!(false, DataType::is_primitive(&Ty::Decimal));
    assert_eq!(false, DataType::is_primitive(&Ty::List));
    assert_eq!(false, DataType::is_primitive(&Ty::Struct));
    assert_eq!(false, DataType::is_primitive(&Ty::Union));
    assert_eq!(false, DataType::is_primitive(&Ty::Dictionary));
  }

  #[test]
  fn test_is_binary_like() {
    assert!(DataType::is_binary_like(&Ty::String));
    assert!(DataType::is_binary_like(&Ty::Binary));

    assert_eq!(false, DataType::is_binary_like(&Ty::Null));
    assert_eq!(false, DataType::is_binary_like(&Ty::Bool));
    assert_eq!(false, DataType::is_binary_like(&Ty::UInt8));
    assert_eq!(false, DataType::is_binary_like(&Ty::UInt16));
    assert_eq!(false, DataType::is_binary_like(&Ty::UInt32));
    assert_eq!(false, DataType::is_binary_like(&Ty::UInt64));
    assert_eq!(false, DataType::is_binary_like(&Ty::Int8));
    assert_eq!(false, DataType::is_binary_like(&Ty::Int16));
    assert_eq!(false, DataType::is_binary_like(&Ty::Int32));
    assert_eq!(false, DataType::is_binary_like(&Ty::Int64));
    assert_eq!(false, DataType::is_binary_like(&Ty::HalfFloat));
    assert_eq!(false, DataType::is_binary_like(&Ty::Float));
    assert_eq!(false, DataType::is_binary_like(&Ty::Double));
    assert_eq!(false, DataType::is_binary_like(&Ty::Date64));
    assert_eq!(false, DataType::is_binary_like(&Ty::Date32));
    assert_eq!(false, DataType::is_binary_like(&Ty::Timestamp));
    assert_eq!(false, DataType::is_binary_like(&Ty::Time32));
    assert_eq!(false, DataType::is_binary_like(&Ty::Time64));
    assert_eq!(false, DataType::is_binary_like(&Ty::Interval));
    assert_eq!(false, DataType::is_binary_like(&Ty::Decimal));
    assert_eq!(false, DataType::is_binary_like(&Ty::List));
    assert_eq!(false, DataType::is_binary_like(&Ty::Struct));
    assert_eq!(false, DataType::is_binary_like(&Ty::Union));
    assert_eq!(false, DataType::is_binary_like(&Ty::Dictionary));
  }
}