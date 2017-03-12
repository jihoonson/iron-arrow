pub mod status;
pub mod ty;

#[cfg(test)]
mod tests {
  use common::status::{StatusCode, ArrowError};
  use common::ty::*;

  #[test]
  fn test_arrow_error() {
    let arrow_error = ArrowError::out_of_memory("out of memory");
    assert_eq!(StatusCode::OutOfMemory, *arrow_error.code());
    assert_eq!(String::from("out of memory"), *arrow_error.message());

    let arrow_error = ArrowError::key_error("key error");
    assert_eq!(StatusCode::KeyError, *arrow_error.code());
    assert_eq!(String::from("key error"), *arrow_error.message());

    let arrow_error = ArrowError::type_error("type error");
    assert_eq!(StatusCode::TypeError, *arrow_error.code());
    assert_eq!(String::from("type error"), *arrow_error.message());

    let arrow_error = ArrowError::invalid("invalid");
    assert_eq!(StatusCode::Invalid, *arrow_error.code());
    assert_eq!(String::from("invalid"), *arrow_error.message());

    let arrow_error = ArrowError::io_error("io error");
    assert_eq!(StatusCode::IOError, *arrow_error.code());
    assert_eq!(String::from("io error"), *arrow_error.message());

    let arrow_error = ArrowError::unknown_error("unknown error");
    assert_eq!(StatusCode::UnknownError, *arrow_error.code());
    assert_eq!(String::from("unknown error"), *arrow_error.message());

    let arrow_error = ArrowError::not_implemented("not implemented");
    assert_eq!(StatusCode::NotImplemented, *arrow_error.code());
    assert_eq!(String::from("not implemented"), *arrow_error.message());
  }

  #[test]
  fn test_field() {
    let field = Field::basic("f1", Ty::Null);
    assert_eq!("f1", field.get_name().as_str());
    assert_eq!(&Ty::Null, field.get_type());
    assert_eq!(&Ty::Null, field.get_type());
    assert_eq!(true, field.is_nullable());
    assert_eq!(0, field.get_dictionary());

    let field = Field::non_null("f2", Ty::Float);
    assert_eq!("f2", field.get_name().as_str());
    assert_eq!(&Ty::Float, field.get_type());
    assert_eq!(false, field.is_nullable());
    assert_eq!(0, field.get_dictionary());

    let field = Field::with_dic("f3", Ty::Int64, -1);
    assert_eq!("f3", field.get_name().as_str());
    assert_eq!(&Ty::Int64, field.get_type());
    assert_eq!(true, field.is_nullable());
    assert_eq!(-1, field.get_dictionary());
  }

  #[test]
  fn test_null_type() {
    let ty = Ty::Null;
    assert_eq!("null", ty.get_name());
    let expected_layout: Vec<&BufferDesc> = Vec::new();
    assert_eq!(expected_layout, ty.get_buffer_layout());
  }

  #[test]
  #[should_panic]
  fn test_bit_width_of_null_type() {
    Ty::Null.bit_width();
  }

  #[test]
  fn test_boolean_type() {
    let ty = Ty::Bool;
    assert_eq!("bool", ty.get_name());
    assert_eq!(vec![K_VALIDITY_BUFFER, K_VALUES_1], ty.get_buffer_layout());
  }

  //  #[test]
  //  fn test_bool() {
  //    assert_eq!(1, RawType::BOOL.bit_width());
  //  }
  //
  //  macro_rules! test_primitive_int_type {
  //    ($test_name: ident, $type_name: ident, $str_name: expr, $raw_type: expr, $rust_type: ty, $buffer_desc: ident, $is_signed: expr) => (
  //      #[test]
  //      fn $test_name() {
  //        use std::mem;
  //
  //        let ty = $type_name::new();
  //        assert_eq!($raw_type, ty.get_type());
  //        assert_eq!($str_name, ty.get_name());
  //        assert_eq!(mem::size_of::<$rust_type>() as i32, ty.get_bit_width());
  //        assert_eq!($is_signed, ty.is_signed());
  //        assert_eq!(vec![K_VALIDITY_BUFFER, $buffer_desc], ty.get_buffer_layout());
  //      }
  //    );
  //  }
  //
  //  macro_rules! test_primitive_float_type {
  //    ($test_name: ident, $type_name: ident, $str_name: expr, $raw_type: expr, $rust_type: ty, $buffer_desc: ident, $precision: expr) => (
  //      #[test]
  //      fn $test_name() {
  //        use std::mem;
  //
  //        let ty = $type_name::new();
  //        assert_eq!($raw_type, ty.get_type());
  //        assert_eq!($str_name, ty.get_name());
  //        assert_eq!(mem::size_of::<$rust_type>() as i32, ty.get_bit_width());
  //        assert_eq!($precision, ty.precision());
  //        assert_eq!(vec![K_VALIDITY_BUFFER, $buffer_desc], ty.get_buffer_layout());
  //      }
  //    );
  //  }
  //
  //  test_primitive_int_type!(test_uint8_type, UInt8Type, "uint8", RawType::UINT8, u8, K_VALUES_8, false);
  //  test_primitive_int_type!(test_uint16_type, UInt16Type, "uint16", RawType::UINT16, u16, K_VALUES_16, false);
  //  test_primitive_int_type!(test_uint32_type, UInt32Type, "uint32", RawType::UINT32, u32, K_VALUES_32, false);
  //  test_primitive_int_type!(test_uint64_type, UInt64Type, "uint64", RawType::UINT64, u64, K_VALUES_64, false);
  //  test_primitive_int_type!(test_int8_type, Int8Type, "int8", RawType::INT8, i8, K_VALUES_8, true);
  //  test_primitive_int_type!(test_int16_type, Int16Type, "int16", RawType::INT16, i16, K_VALUES_16, true);
  //  test_primitive_int_type!(test_int32_type, Int32Type, "int32", RawType::INT32, i32, K_VALUES_32, true);
  //  test_primitive_int_type!(test_int64_type, Int64Type, "int64", RawType::INT64, i64, K_VALUES_64, true);
  //  test_primitive_float_type!(test_half_float_type, HalfFloatType, "halffloat", RawType::HALF_FLOAT, u16, K_VALUES_16, Precision::HALF);
  //  test_primitive_float_type!(test_float_type, FloatType, "float", RawType::FLOAT, f32, K_VALUES_32, Precision::SINGLE);
  //  test_primitive_float_type!(test_double_type, DoubleType, "double", RawType::DOUBLE, f64, K_VALUES_64, Precision::DOUBLE);

  //  #[test]
  //  fn test_list_type() {
  //    let ty = ListType::with_value_type(DoubleType::new());
  //    assert_eq!(RawType::LIST, ty.get_type());
  //    assert_eq!("list", ty.get_name());
  //    assert_eq!(vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER], ty.get_buffer_layout());
  //    assert_eq!(&DoubleType::new(), ty.value_type());
  //    assert_eq!(1, ty.num_children());
  //    assert_eq!(Field::basic("item", DoubleType::new()), ty[0]);
  //    assert_eq!(&Field::basic("item", DoubleType::new()), ty.value_field());
  //    assert_eq!(&vec![Field::basic("item", DoubleType::new())], ty.get_children())
  //  }
  //
  //  #[test]
  //  fn test_binary_type() {
  //    let ty = BinaryType::new();
  //    assert_eq!(RawType::BINARY, ty.get_type());
  //    assert_eq!("binary", ty.get_name());
  //    assert_eq!(vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], ty.get_buffer_layout());
  //  }
  //
  //  #[test]
  //  fn test_string_type() {
  //    let ty = StringType::new();
  //    assert_eq!(RawType::STRING, ty.get_type());
  //    assert_eq!("utf8", ty.get_name());
  //    assert_eq!(vec![K_VALIDITY_BUFFER, K_OFFSET_BUFFER, K_VALUES_8], ty.get_buffer_layout());
  //  }

  //  #[test]
  //  fn test_struct_type() {
  //    let ty = StructType::new(vec![Field::basic("f1", NullType::new()), Field::basic("f2", Int32Type::new())]);
  //    assert_eq!(RawType::STRUCT, ty.get_type());
  //    assert_eq!("struct", ty.get_name());
  //    assert_eq!(vec![K_VALIDITY_BUFFER], ty.get_buffer_layout());
  //    assert_eq!(2, ty.num_children());
  //    assert_eq!(Field::basic("f1", NullType::new()), ty[0]);
  //    assert_eq!(Field::basic("f2", Int32Type::new()), ty[1]);
  //  }

  //  #[test]
  //  fn test_struct_type() {
  //    let ty = RawType::STRUCT { children: vec![Field::basic("f1", RawType::DATE), Field::basic("f2", RawType::DOUBLE)] };
  //  }
}