use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};

use std::ptr;
use std::mem;

use std::fmt::{Debug, Formatter, Error};

#[derive(Eq, PartialEq)]
pub struct Array {
  ty: Ty,
  length: i64,
  offset: i64,
  null_count: i64,
  null_bitmap: Option<PoolBuffer>,
  data: ArrayData
}

pub enum ArrayData {
  Null,
  Bool {
    values: Vec<bool>
  },

  UInt8 {
    values: Vec<u8>
  },
  Int8 {
    values: Vec<i8>
  },
  UInt16 {
    values: Vec<u16>
  },
  Int16 {
    values: Vec<i16>
  },
  UInt32 {
    values: Vec<u32>
  },
  Int32 {
    values: Vec<i32>
  },
  UInt64 {
    values: Vec<u64>
  },
  Int64 {
    values: Vec<i64>
  },

  HalfFloat {
    values: Vec<u16>
  },
  Float {
    values: Vec<f32>
  },
  Double {
    values: Vec<f64>
  },

  Binary {
    value_offsets: *const i32, // TODO => maybe Vec<i32>,
    values: *const u8
  },
  String {
    value_offsets: *const i32,
    values: *const u8
  },
  FixedSizeBinary {
    values: *const u8
  },

  Date64 {
    values: Vec<i64>
  },
  Date32 {
    values: Vec<i32>
  },
  Timestamp {
    values: Vec<i64>
  },
  Time32 {
    values: Vec<i32>
  },
  Time64 {
    values: Vec<i64>
  },
  Interval {
    values: Vec<i64>
  },

  Decimal {
    values: *const u8
  },

  List {
    value_offsets: *const i32,
    value_array: Box<Array>
  },
  Struct {
    fields: Vec<Box<Array>>
  },
  Union {
    fields: Vec<Box<Array>>,
    value_offsets: *const i32
  },

  Dictionary {
    indices: Box<Array>
  }
}

impl PartialEq for ArrayData {
  fn eq(&self, other: &Self) -> bool {
    // TODO
    unimplemented!()
  }
}

impl Eq for ArrayData {

}

impl Array {
  #[inline]
  fn compute_null_count(null_bitmap: &Option<PoolBuffer>, offset: i64, length: i64) -> i64 {
    match null_bitmap {
      &Some(ref buffer) => {
        let null_bitmap_data = buffer.data();
        if !null_bitmap_data.is_null() {
          length - bit_util::count_set_bits(null_bitmap_data, offset, length)
        } else {
          0
        }
      },
      &None => 0
    }
  }

  pub fn primitive(ty: Ty, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: &PoolBuffer) -> Array {
    Array::fixed_width(ty, length, offset, null_bitmap, values)
  }

  pub fn null(length: i64, offset: i64) -> Array {
    Array {
      ty: Ty::null(),
      length,
      offset,
      null_count: length,
      null_bitmap: None,
      data: ArrayData::Null
    }
  }

  pub fn fixed_width(ty: Ty, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: &PoolBuffer) -> Array {
    let data = match ty {
      Ty::NA => ArrayData::Null,
      Ty::Bool => ArrayData::Bool { values: values.as_vec() },

      Ty::Int8 => ArrayData::Int8 { values: values.as_vec() },
      Ty::Int16 => ArrayData::Int16 { values: values.as_vec() },
      Ty::Int32 => ArrayData::Int32 { values: values.as_vec() },
      Ty::Int64 => ArrayData::Int64 { values: values.as_vec() },
      Ty::UInt8 => ArrayData::UInt8 { values: values.as_vec() },
      Ty::UInt16 => ArrayData::UInt16 { values: values.as_vec() },
      Ty::UInt32 => ArrayData::UInt32 { values: values.as_vec() },
      Ty::UInt64 => ArrayData::UInt64 { values: values.as_vec() },

      Ty::HalfFloat => ArrayData::HalfFloat { values: values.as_vec() },
      Ty::Float => ArrayData::Float { values: values.as_vec() },
      Ty::Double => ArrayData::Double { values: values.as_vec() },

      Ty::Date64 { unit: ref _unit } => ArrayData::Date64 { values: values.as_vec() },
      Ty::Date32 { unit: ref _unit } => ArrayData::Date32 { values: values.as_vec() },
      Ty::Time64 { unit: ref _unit } => ArrayData::Time64 { values: values.as_vec() },
      Ty::Time32 { unit: ref _unit } => ArrayData::Time32 { values: values.as_vec() },
      Ty::Timestamp { unit: ref _unit, timezone: ref _timezone } => ArrayData::Timestamp { values: values.as_vec() },
      Ty::Interval { unit: ref _unit } => ArrayData::Interval { values: values.as_vec() },

      Ty::FixedSizeBinary { byte_width } => ArrayData::FixedSizeBinary { values: values.data() },
      Ty::Decimal { precision: _precision, scale: _scale } => ArrayData::Decimal { values: values.data() },

      _ => panic!("[{:?}] is not supported type", ty)
    };

    Array {
      ty,
      length,
      offset,
      null_count: Array::compute_null_count(&null_bitmap, offset, length),
      null_bitmap,
      data,
    }
  }

  pub fn variable_width(ty: Ty, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: &PoolBuffer, value_offsets: &PoolBuffer) -> Array {
    let data = match ty {
      Ty::Binary => ArrayData::Binary {
        value_offsets: unsafe { mem::transmute::<*const u8, *const i32>(value_offsets.data()) },
        values: values.data()
      },
      _ => panic!()
    };

    Array {
      ty,
      length,
      offset,
      null_count: Array::compute_null_count(&null_bitmap, offset, length),
      null_bitmap,
      data
    }
  }

  pub fn list(value_type: Box<Ty>, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, value_array: Array, value_offsets: &PoolBuffer) -> Array {
    let data = ArrayData::List {
      value_offsets: unsafe { mem::transmute::<*const u8, *const i32>(value_offsets.data()) },
      value_array: Box::new(value_array)
    };
    Array {
      ty: Ty::list(value_type),
      length,
      offset,
      null_count: Array::compute_null_count(&null_bitmap, offset, length),
      null_bitmap,
      data
    }
  }

  pub fn is_null(&self, i: i64) -> bool {
    match self.ty() {
      &Ty::NA => true,
      _ => match self.null_bitmap_buffer() {
        &Some(ref bitmap) => bit_util::bit_not_set(bitmap.data(), i + self.offset()),
        &None => false
      }
    }
  }

  pub fn is_valid(&self, i: i64) -> bool {
    match self.ty() {
      &Ty::NA => false,
      _ => match self.null_bitmap_buffer() {
        &Some(ref bitmap) => bit_util::get_bit(bitmap.data(), i + self.offset()),
        &None => true
      }
    }
  }

  #[inline]
  pub fn len(&self) -> i64 {
    self.length
  }

  #[inline]
  pub fn offset(&self) -> i64 {
    self.offset
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
    self.null_count
  }

  #[inline]
  pub fn ty(&self) -> &Ty {
    &self.ty
  }

  #[inline]
  pub fn null_bitmap_buffer(&self) -> &Option<PoolBuffer> {
    &self.null_bitmap
  }

  #[inline]
  pub fn data(&self) -> &ArrayData {
    &self.data
  }
}

impl Debug for Box<Array> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl Clone for Box<Array> {
  fn clone(&self) -> Self {
    unimplemented!()
  }
}

fn raw_value<T: Copy>(values: *const T, i: i64) -> T {
  unsafe { *values.offset(i as isize) }
}

fn raw_values<T>(value_buffer: &Option<PoolBuffer>, offset: i64) -> *const T {
  match value_buffer {
    &Some(ref buffer) => {
      unsafe {
        use std::ptr;
        mem::transmute::<*const u8, *const T>(buffer.data().offset(offset as isize))
      }
    },
    &None => panic!("value buffer doesn't exist")
  }
}

// TODO: maybe need cast?
pub trait PrimitiveArray<T: Copy> {
  fn prim_value(&self, i: i64) -> T;

  fn prim_values(&self) -> &[T];
//  fn values(&self) -> *const T;
}

impl PrimitiveArray<bool> for Array {
  fn prim_value(&self, i: i64) -> bool {
    match self.data() {
      &ArrayData::Bool { ref values } => values[i as usize],
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }

  fn prim_values(&self) -> &[bool] {
    match self.data() {
      &ArrayData::Bool { ref values } => values.as_slice(),
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }
}

macro_rules! impl_primitive_array {
    ($ty: path, $prim_ty: ident) => {
      impl PrimitiveArray<$prim_ty> for Array {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.data() {
            &$ty { ref values } => values[i as usize],
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

        fn prim_values(&self) -> &[$prim_ty] {
          match self.data() {
            &$ty { ref values } => values.as_slice(),
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
      }
    };

    ($ty1: path, $ty2: path, $prim_ty: ident) => {
      impl PrimitiveArray<$prim_ty> for Array {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } => values[i as usize],
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

        fn prim_values(&self) -> &[$prim_ty] {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } => values.as_slice(),
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
      }
    };

    ($ty1: path, $ty2: path, $ty3: path, $prim_ty: ident) => {
      impl PrimitiveArray<$prim_ty> for Array {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } => values[i as usize],
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

        fn prim_values(&self) -> &[$prim_ty] {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } => values.as_slice(),
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
      }
    };

    ($ty1: path, $ty2: path, $ty3: path, $ty4: path, $ty5: path, $prim_ty: ident) => {
      impl PrimitiveArray<$prim_ty> for Array {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } | &$ty4 { ref values } | &$ty5 { ref values } => values[i as usize],
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

        fn prim_values(&self) -> &[$prim_ty] {
          match self.data() {
            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } | &$ty4 { ref values } | &$ty5 { ref values } => values.as_slice(),
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
      }
    };
}

impl_primitive_array!(ArrayData::Int8, i8);
impl_primitive_array!(ArrayData::Int16, i16);
impl_primitive_array!(ArrayData::Int32, ArrayData::Date32, ArrayData::Time32, i32);
impl_primitive_array!(ArrayData::Int64, ArrayData::Date64, ArrayData::Time64, ArrayData::Timestamp, ArrayData::Interval, i64);
impl_primitive_array!(ArrayData::UInt8, u8);
impl_primitive_array!(ArrayData::UInt16, ArrayData::HalfFloat, u16);
impl_primitive_array!(ArrayData::UInt32, u32);
impl_primitive_array!(ArrayData::UInt64, u64);

impl_primitive_array!(ArrayData::Float, f32);
impl_primitive_array!(ArrayData::Double, f64);

pub struct VariableWidthElem {
  p: *const u8,
  len: i32
}

pub trait VariableWidthArray {
  fn value(&self, i: i64) -> VariableWidthElem;

  fn value_offset(&self, i: i64) -> i32;

  fn value_len(&self, i: i64) -> i32;
}

fn value_offset(value_offsets: &*const i32, i: i64) -> i32 {
  unsafe { *value_offsets.offset(i as isize) }
}

fn value_len(value_offsets: &*const i32, i: i64) -> i32 {
  unsafe {
    let i_as_isize = i as isize;
    let pos = *value_offsets.offset(i_as_isize);
    *value_offsets.offset(i_as_isize + 1) - pos
  }
}

impl VariableWidthArray for Array {
  fn value(&self, i: i64) -> VariableWidthElem {
    match self.data {
      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
        let offset = i + self.offset();
        unsafe {
          let pos = *value_offsets.offset(i as isize);
          let value_len = *value_offsets.offset((offset + 1) as isize) - pos;
          VariableWidthElem {
            p: values.offset(pos as isize),
            len: value_len
          }
        }
      },
      ArrayData::List { ref value_offsets, ref value_array } => {
        unimplemented!()
      }
      _ => panic!()
    }
  }

  fn value_offset(&self, i: i64) -> i32 {
    match self.data {
      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
        value_offset(value_offsets, i)
      },
      ArrayData::List { ref value_offsets, ref value_array } => {
        value_offset(value_offsets, i)
      },
      _ => panic!()
    }
  }

  fn value_len(&self, i: i64) -> i32 {
    match self.data {
      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
        value_len(value_offsets, i)
      },
      ArrayData::List { ref value_offsets, ref value_array } => {
        value_len(value_offsets, i)
      },
      _ => panic!()
    }
  }
}

pub trait StringArray {
  fn string(&self, i: i64) -> String;
}

impl <T> StringArray for T where T: VariableWidthArray {
  fn string(&self, i: i64) -> String {
    let elem = self.value(i);
    unsafe { String::from_raw_parts(mem::transmute::<*const u8, *mut u8>(elem.p), elem.len as usize, elem.len as usize) }
  }
}

pub trait FixedSizeBinaryArray {
  fn byte_width(&self) -> i32;

  fn fixed_size_value(&self, i: i64) -> *const u8;

  fn fixed_size_values(&self) -> *const u8;
}

impl FixedSizeBinaryArray for Array {
  fn byte_width(&self) -> i32 {
    match self.ty() {
      &Ty::FixedSizeBinary { byte_width } => byte_width,
      &Ty::Decimal { precision: _precision, scale: _scale } => 16,
      _ => panic!("{:?} is not fixed sized binary type", self.ty())
    }
  }

  fn fixed_size_value(&self, i: i64) -> *const u8 {
    match self.data() {
      &ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset(((self.offset() + i) * self.byte_width() as i64) as isize) },
      _ => panic!()
    }
  }

  fn fixed_size_values(&self) -> *const u8 {
    match self.data() {
      &ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset((self.offset() * self.byte_width() as i64) as isize) },
      _ => panic!()
    }
  }
}

pub trait ListArray {
  fn list_values(&self) -> &Box<Array>;

  fn value_type(&self) -> &Ty;
}

impl ListArray for Array {
  fn list_values(&self) -> &Box<Array> {
    match self.data {
      ArrayData::List { ref value_offsets, ref value_array } => value_array,
      _ => panic!()
    }
  }

  fn value_type(&self) -> &Ty {
    match self.ty() {
      &Ty::List { ref value_type } => value_type,
      _ => panic!()
    }
  }
}

//pub trait BaseArray {
//  fn is_null(&self, i: i64) -> bool {
//    !self.null_bitmap_data().is_null() &&
//      bit_util::bit_not_set(self.null_bitmap_data(), i + self.offset())
//  }
//
//  fn is_valid(&self, i: i64) -> bool {
//    !self.null_bitmap_data().is_null() &&
//      bit_util::get_bit(self.null_bitmap_data(), i + self.offset())
//  }
//
//  fn len(&self) -> i64 {
//    self.data().len()
//  }
//
//  fn offset(&self) -> i64 {
//    self.data().offset()
//  }
//
//  fn null_count(&self) -> i64 {
//    self.data().null_count()
//  }
//
//  fn data_type(&self) -> &Box<DataType> {
//    self.data().data_type()
//  }
//
//  fn ty(&self) -> Ty {
//    self.data_type().ty()
//  }
//
//  fn null_bitmap(&self) -> &Box<PoolBuffer> {
//    self.data().null_bitmap()
//  }
//
//  fn null_bitmap_data(&self) -> *const u8 {
//    self.data().raw_null_bitmap()
//  }
//
//  fn values(&self) -> &Box<PoolBuffer> {
//    self.data().values()
//  }
//
//  fn num_fields(&self) -> i32 {
//    self.data().num_children()
//  }
//
//  fn data(&self) -> &ArrayData;
//}

pub trait Cast {
//  fn as_null(&self) -> &NullArray {
//    unimplemented!("Cannot cast to null")
//  }

  fn as_bool(&self) -> &PrimitiveArray<bool> {
    unimplemented!("Cannot cast to boolean")
  }

  fn as_int8(&self) -> &PrimitiveArray<i8> {
    unimplemented!("Cannot cast to int8")
  }

//  fn as_int16(&self) -> &Int16Array {
//    unimplemented!("Cannot cast to int16")
//  }
//
//  fn as_int32(&self) -> &Int32Array {
//    unimplemented!("Cannot cast to int32")
//  }
//
//  fn as_int64(&self) -> &Int64Array {
//    unimplemented!("Cannot cast to int64")
//  }
//
//  fn as_uint8(&self) -> &UInt8Array {
//    unimplemented!("Cannot cast to uint8")
//  }
//
//  fn as_uint16(&self) -> &UInt16Array {
//    unimplemented!("Cannot cast to uint16")
//  }
//
//  fn as_uint32(&self) -> &UInt32Array {
//    unimplemented!("Cannot cast to uint32")
//  }
//
//  fn as_uint64(&self) -> &UInt64Array {
//    unimplemented!("Cannot cast to uint64")
//  }
//
//  fn as_float(&self) -> &FloatArray {
//    unimplemented!("Cannot cast to float")
//  }
//
//  fn as_double(&self) -> &DoubleArray {
//    unimplemented!("Cannot cast to double")
//  }
//
//  fn as_halffloat(&self) -> &HalfFloatArray {
//    unimplemented!("Cannot cast to halffloat")
//  }
}

//pub trait Array : BaseArray + Cast {}
//
//pub fn array_eq(a1: &Array, a2: &Array) -> bool {
//  unimplemented!()
//}
//
//impl PartialEq for Box<Array> {
//  fn eq(&self, other: &Self) -> bool {
//    unimplemented!()
//  }
//}
//
//impl Eq for Box<Array> {
//
//}
//
//impl Debug for Box<Array> {
//  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//    unimplemented!()
//  }
//}
//
//impl Clone for Box<Array> {
//  fn clone(&self) -> Self {
//    unimplemented!()
//  }
//}
//
//
//#[derive(PartialEq, Eq)]
//pub struct NullArray {
//  data: ArrayData
//}
//
//impl NullArray {
//  pub fn new(len: i64) -> Self {
//    NullArray {
//      data: ArrayData::null_data(Box::new(ty::NullType::new()), len, 0)
//    }
//  }
//
//  pub fn from_data(data: ArrayData) -> Self {
//    NullArray {
//      data
//    }
//  }
//}
//
//impl BaseArray for NullArray {
//  fn is_null(&self, i: i64) -> bool {
//    true
//  }
//
//  fn data(&self) -> &ArrayData {
//    &self.data
//  }
//}
//
//impl Cast for NullArray {
//  fn as_null(&self) -> &NullArray {
//    &self
//  }
//}
//
//trait NumericArray<T> : Array {
//  fn value(&self, i: i64) -> T;
//}
//
//#[derive(PartialEq, Eq)]
//pub struct BooleanArray {
//  data: ArrayData,
//  raw_values: *const u8
//}
//
//impl BooleanArray {
//  pub fn from_data(data: ArrayData) -> Self {
//    let raw_values = data.raw_values();
//    BooleanArray {
//      data,
//      raw_values
//    }
//  }
//
//  pub fn raw_values(&self) -> *const u8 {
//    self.raw_values
//  }
//
//  pub fn value(&self, i: i64) -> bool {
//    bit_util::get_bit(self.raw_values, i + self.data.offset())
//  }
//}
//
//impl BaseArray for BooleanArray {
//  fn data(&self) -> &ArrayData {
//    &self.data
//  }
//}
//
//impl Cast for BooleanArray {
//  fn as_bool(&self) -> &BooleanArray {
//    &self
//  }
//}
//
//macro_rules! define_numeric_array {
//    ($name: ident, $cast_fn: ident) => {
//      pub struct $name {
//        data: ArrayData,
//        raw_values: *const u8
//      }
//
//      impl $name {
//        pub fn from_data(data: ArrayData) -> Self {
//          let raw_values = data.raw_values();
//          $name {
//            data,
//            raw_values
//          }
//        }
//
//        pub fn raw_values(&self) -> *const u8 {
//          self.raw_values
//        }
//
//        pub fn value(&self, i: i64) -> bool {
//          bit_util::get_bit(self.raw_values, i + self.data.offset())
//        }
//      }
//
//      impl BaseArray for $name {
//        fn data(&self) -> &ArrayData {
//          &self.data
//        }
//      }
//
//      impl Cast for $name {
//        fn $cast_fn(&self) -> &$name {
//          &self
//        }
//      }
//    };
//}
//
//define_numeric_array!(Int8Array, as_int8);
//define_numeric_array!(Int16Array, as_int16);
//define_numeric_array!(Int32Array, as_int32);
//define_numeric_array!(Int64Array, as_int64);
//define_numeric_array!(UInt8Array, as_uint8);
//define_numeric_array!(UInt16Array, as_uint16);
//define_numeric_array!(UInt32Array, as_uint32);
//define_numeric_array!(UInt64Array, as_uint64);
//define_numeric_array!(FloatArray, as_float);
//define_numeric_array!(DoubleArray, as_double);
//define_numeric_array!(HalfFloatArray, as_halffloat);




//#[derive(Debug, Eq, PartialEq)]
//pub enum ArrayType {
//
//}
//
//macro_rules! define_base_array {
//    ($name: ident) => {
//      #[derive(Eq, PartialEq)]
//      pub struct $name<'a> {
//        data: ArrayData<'a>,
//        null_bitmap_data: *const u8
//      }
//
//      impl<'a> $name<'a> {
//        pub fn is_null(&self, i: i64) -> bool {
//          self.null_bitmap_data.is_null() || bit_util::bit_not_set(self.null_bitmap_data, i + self.data.offset())
//        }
//
//        pub fn len(&self) -> i64 {
//          self.data.len()
//        }
//
//        pub fn offset(&self) -> i64 {
//          self.data.offset()
//        }
//
//        pub fn data(&self) -> &ArrayData {
//          &self.data
//        }
//
//        pub fn null_bitmap_data(&self) -> &*const u8 {
//          &self.null_bitmap_data
//        }
//      }
//    };
//}
//
//define_base_array!(NullArray);
//
//impl <'a> NullArray<'a> {
//  pub fn with_data(array_data: ArrayData<'a>) -> NullArray<'a> {
//    let null_data = ArrayData {
//      ty: array_data.ty,
//      length: array_data.length,
//      null_count: array_data.length,
//      offset: 0,
//      buffers: array_data.buffers,
//      children: array_data.children
//    };
//    NullArray {
//      data: null_data,
//      null_bitmap_data: ptr::null()
//    }
//  }
//}

#[cfg(test)]
mod tests {
  #[test]
  fn test_null_array() {
    use array::Array;

    let arr = Array::null(100, 0);

    assert_eq!(100, arr.len());
    assert_eq!(0, arr.offset());
    assert!(arr.is_null(37));
  }
}