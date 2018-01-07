use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};
use builder::{ArrayBuilder, BuilderData};

use std::ptr;
use std::mem;

use std::fmt::{Debug, Formatter, Error};

#[derive(Eq, PartialEq)]
pub struct Array<'a> {
//  ty: Ty,
//  length: i64,
//  offset: i64,
//  null_count: i64,
//  null_bitmap: Option<PoolBuffer>,
//  data: ArrayData

  builder: ArrayBuilder<'a>,
  data: ArrayData<'a>
}

#[derive(Eq, PartialEq)]
pub enum ArrayData<'a> {
  Null,
  Bool {
//    values: *const u8
  },

  UInt8 {
//    values: *const u8
    values: &'a [u8]
  },
  Int8 {
    values: *const i8
  },
  UInt16 {
    values: *const u16
  },
  Int16 {
    values: *const i16
  },
  UInt32 {
    values: *const u32
  },
  Int32 {
    values: *const i32
  },
  UInt64 {
    values: *const u64
  },
  Int64 {
    values: *const i64
  },

  HalfFloat {
    values: *const u16
  },
  Float {
    values: *const f32
  },
  Double {
    values: *const f64
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
    values: *const i64
  },
  Date32 {
    values: *const i32
  },
  Timestamp {
    values: *const i64
  },
  Time32 {
    values: *const i32
  },
  Time64 {
    values: *const i64
  },
  Interval {
    values: *const i64
  },

  Decimal {
    values: *const u8
  },

  List {
    value_offsets: *const i32,
    value_array: Box<Array<'a>>
  },
  Struct {
    fields: Vec<Box<Array<'a>>>
  },
  Union {
    fields: Vec<Box<Array<'a>>>,
    value_offsets: *const i32
  },

  Dictionary {
    indices: Box<Array<'a>>
  }
}

//impl PartialEq for ArrayData {
//  fn eq(&self, other: &Self) -> bool {
//    // TODO
//    unimplemented!()
//  }
//}
//
//impl Eq for ArrayData {
//
//}

impl <'a> Array<'a> {
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

//  pub fn null(length: i64, offset: i64) -> Array {
//    Array {
//      ty: Ty::null(),
//      length,
//      offset,
//      null_count: length,
//      null_bitmap: None,
//      data: ArrayData::Null
//    }
//  }
//
//  pub fn fixed_width(ty: Ty, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: &PoolBuffer) -> Array {
//    let data = match ty {
//      Ty::NA => ArrayData::Null,
//      Ty::Bool => ArrayData::Bool { values: values.data() },
//
//      Ty::Int8 => ArrayData::Int8 { values: unsafe { mem::transmute::<*const u8, *const i8>(values.data()) } },
//      Ty::Int16 => ArrayData::Int16 { values: unsafe { mem::transmute::<*const u8, *const i16>(values.data()) } },
//      Ty::Int32 => ArrayData::Int32 { values: unsafe { mem::transmute::<*const u8, *const i32>(values.data()) } },
//      Ty::Int64 => ArrayData::Int64 { values: unsafe { mem::transmute::<*const u8, *const i64>(values.data()) } },
////      Ty::UInt8 => ArrayData::UInt8 { values: values.data() },
//      Ty::UInt8 => ArrayData::UInt8 { values: values.as_vec() },
//      Ty::UInt16 => ArrayData::UInt16 { values: unsafe { mem::transmute::<*const u8, *const u16>(values.data()) } },
//      Ty::UInt32 => ArrayData::UInt32 { values: unsafe { mem::transmute::<*const u8, *const u32>(values.data()) } },
//      Ty::UInt64 => ArrayData::UInt64 { values: unsafe { mem::transmute::<*const u8, *const u64>(values.data()) } },
//
//      Ty::HalfFloat => ArrayData::HalfFloat { values: unsafe { mem::transmute::<*const u8, *const u16>(values.data()) } },
//      Ty::Float => ArrayData::Float { values: unsafe { mem::transmute::<*const u8, *const f32>(values.data()) } },
//      Ty::Double => ArrayData::Double { values: unsafe { mem::transmute::<*const u8, *const f64>(values.data()) } },
//
//      Ty::Date64 { unit: ref _unit } => ArrayData::Date64 { values: unsafe { mem::transmute::<*const u8, *const i64>(values.data()) } },
//      Ty::Date32 { unit: ref _unit } => ArrayData::Date32 { values: unsafe { mem::transmute::<*const u8, *const i32>(values.data()) } },
//      Ty::Time64 { unit: ref _unit } => ArrayData::Time64 { values: unsafe { mem::transmute::<*const u8, *const i64>(values.data()) } },
//      Ty::Time32 { unit: ref _unit } => ArrayData::Time32 { values: unsafe { mem::transmute::<*const u8, *const i32>(values.data()) } },
//      Ty::Timestamp { unit: ref _unit, timezone: ref _timezone } => ArrayData::Timestamp { values: unsafe { mem::transmute::<*const u8, *const i64>(values.data()) } },
//      Ty::Interval { unit: ref _unit } => ArrayData::Interval { values: unsafe { mem::transmute::<*const u8, *const i64>(values.data()) } },
//
//      Ty::FixedSizeBinary { byte_width } => ArrayData::FixedSizeBinary { values: values.data() },
//      Ty::Decimal { precision: _precision, scale: _scale } => ArrayData::Decimal { values: values.data() },
//
//      _ => panic!("[{:?}] is not supported type", ty)
//    };
//
//    Array {
//      ty,
//      length,
//      offset,
//      null_count: Array::compute_null_count(&null_bitmap, offset, length),
//      null_bitmap,
//      data,
//    }
//  }
//
//  pub fn variable_width(ty: Ty, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, values: &PoolBuffer, value_offsets: &PoolBuffer) -> Array {
//    let data = match ty {
//      Ty::Binary => ArrayData::Binary {
//        value_offsets: unsafe { mem::transmute::<*const u8, *const i32>(value_offsets.data()) },
//        values: values.data()
//      },
//      _ => panic!()
//    };
//
//    Array {
//      ty,
//      length,
//      offset,
//      null_count: Array::compute_null_count(&null_bitmap, offset, length),
//      null_bitmap,
//      data
//    }
//  }
//
//  pub fn list(value_type: Box<Ty>, length: i64, offset: i64, null_bitmap: Option<PoolBuffer>, value_array: Array, value_offsets: &PoolBuffer) -> Array {
//    let data = ArrayData::List {
//      value_offsets: unsafe { mem::transmute::<*const u8, *const i32>(value_offsets.data()) },
//      value_array: Box::new(value_array)
//    };
//    Array {
//      ty: Ty::list(value_type),
//      length,
//      offset,
//      null_count: Array::compute_null_count(&null_bitmap, offset, length),
//      null_bitmap,
//      data
//    }
//  }

  pub fn new(builder: ArrayBuilder<'a>) -> Array<'a> {
    let data = match builder.ty() {
      &Ty::NA => ArrayData::Null,
      &Ty::Bool => ArrayData::Bool {},
      &Ty::UInt8 => {
        let values = match builder.data() {
          &BuilderData::UInt8 { ref data } => {
            use std::slice;
            unsafe { slice::from_raw_parts(data.data(), data.size() as usize) }
          }
          _ => panic!()
        };
        ArrayData::UInt8 { values }
      },
      _ => panic!()
    };

    Array {
      builder,
      data
    }
  }

  pub fn is_null(&self, i: i64) -> bool {
    match self.ty() {
      &Ty::NA => true,
      _ => bit_util::bit_not_set(self.null_bitmap_buffer().data(), i + self.offset())
    }
  }

  pub fn is_valid(&self, i: i64) -> bool {
    match self.ty() {
      &Ty::NA => false,
      _ => bit_util::get_bit(self.null_bitmap_buffer().data(), i + self.offset())
    }
  }

  #[inline]
  pub fn len(&self) -> i64 {
//    self.length
    self.builder.len()
  }

  #[inline]
  pub fn offset(&self) -> i64 {
//    self.offset
    unimplemented!()
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
//    self.null_count
    self.builder.null_count()
  }

  #[inline]
  pub fn ty(&self) -> &Ty {
//    &self.ty
    self.builder.ty()
  }

  #[inline]
  pub fn null_bitmap_buffer(&self) -> &PoolBuffer {
//    &self.null_bitmap
    self.builder.null_bitmap()
  }

//  #[inline]
//  pub fn data(&self) -> &ArrayData {
//    &self.data
//  }
}

impl <'a> Debug for Box<Array<'a>> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl <'a> Clone for Box<Array<'a>> {
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

pub trait BooleanArray {
  fn bool_value(&self, i: i64) -> bool;
}

impl <'a> BooleanArray for Array<'a> {
  fn bool_value(&self, i: i64) -> bool {
    match self.builder.data() {
      &BuilderData::Bool { ref data } => bit_util::get_bit(data.data(), i),
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }
}

pub trait UInt8Array {
  fn u8_value(&self, i: i64) -> u8;
}

impl <'a> UInt8Array for Array<'a> {
  fn u8_value(&self, i: i64) -> u8 {
    match self.data {
      ArrayData::UInt8 { ref values } => values[i as usize],
      _ => panic!()
    }
  }
}

pub trait PrimitiveArray<T: Copy> {
  fn prim_value(&self, i: i64) -> T;

//  fn prim_values(&self) -> &[T]; TODO: support this after PoolBuffer.as_vec() is fixed
}

macro_rules! impl_primitive_array {
    ($ty: path, $prim_ty: ident) => {
      impl <'a > PrimitiveArray<$prim_ty> for Array<'a> {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.builder.data() {
//            &$ty { ref values } => values[i as usize],
              &$ty { ref data } => unsafe { *data.data().offset(i as isize) },
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

//        fn prim_values(&self) -> &[$prim_ty] {
//          match self.data() {
//            &$ty { ref values } => values.as_slice(),
//            _ => panic!("{:?} is not a boolean array", self.ty())
//          }
//        }
      }
    };

    ($ty1: path, $ty2: path, $prim_ty: ident) => {
      impl <'a > PrimitiveArray<$prim_ty> for Array<'a > {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.builder.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } => values[i as usize],
              &$ty1 { ref data } | &$ty2 { ref data } => unsafe { *data.data().offset(i as isize) },
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

//        fn prim_values(&self) -> &[$prim_ty] {
//          match self.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } => values.as_slice(),
//            _ => panic!("{:?} is not a boolean array", self.ty())
//          }
//        }
      }
    };

    ($ty1: path, $ty2: path, $ty3: path, $prim_ty: ident) => {
      impl <'a > PrimitiveArray<$prim_ty> for Array<'a > {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.builder.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } => values[i as usize],
              &$ty1 { ref data } | &$ty2 { ref data } | &$ty3 { ref data } => unsafe { *data.data().offset(i as isize) },
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

//        fn prim_values(&self) -> &[$prim_ty] {
//          match self.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } => values.as_slice(),
//            _ => panic!("{:?} is not a boolean array", self.ty())
//          }
//        }
      }
    };

    ($ty1: path, $ty2: path, $ty3: path, $ty4: path, $ty5: path, $prim_ty: ident) => {
      impl <'a > PrimitiveArray<$prim_ty> for Array<'a > {
        fn prim_value(&self, i: i64) -> $prim_ty {
          match self.builder.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } | &$ty4 { ref values } | &$ty5 { ref values } => values[i as usize],
            &$ty1 { ref data } | &$ty2 { ref data } | &$ty3 { ref data } | &$ty4 { ref data } | &$ty5 { ref data } => unsafe { *data.data().offset(i as isize) },
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

//        fn prim_values(&self) -> &[$prim_ty] {
//          match self.data() {
//            &$ty1 { ref values } | &$ty2 { ref values } | &$ty3 { ref values } | &$ty4 { ref values } | &$ty5 { ref values } => values.as_slice(),
//            _ => panic!("{:?} is not a boolean array", self.ty())
//          }
//        }
      }
    };
}

//impl_primitive_array!(BuilderData::Int8, i8);
//impl_primitive_array!(ArrayData::Int16, i16);
//impl_primitive_array!(ArrayData::Int32, ArrayData::Date32, ArrayData::Time32, i32);
//impl_primitive_array!(ArrayData::Int64, ArrayData::Date64, ArrayData::Time64, ArrayData::Timestamp, ArrayData::Interval, i64);
////impl_primitive_array!(ArrayData::UInt8, u8);
//impl_primitive_array!(ArrayData::UInt16, ArrayData::HalfFloat, u16);
//impl_primitive_array!(ArrayData::UInt32, u32);
//impl_primitive_array!(ArrayData::UInt64, u64);
//
//impl_primitive_array!(ArrayData::Float, f32);
//impl_primitive_array!(ArrayData::Double, f64);
//
//pub struct VariableWidthElem {
//  p: *const u8,
//  len: i32
//}
//
//pub trait VariableWidthArray {
//  fn value(&self, i: i64) -> VariableWidthElem;
//
//  fn value_offset(&self, i: i64) -> i32;
//
//  fn value_len(&self, i: i64) -> i32;
//}
//
//fn value_offset(value_offsets: &*const i32, i: i64) -> i32 {
//  unsafe { *value_offsets.offset(i as isize) }
//}
//
//fn value_len(value_offsets: &*const i32, i: i64) -> i32 {
//  unsafe {
//    let i_as_isize = i as isize;
//    let pos = *value_offsets.offset(i_as_isize);
//    *value_offsets.offset(i_as_isize + 1) - pos
//  }
//}
//
//impl VariableWidthArray for Array {
//  fn value(&self, i: i64) -> VariableWidthElem {
//    match self.data {
//      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
//        let offset = i + self.offset();
//        unsafe {
//          let pos = *value_offsets.offset(i as isize);
//          let value_len = *value_offsets.offset((offset + 1) as isize) - pos;
//          VariableWidthElem {
//            p: values.offset(pos as isize),
//            len: value_len
//          }
//        }
//      },
//      ArrayData::List { ref value_offsets, ref value_array } => {
//        unimplemented!()
//      }
//      _ => panic!()
//    }
//  }
//
//  fn value_offset(&self, i: i64) -> i32 {
//    match self.data {
//      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
//        value_offset(value_offsets, i)
//      },
//      ArrayData::List { ref value_offsets, ref value_array } => {
//        value_offset(value_offsets, i)
//      },
//      _ => panic!()
//    }
//  }
//
//  fn value_len(&self, i: i64) -> i32 {
//    match self.data {
//      ArrayData::Binary { ref value_offsets, ref values } | ArrayData::String { ref value_offsets, ref values } => {
//        value_len(value_offsets, i)
//      },
//      ArrayData::List { ref value_offsets, ref value_array } => {
//        value_len(value_offsets, i)
//      },
//      _ => panic!()
//    }
//  }
//}
//
//pub trait StringArray {
//  fn string(&self, i: i64) -> String;
//}
//
//impl <T> StringArray for T where T: VariableWidthArray {
//  fn string(&self, i: i64) -> String {
//    let elem = self.value(i);
//    unsafe { String::from_raw_parts(mem::transmute::<*const u8, *mut u8>(elem.p), elem.len as usize, elem.len as usize) }
//  }
//}
//
//pub trait FixedSizeBinaryArray {
//  fn byte_width(&self) -> i32;
//
//  fn fixed_size_value(&self, i: i64) -> *const u8;
//
//  fn fixed_size_values(&self) -> *const u8;
//}
//
//impl FixedSizeBinaryArray for Array {
//  fn byte_width(&self) -> i32 {
//    match self.ty() {
//      &Ty::FixedSizeBinary { byte_width } => byte_width,
//      &Ty::Decimal { precision: _precision, scale: _scale } => 16,
//      _ => panic!("{:?} is not fixed sized binary type", self.ty())
//    }
//  }
//
//  fn fixed_size_value(&self, i: i64) -> *const u8 {
//    match self.data() {
//      &ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset(((self.offset() + i) * self.byte_width() as i64) as isize) },
//      _ => panic!()
//    }
//  }
//
//  fn fixed_size_values(&self) -> *const u8 {
//    match self.data() {
//      &ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset((self.offset() * self.byte_width() as i64) as isize) },
//      _ => panic!()
//    }
//  }
//}
//
//pub trait ListArray {
//  fn list_values(&self) -> &Box<Array>;
//
//  fn value_type(&self) -> &Ty;
//}
//
//impl ListArray for Array {
//  fn list_values(&self) -> &Box<Array> {
//    match self.data {
//      ArrayData::List { ref value_offsets, ref value_array } => value_array,
//      _ => panic!()
//    }
//  }
//
//  fn value_type(&self) -> &Ty {
//    match self.ty() {
//      &Ty::List { ref value_type } => value_type,
//      _ => panic!()
//    }
//  }
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
