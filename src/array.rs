use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};
use builder::{ArrayBuilder, BuilderData};

use std::ptr;
use std::mem;
use std::slice;

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
  Bool,

  UInt8 {
    values: &'a [u8]
  },
  Int8 {
    values: &'a [i8]
  },
  UInt16 {
    values: &'a [u16]
  },
  Int16 {
    values: &'a [i16]
  },
  UInt32 {
    values: &'a [u32]
  },
  Int32 {
    values: &'a [i32]
  },
  UInt64 {
    values: &'a [u64]
  },
  Int64 {
    values: &'a [i64]
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

  pub fn new(builder: ArrayBuilder<'a>) -> Array<'a> {
    let data = match builder.data() {
      &BuilderData::Null => ArrayData::Null,
      &BuilderData::Bool { ref null_bitmap, ref data } => ArrayData::Bool,
      &BuilderData::UInt8 { ref null_bitmap, ref data } => {
        ArrayData::UInt8 {
          values : unsafe { slice::from_raw_parts(data.data(), builder.len() as usize) }
        }
      },
      &BuilderData::Int8 { ref null_bitmap, ref data } => {
        ArrayData::Int8 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const i8>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::UInt16 { ref null_bitmap, ref data } => {
        ArrayData::UInt16 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const u16>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::Int16 { ref null_bitmap, ref data } => {
        ArrayData::Int16 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const i16>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::UInt32 { ref null_bitmap, ref data } => {
        ArrayData::UInt32 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const u32>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::Int32 { ref null_bitmap, ref data } => {
        ArrayData::Int32 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const i32>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::UInt64 { ref null_bitmap, ref data } => {
        ArrayData::UInt64 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const u64>(data.data()), builder.len() as usize) }
        }
      },
      &BuilderData::Int64 { ref null_bitmap, ref data } => {
        ArrayData::Int64 {
          values : unsafe { slice::from_raw_parts(mem::transmute::<*const u8, *const i64>(data.data()), builder.len() as usize) }
        }
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
      _ => match self.null_bitmap_buffer() {
        Some(ref null_bitmap) => bit_util::bit_not_set(null_bitmap.data(), i + self.offset()),
        None => panic!()
      }
    }
  }

  pub fn is_valid(&self, i: i64) -> bool {
    match self.ty() {
      &Ty::NA => false,
      _ => match self.null_bitmap_buffer() {
        Some(ref null_bitmap) => bit_util::get_bit(null_bitmap.data(), i + self.offset()),
        None => panic!()
      }
    }
  }

  #[inline]
  pub fn len(&self) -> i64 {
    self.builder.len()
  }

  #[inline]
  pub fn offset(&self) -> i64 {
//    self.offset
    unimplemented!()
  }

  #[inline]
  pub fn null_count(&self) -> i64 {
    self.builder.null_count()
  }

  #[inline]
  pub fn ty(&self) -> &Ty {
    self.builder.ty()
  }

  #[inline]
  pub fn null_bitmap_buffer(&self) -> Option<&PoolBuffer> {
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

#[inline]
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

pub trait ArrowSlice<T> {
  fn value(&self, i: i64) -> T;
  fn values(&self) -> &[T];
}

impl <'a> ArrowSlice<bool> for Array<'a> {
  fn value(&self, i: i64) -> bool {
    match self.builder.data() {
      &BuilderData::Bool { ref null_bitmap, ref data } => bit_util::get_bit(data.data(), i),
      _ => panic!("{:?} is not a boolean array", self.ty())
    }
  }

  fn values(&self) -> &[bool] {
    unimplemented!()
  }
}

macro_rules! impl_arrow_slice {
    ($ty: path, $prim_ty: ident) => {
      impl <'a > ArrowSlice<$prim_ty> for Array<'a> {
        fn value(&self, i: i64) -> $prim_ty {
          match self.data {
            $ty { ref values } => values[i as usize],
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }

        fn values(&self) -> &[$prim_ty] {
          match self.data {
            $ty { ref values } => *values,
            _ => panic!("{:?} is not a boolean array", self.ty())
          }
        }
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

impl_arrow_slice!(ArrayData::Int8, i8);
impl_arrow_slice!(ArrayData::Int16, i16);
impl_arrow_slice!(ArrayData::Int32, i32);
impl_arrow_slice!(ArrayData::Int64, i64);
//impl_arrow_slice!(ArrayData::Int32, ArrayData::Date32, ArrayData::Time32, i32);
//impl_arrow_slice!(ArrayData::Int64, ArrayData::Date64, ArrayData::Time64, ArrayData::Timestamp, ArrayData::Interval, i64);
impl_arrow_slice!(ArrayData::UInt8, u8);
impl_arrow_slice!(ArrayData::UInt16, u16);
//impl_arrow_slice!(ArrayData::UInt16, ArrayData::HalfFloat, u16);
impl_arrow_slice!(ArrayData::UInt32, u32);
impl_arrow_slice!(ArrayData::UInt64, u64);

//impl_primitive_array!(ArrayData::Float, f32);
//impl_primitive_array!(ArrayData::Double, f64);

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

impl <'a> VariableWidthArray for Array<'a> {
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

impl <'a> FixedSizeBinaryArray for Array<'a> {
  fn byte_width(&self) -> i32 {
    match self.ty() {
      &Ty::FixedSizeBinary { byte_width } => byte_width,
      &Ty::Decimal { precision: _precision, scale: _scale } => 16,
      _ => panic!("{:?} is not fixed sized binary type", self.ty())
    }
  }

  fn fixed_size_value(&self, i: i64) -> *const u8 {
    match self.data {
      ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset(((self.offset() + i) * self.byte_width() as i64) as isize) },
      _ => panic!()
    }
  }

  fn fixed_size_values(&self) -> *const u8 {
    match self.data {
      ArrayData::FixedSizeBinary { ref values } => unsafe { values.offset((self.offset() * self.byte_width() as i64) as isize) },
      _ => panic!()
    }
  }
}

pub trait ListArray<'a> {
  fn list_values(&self) -> &Box<Array<'a>>;

  fn value_type(&self) -> &Ty;
}

impl <'a> ListArray<'a> for Array<'a> {
  fn list_values(&self) -> &Box<Array<'a>> {
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

pub trait Cast {
//  fn as_null(&self) -> &NullArray {
//    unimplemented!("Cannot cast to null")
//  }

  fn as_bool(&self) -> &ArrowSlice<bool> {
    unimplemented!("Cannot cast to boolean")
  }

  fn as_int8(&self) -> &ArrowSlice<i8> {
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
