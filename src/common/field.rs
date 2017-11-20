use common::KeyValueMetadata;
use common::ty::*;
use array::Array;

use std::fmt::{Debug, Formatter, Error};

pub trait Field {
  fn get_name(&self) -> &String;
  fn get_type(&self) -> &DowncastDataType;
  fn nullable(&self) -> bool;
}

pub trait Downcast {
  fn as_null(&self) -> &NullField {
    panic!("Cannot cast to null")
  }

  fn as_bool(&self) -> &BooleanField {
    panic!("Cannot cast to bool")
  }

  fn as_uint8(&self) -> &UInt8Field  {
    panic!("Cannot cast to uint8")
  }

  fn as_int8(&self) -> &Int8Field  {
    panic!("Cannot cast to int8")
  }

  fn as_uint16(&self) -> &UInt16Field  {
    panic!("Cannot cast to uint16")
  }

  fn as_int16(&self) -> &Int16Field  {
    panic!("Cannot cast to int16")
  }

  fn as_uint32(&self) -> &UInt32Field  {
    panic!("Cannot cast to uint32")
  }

  fn as_int32(&self) -> &Int32Field  {
    panic!("Cannot cast to int32")
  }

  fn as_uint64(&self) -> &UInt64Field  {
    panic!("Cannot cast to uint64")
  }

  fn as_int64(&self) -> &Int64Field  {
    panic!("Cannot cast to int64")
  }

  fn as_half_float(&self) -> &HalfFloatField  {
    panic!("Cannot cast to half_float")
  }

  fn as_float(&self) -> &FloatField  {
    panic!("Cannot cast to float")
  }

  fn as_double(&self) -> &DoubleField  {
    panic!("Cannot cast to double")
  }

  fn as_string(&self) -> &StringField  {
    panic!("Cannot cast to string")
  }

  fn as_binary(&self) -> &BinaryField  {
    panic!("Cannot cast to binary")
  }

  fn as_fixed_sized_binary(&self) -> &FixedSizedBinaryField  {
    panic!("Cannot cast to fixed_sized_binary")
  }

  fn as_date64(&self) -> &Date64Field  {
    panic!("Cannot cast to date64")
  }

  fn as_date32(&self) -> &Date32Field  {
    panic!("Cannot cast to date32")
  }

  fn as_timestamp(&self) -> &TimestampField  {
    panic!("Cannot cast to timestamp")
  }

  fn as_time32(&self) -> &Time32Field  {
    panic!("Cannot cast to time32")
  }

  fn as_time64(&self) -> &Time64Field  {
    panic!("Cannot cast to time64")
  }

  fn as_interval(&self) -> &IntervalField  {
    panic!("Cannot cast to interval")
  }

  fn as_decimal(&self) -> &DecimalField  {
    panic!("Cannot cast to decimal")
  }

  fn as_list(&self) -> &ListField  {
    panic!("Cannot cast to list")
  }

  fn as_struct(&self) -> &StructField  {
    panic!("Cannot cast to struct")
  }

  fn as_union(&self) -> &UnionField  {
    panic!("Cannot cast to union")
  }

  fn as_dictionary(&self) -> &DictionaryField  {
    panic!("Cannot cast to dictionary")
  }
}

pub trait DowncastField : Field + Downcast {}

impl Debug for Box<DowncastField> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl PartialEq for Box<DowncastField> {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for Box<DowncastField> {

}

impl Clone for Box<DowncastField> {
  fn clone(&self) -> Self {
    unimplemented!()
  }
}

macro_rules! define_field {
    ($name: ident, $data_type: ident) => {

      #[derive(Debug, PartialEq, Eq, Clone)]
      pub struct $name {
        name: String,
        ty: $data_type,
        nullable: bool,
        metadata: Option<KeyValueMetadata>
      }

      impl $name {
        pub fn new(name: String) -> $name {
          $name::create(name, true, Option::None)
        }

        pub fn non_nullable(name: String) -> $name {
          $name::create(name, false, Option::None)
        }

        pub fn with_metadata(name: String, metadata: KeyValueMetadata) -> $name {
            $name::create(name, true, Option::from(metadata))
        }

        pub fn non_nullable_with_metadata(name: String, metadata: KeyValueMetadata) -> $name {
            $name::create(name, false, Option::from(metadata))
        }

        fn create(name: String, nullable: bool, metadata: Option<KeyValueMetadata>) -> $name {
          $name {
            name,
            ty: $data_type::new(),
            nullable,
            metadata
          }
        }

        pub fn add_metadata(&self, metadata: KeyValueMetadata) -> $name {
          $name::create(self.name.clone(), self.nullable, Option::from(metadata))
        }

        pub fn remove_metadata(&self) -> $name {
          $name::create(self.name.clone(), self.nullable, Option::None)
        }

        pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
          &self.metadata
        }
      }
    };
}

macro_rules! impl_default_traits {
    ($name: ident, $downcast_method: ident) => {
      impl Field for $name {
        fn get_name(&self) -> &String {
          &self.name
        }

        fn get_type(&self) -> &DowncastDataType {
          &self.ty
        }

        fn nullable(&self) -> bool {
          self.nullable
        }
      }

      impl Downcast for $name {
        fn $downcast_method(&self) -> &$name {
          &self
        }
      }

      impl DowncastField for $name {}
    };
}

define_field!(NullField, NullType);
impl_default_traits!(NullField, as_null);

define_field!(BooleanField, BooleanType);
impl_default_traits!(BooleanField, as_bool);

define_field!(Int8Field, Int8Type);
impl_default_traits!(Int8Field, as_int8);
define_field!(Int16Field, Int16Type);
impl_default_traits!(Int16Field, as_int16);
define_field!(Int32Field, Int32Type);
impl_default_traits!(Int32Field, as_int32);
define_field!(Int64Field, Int64Type);
impl_default_traits!(Int64Field, as_int64);
define_field!(UInt8Field, UInt8Type);
impl_default_traits!(UInt8Field, as_uint8);
define_field!(UInt16Field, UInt16Type);
impl_default_traits!(UInt16Field, as_uint16);
define_field!(UInt32Field, UInt32Type);
impl_default_traits!(UInt32Field, as_uint32);
define_field!(UInt64Field, UInt64Type);
impl_default_traits!(UInt64Field, as_uint64);

define_field!(HalfFloatField, HalfFloatType);
impl_default_traits!(HalfFloatField, as_half_float);
define_field!(FloatField, FloatType);
impl_default_traits!(FloatField, as_float);
define_field!(DoubleField, DoubleType);
impl_default_traits!(DoubleField, as_double);

define_field!(StringField, StringType);
impl_default_traits!(StringField, as_string);

define_field!(BinaryField, BinaryType);
impl_default_traits!(BinaryField, as_binary);


#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FixedSizedBinaryField {
  name: String,
  ty: FixedSizedBinaryType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl FixedSizedBinaryField {
  pub fn new(name: String, byte_width: i32) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(name, true, byte_width, Option::None)
  }

  pub fn non_nullable(name: String, byte_width: i32) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(name, false, byte_width, Option::None)
  }

  pub fn with_metadata(name: String, byte_width: i32, metadata: KeyValueMetadata) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(name, true, byte_width, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, byte_width: i32, metadata: KeyValueMetadata) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(name, false, byte_width, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, byte_width: i32, metadata: Option<KeyValueMetadata>) -> FixedSizedBinaryField {
    FixedSizedBinaryField {
      name,
      ty: FixedSizedBinaryType::new(byte_width),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(self.name.clone(), self.nullable, self.ty.byte_width(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> FixedSizedBinaryField {
    FixedSizedBinaryField::create(self.name.clone(), self.nullable, self.ty.byte_width(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(FixedSizedBinaryField, as_fixed_sized_binary);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Date32Field {
  name: String,
  ty: Date32Type,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl Date32Field {
  pub fn new(name: String, unit: DateUnit) -> Date32Field {
    Date32Field::create(name, true, unit, Option::None)
  }

  pub fn non_nullable(name: String, unit: DateUnit) -> Date32Field {
    Date32Field::create(name, false, unit, Option::None)
  }

  pub fn with_metadata(name: String, unit: DateUnit, metadata: KeyValueMetadata) -> Date32Field {
    Date32Field::create(name, true, unit, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, unit: DateUnit, metadata: KeyValueMetadata) -> Date32Field {
    Date32Field::create(name, false, unit, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, unit: DateUnit, metadata: Option<KeyValueMetadata>) -> Date32Field {
    Date32Field {
      name,
      ty: Date32Type::new(unit),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Date32Field {
    Date32Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> Date32Field {
    Date32Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(Date32Field, as_date32);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Date64Field {
  name: String,
  ty: Date64Type,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl Date64Field {
  pub fn new(name: String, unit: DateUnit) -> Date64Field {
    Date64Field::create(name, true, unit, Option::None)
  }

  pub fn non_nullable(name: String, unit: DateUnit) -> Date64Field {
    Date64Field::create(name, false, unit, Option::None)
  }

  pub fn with_metadata(name: String, unit: DateUnit, metadata: KeyValueMetadata) -> Date64Field {
    Date64Field::create(name, true, unit, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, unit: DateUnit, metadata: KeyValueMetadata) -> Date64Field {
    Date64Field::create(name, false, unit, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, unit: DateUnit, metadata: Option<KeyValueMetadata>) -> Date64Field {
    Date64Field {
      name,
      ty: Date64Type::new(unit),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Date64Field {
    Date64Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> Date64Field {
    Date64Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(Date64Field, as_date64);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TimestampField {
  name: String,
  ty: TimestampType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl TimestampField {
  pub fn new(name: String) -> TimestampField {
    TimestampField::create(name, true, TimestampType::default_unit(), TimestampType::default_timezone(), Option::None)
  }

  pub fn non_nullable(name: String, unit: TimeUnit) -> TimestampField {
    TimestampField::create(name, false, unit, TimestampType::default_timezone(), Option::None)
  }

  pub fn with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> TimestampField {
    TimestampField::create(name, true, unit, TimestampType::default_timezone(), Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> TimestampField {
    TimestampField::create(name, false, unit, TimestampType::default_timezone(), Option::from(metadata))
  }

  fn create(name: String, nullable: bool, unit: TimeUnit, timezone: String, metadata: Option<KeyValueMetadata>) -> TimestampField {
    TimestampField {
      name,
      ty: TimestampType::with_unit_and_timezone(unit, timezone),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> TimestampField {
    TimestampField::create(self.name.clone(), self.nullable, self.ty.unit().clone(), self.ty.timezone().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> TimestampField {
    TimestampField::create(self.name.clone(), self.nullable, self.ty.unit().clone(), self.ty.timezone().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(TimestampField, as_timestamp);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Time32Field {
  name: String,
  ty: Time32Type,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl Time32Field {
  pub fn new(name: String) -> Time32Field {
    Time32Field::create(name, true, Time32Type::default_unit(), Option::None)
  }

  pub fn non_nullable(name: String, unit: TimeUnit) -> Time32Field {
    Time32Field::create(name, false, unit, Option::None)
  }

  pub fn with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> Time32Field {
    Time32Field::create(name, true, unit, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> Time32Field {
    Time32Field::create(name, false, unit, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, unit: TimeUnit, metadata: Option<KeyValueMetadata>) -> Time32Field {
    Time32Field {
      name,
      ty: Time32Type::with_unit(unit),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Time32Field {
    Time32Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> Time32Field {
    Time32Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(Time32Field, as_time32);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Time64Field {
  name: String,
  ty: Time64Type,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl Time64Field {
  pub fn new(name: String) -> Time64Field {
    Time64Field::create(name, true, Time64Type::default_unit(), Option::None)
  }

  pub fn non_nullable(name: String, unit: TimeUnit) -> Time64Field {
    Time64Field::create(name, false, unit, Option::None)
  }

  pub fn with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> Time64Field {
    Time64Field::create(name, true, unit, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, unit: TimeUnit, metadata: KeyValueMetadata) -> Time64Field {
    Time64Field::create(name, false, unit, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, unit: TimeUnit, metadata: Option<KeyValueMetadata>) -> Time64Field {
    Time64Field {
      name,
      ty: Time64Type::with_unit(unit),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Time64Field {
    Time64Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> Time64Field {
    Time64Field::create(self.name.clone(), self.nullable, self.ty.unit().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(Time64Field, as_time64);

define_field!(IntervalField, IntervalType);
impl_default_traits!(IntervalField, as_interval);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DecimalField {
  name: String,
  ty: DecimalType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl DecimalField {
  pub fn new(name: String, precision: i32, scale: i32) -> DecimalField {
    DecimalField::create(name, true, precision, scale, Option::None)
  }

  pub fn non_nullable(name: String, precision: i32, scale: i32) -> DecimalField {
    DecimalField::create(name, false, precision, scale, Option::None)
  }

  pub fn with_metadata(name: String, precision: i32, scale: i32, metadata: KeyValueMetadata) -> DecimalField {
    DecimalField::create(name, true, precision, scale, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, precision: i32, scale: i32, metadata: KeyValueMetadata) -> DecimalField {
    DecimalField::create(name, false, precision, scale, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, precision: i32, scale: i32, metadata: Option<KeyValueMetadata>) -> DecimalField {
    DecimalField {
      name,
      ty: DecimalType::new(precision, scale),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> DecimalField {
    DecimalField::create(self.name.clone(), self.nullable, self.ty.precision(), self.ty.scale(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> DecimalField {
    DecimalField::create(self.name.clone(), self.nullable, self.ty.precision(), self.ty.scale(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(DecimalField, as_decimal);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ListField {
  name: String,
  ty: ListType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl ListField {
  pub fn new(name: String, value_field: Box<DowncastField>) -> ListField {
    ListField::create(name, true, value_field, Option::None)
  }

  pub fn non_nullable(name: String, value_field: Box<DowncastField>) -> ListField {
    ListField::create(name, false, value_field, Option::None)
  }

  pub fn with_metadata(name: String, value_field: Box<DowncastField>, metadata: KeyValueMetadata) -> ListField {
    ListField::create(name, true, value_field, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, value_field: Box<DowncastField>, metadata: KeyValueMetadata) -> ListField {
    ListField::create(name, false, value_field, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, value_field: Box<DowncastField>, metadata: Option<KeyValueMetadata>) -> ListField {
    ListField {
      name,
      ty: ListType::new(value_field),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> ListField {
    ListField::create(self.name.clone(), self.nullable, self.ty.value_field().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> ListField {
    ListField::create(self.name.clone(), self.nullable, self.ty.value_field().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(ListField, as_list);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StructField {
  name: String,
  ty: StructType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl StructField {
  pub fn new(name: String, fields: Vec<Box<DowncastField>>) -> StructField {
    StructField::create(name, true, fields, Option::None)
  }

  pub fn non_nullable(name: String, fields: Vec<Box<DowncastField>>) -> StructField {
    StructField::create(name, false, fields, Option::None)
  }

  pub fn with_metadata(name: String, fields: Vec<Box<DowncastField>>, metadata: KeyValueMetadata) -> StructField {
    StructField::create(name, true, fields, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, fields: Vec<Box<DowncastField>>, metadata: KeyValueMetadata) -> StructField {
    StructField::create(name, false, fields, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, fields: Vec<Box<DowncastField>>, metadata: Option<KeyValueMetadata>) -> StructField {
    StructField {
      name,
      ty: StructType::new(fields),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> StructField {
    StructField::create(self.name.clone(), self.nullable, self.ty.get_children().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> StructField {
    StructField::create(self.name.clone(), self.nullable, self.ty.get_children().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(StructField, as_struct);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnionField {
  name: String,
  ty: UnionType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl UnionField {
  pub fn new(name: String, fields: Vec<Box<DowncastField>>, type_codes: Vec<u8>) -> UnionField {
    UnionField::create(name, true, fields, type_codes, Option::None)
  }

  pub fn non_nullable(name: String, fields: Vec<Box<DowncastField>>, type_codes: Vec<u8>) -> UnionField {
    UnionField::create(name, false, fields, type_codes, Option::None)
  }

  pub fn with_metadata(name: String, fields: Vec<Box<DowncastField>>, type_codes: Vec<u8>, metadata: KeyValueMetadata) -> UnionField {
    UnionField::create(name, true, fields, type_codes, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, fields: Vec<Box<DowncastField>>, type_codes: Vec<u8>, metadata: KeyValueMetadata) -> UnionField {
    UnionField::create(name, false, fields, type_codes, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, fields: Vec<Box<DowncastField>>, type_codes: Vec<u8>, metadata: Option<KeyValueMetadata>) -> UnionField {
    UnionField {
      name,
      ty: UnionType::new(fields, type_codes),
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> UnionField {
    UnionField::create(self.name.clone(), self.nullable, self.ty.get_children().clone(), self.ty.type_codes().clone(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> UnionField {
    UnionField::create(self.name.clone(), self.nullable, self.ty.get_children().clone(), self.ty.type_codes().clone(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(UnionField, as_union);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DictionaryField {
  name: String,
  ty: DictionaryType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl DictionaryField {
  pub fn new(name: String, index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool) -> DictionaryField {
    DictionaryField::create(name, true, index_type, dictionary, ordered, Option::None)
  }

  pub fn non_nullable(name: String, index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool) -> DictionaryField {
    DictionaryField::create(name, false, index_type, dictionary, ordered, Option::None)
  }

  pub fn with_metadata(name: String, index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool, metadata: KeyValueMetadata) -> DictionaryField {
    DictionaryField::create(name, true, index_type, dictionary, ordered, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool, metadata: KeyValueMetadata) -> DictionaryField {
    DictionaryField::create(name, false, index_type, dictionary, ordered, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, index_type: Box<Integer>, dictionary: Box<Array>, ordered: bool, metadata: Option<KeyValueMetadata>) -> DictionaryField {
    DictionaryField {
      name,
      ty: if ordered { DictionaryType::ordered(index_type, dictionary) } else { DictionaryType::unordered(index_type, dictionary) },
      nullable,
      metadata
    }
  }

  pub fn add_metadata(&self, metadata: KeyValueMetadata) -> DictionaryField {
    DictionaryField::create(self.name.clone(), self.nullable, self.ty.index_type().clone(), self.ty.dictionary().clone(), self.ty.is_ordered(), Option::from(metadata))
  }

  pub fn remove_metadata(&self) -> DictionaryField {
    DictionaryField::create(self.name.clone(), self.nullable, self.ty.index_type().clone(), self.ty.dictionary().clone(), self.ty.is_ordered(), Option::None)
  }

  pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }
}

impl_default_traits!(DictionaryField, as_dictionary);

//pub struct Field {
//    name: String,
//    ty: Box<DataType>,
//    nullable: bool,
//    metadata: Option<KeyValueMetadata>
//}
//
//impl Field {
//    pub fn new(name: String, ty: Box<DataType>) -> Field {
//        Field::create(name, ty, true, Option::None)
//    }
//
//    pub fn non_nullable(name: String, ty: Box<DataType>) -> Field {
//        Field::create(name, ty, false, Option::None)
//    }
//
//    pub fn with_metadata(name: String, ty: Box<DataType>, metadata: KeyValueMetadata) -> Field {
//        Field::create(name, ty, true, Option::from(metadata))
//    }
//
//    pub fn non_nullable_with_metadata(name: String, ty: Box<DataType>, metadata: KeyValueMetadata) -> Field {
//        Field::create(name, ty, false, Option::from(metadata))
//    }
//
//    fn create(name: String, ty: Box<DataType>, nullable: bool, metadata: Option<KeyValueMetadata>) -> Field {
//        Field {
//            name,
//            ty,
//            nullable,
//            metadata
//        }
//    }
//
//    pub fn get_name(&self) -> &String {
//        &self.name
//    }
//
//    pub fn get_type(&self) -> &DataType {
//        self.ty.as_ref()
//    }
//
//    pub fn nullable(&self) -> bool {
//        self.nullable
//    }
//
//    pub fn add_metadata(&self, metadata: KeyValueMetadata) -> Field {
//        Field::create(self.name.clone(), self.ty.clone(), self.nullable, Option::from(metadata))
//    }
//
//    pub fn remove_metadata(&self) -> Field {
//        Field::create(self.name.clone(), self.ty.clone(), self.nullable, Option::None)
//    }
//
//    pub fn get_metadata(&self) -> &Option<KeyValueMetadata> {
//        &self.metadata
//    }
//}
//
//impl PartialEq for Field {
//    fn eq(&self, other: &Field) -> bool {
//        self.name == other.name &&
//            self.ty.as_ref() == other.ty.as_ref() &&
//            self.nullable == other.nullable &&
//            self.metadata == other.metadata
//    }
//}
//
//impl Eq for Field {
//
//}
//
//impl Debug for Field {
//    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//        f.debug_struct("Field")
//            .field("name", &self.name)
//            //        .field("ty")
//            .field("nullable", &self.nullable)
//            .field("metadata", &self.metadata)
//            .finish()
//    }
//}
//
//impl Clone for Field {
//    fn clone(&self) -> Self {
//        unimplemented!()
//    }
//}

//impl ToString for Field {
//  fn to_string(&self) -> String {
//    let str = self.name.clone() + ": " + self.ty.to_string().as_str();
//    if self.nullable {
//      str + " not null"
//    } else {
//      str
//    }
//  }
//}