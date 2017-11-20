use common::KeyValueMetadata;
use common::ty::{Ty, DowncastDataType, NullType, FloatType, Int64Type, ListType, TimestampType};

use std::fmt::{Debug, Formatter, Error};

pub trait Field {
  fn get_name(&self) -> &String;
  fn get_type(&self) -> &DowncastDataType;
  fn nullable(&self) -> bool;
}

impl Debug for Box<Field> {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    unimplemented!()
  }
}

impl PartialEq for Box<Field> {
  fn eq(&self, other: &Box<Field>) -> bool {
    unimplemented!()
  }
}

impl Eq for Box<Field> {

}

impl Clone for Box<Field> {
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
    };
}

define_field!(NullField, NullType);
define_field!(FloatField, FloatType);
define_field!(Int64Field, Int64Type);
define_field!(TimestampField, TimestampType);

pub struct ListField {
  name: String,
  ty: ListType,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl ListField {
  pub fn new(name: String, value_field: Box<Field>) -> ListField {
    ListField::create(name, true, value_field, Option::None)
  }

  pub fn non_nullable(name: String, value_field: Box<Field>) -> ListField {
    ListField::create(name, false, value_field, Option::None)
  }

  pub fn with_metadata(name: String, value_field: Box<Field>, metadata: KeyValueMetadata) -> ListField {
    ListField::create(name, true, value_field, Option::from(metadata))
  }

  pub fn non_nullable_with_metadata(name: String, value_field: Box<Field>, metadata: KeyValueMetadata) -> ListField {
    ListField::create(name, false, value_field, Option::from(metadata))
  }

  fn create(name: String, nullable: bool, value_field: Box<Field>, metadata: Option<KeyValueMetadata>) -> ListField {
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

impl Field for ListField {
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