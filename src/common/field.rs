use common::KeyValueMetadata;
use common::ty::*;
use array::Array;

use std::fmt::{Debug, Formatter, Error};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Field<'a> {
  name: String,
  data_type: Ty<'a>,
  nullable: bool,
  metadata: Option<KeyValueMetadata>
}

impl <'a> Field<'a> {
  pub fn new(name: String, data_type: Ty<'a>) -> Field<'a> {
    Field {
      name,
      data_type,
      nullable: true,
      metadata: None
    }
  }

  pub fn non_null(name: String, data_type: Ty<'a>) -> Field<'a> {
    Field {
      name,
      data_type,
      nullable: false,
      metadata: None
    }
  }

  pub fn new_with_metadata(name: String, data_type: Ty<'a>, metadata: KeyValueMetadata) -> Field<'a> {
    Field {
      name,
      data_type,
      nullable: true,
      metadata: Some(metadata)
    }
  }

  pub fn non_null_with_metadata(name: String, data_type: Ty<'a>, metadata: KeyValueMetadata) -> Field<'a> {
    Field {
      name,
      data_type,
      nullable: false,
      metadata: Some(metadata)
    }
  }

  #[inline]
  pub fn name(&self) -> &String {
    &self.name
  }

  #[inline]
  pub fn data_type(&self) -> &Ty {
    &self.data_type
  }

  #[inline]
  pub fn nullable(&self) -> bool {
    self.nullable
  }

  pub fn metadata(&self) -> &Option<KeyValueMetadata> {
    &self.metadata
  }

  pub fn with_metadata(&self, metadata: KeyValueMetadata) -> Field<'a> {
    Field {
      name: self.name.clone(),
      data_type: self.data_type.clone(),
      nullable: self.nullable,
      metadata: Some(metadata)
    }
  }

  pub fn without_metadata(&self) -> Field<'a> {
    Field {
      name: self.name.clone(),
      data_type: self.data_type.clone(),
      nullable: self.nullable,
      metadata: None
    }
  }
}
