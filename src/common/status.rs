#[derive(Debug, Eq, PartialEq)]
pub enum StatusCode {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,

  UnknownError = 9,
  NotImplemented = 10,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ArrowError {
  code: StatusCode,
  posix_code: i16,
  message: String
}

impl ArrowError {
  pub fn out_of_memory(message: String) -> ArrowError {
    ArrowError::new(StatusCode::OutOfMemory, message, -1)
  }

  pub fn key_error(message: String) -> ArrowError {
    ArrowError::new(StatusCode::KeyError, message, -1)
  }

  pub fn type_error(message: String) -> ArrowError {
    ArrowError::new(StatusCode::TypeError, message, -1)
  }

  pub fn invalid(message: String) -> ArrowError {
    ArrowError::new(StatusCode::Invalid, message, -1)
  }

  pub fn io_error(message: String) -> ArrowError {
    ArrowError::new(StatusCode::IOError, message, -1)
  }

  pub fn unknown_error(message: String) -> ArrowError {
    ArrowError::new(StatusCode::UnknownError, message, -1)
  }

  pub fn not_implemented(message: String) -> ArrowError {
    ArrowError::new(StatusCode::NotImplemented, message, -1)
  }

  fn new(code: StatusCode, message: String, posix_code: i16) -> ArrowError {
    ArrowError {
      code: code,
      posix_code: posix_code,
      message: message
    }
  }

  pub fn code(&self) -> &StatusCode {
    &self.code
  }

  pub fn posix_code(&self) -> i16 {
    self.posix_code
  }

  pub fn message(&self) -> &String {
    &self.message
  }
}

#[cfg(test)]
mod tests {
  use common::status::{StatusCode, ArrowError};

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
}
