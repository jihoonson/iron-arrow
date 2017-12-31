use common::status::ArrowError;
use common::bit_util;
use common::ty;
use common::ty::Ty;
use memory_pool::MemoryPool;
use buffer::{Buffer, PoolBuffer};

use std::ptr;
use std::mem;

// Should not be generic because it's used like Vec<ArrayBuilder>
pub enum ArrayBuilder {

}