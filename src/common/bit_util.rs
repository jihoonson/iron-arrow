use std::i64;

static ROUND_TO: i64 = 64;
static FORCE_CARRY_ADDEND: i64 = 64 - 1;
static TRUNCATE_BITMASK: i64 = !(64 - 1);
static MAX_ROUNDABLE_NUM: i64 = i64::MAX - 64;

#[inline]
pub fn round_up_to_multiple_of_64(val: i64) -> i64 {
  if val <= MAX_ROUNDABLE_NUM {
    (val + FORCE_CARRY_ADDEND) & TRUNCATE_BITMASK
  } else {
    val
  }
}

#[inline]
pub fn next_power_2(val: i64) -> i64 {
  let mut n = val - 1;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}