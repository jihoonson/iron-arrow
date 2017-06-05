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