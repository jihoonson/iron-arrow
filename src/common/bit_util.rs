use std::i64;
use std::mem;
use std::cmp;
use std::ptr;
use std::intrinsics;

const ROUND_TO: i64 = 64;
const FORCE_CARRY_ADDEND: i64 = 64 - 1;
const TRUNCATE_BITMASK: i64 = !(64 - 1);
const MAX_ROUNDABLE_NUM: i64 = i64::MAX - 64;

const K_BITMASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

#[inline]
pub fn round_up_to_multiple_of_64(val: i64) -> i64 {
  if val <= MAX_ROUNDABLE_NUM {
    (val + FORCE_CARRY_ADDEND) & TRUNCATE_BITMASK
  } else {
    val
  }
}

#[inline]
pub fn round_up(value: i64, factor: i64) -> i64 {
  (value + (factor - 1)) / factor * factor
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

#[inline]
pub fn bit_not_set(bits: *const u8, i: i64) -> bool {
  (unsafe { *bits.offset(i as isize / 8) as u8 } & K_BITMASK[i as usize % 8]) == 0
}

#[inline]
pub fn get_bit(bits: *const u8, i: i64) -> bool {
  (unsafe { *bits.offset(i as isize / 8) as u8 } & K_BITMASK[i as usize % 8]) != 0
}

const pop_len: i64 = (mem::size_of::<i64>() * 8) as i64;

pub fn count_set_bits(data: *const u8, bit_offset: i64, len: i64) -> i64 {
  let mut count: i64 = 0;

  // The first bit offset where we can use a 64-bit wide hardware popcount
  let fast_count_start = round_up(bit_offset, pop_len);

  // The number of bits until fast_count_start
  let initial_bits = cmp::min(len, fast_count_start - bit_offset);
  for i in bit_offset..(bit_offset + initial_bits) {
    count = if get_bit(data, i) {
      count + 1
    } else {
      count
    };
  }

  let fast_counts = (len - initial_bits) / pop_len;

  let u64_data: *const u64 = unsafe { mem::transmute::<*const u8, *const u64>(data).offset((fast_count_start / pop_len) as isize) };

  // popcount as much as possible with the widest possible count
  for i in 0..fast_counts {
    count = count + unsafe { intrinsics::ctpop(u64_data.offset(i as isize).read()) } as i64;
  }

  // Account for left over bit (in theory we could fall back to smaller
  // versions of popcount but the code complexity is likely not worth it)
  let tail_index = bit_offset + initial_bits + fast_counts * pop_len;
  for i in tail_index..bit_offset + len {
    count = if get_bit(data, i) {
      count + 1
    } else {
      count
    };
  }

  count
}

#[cfg(test)]
mod test {
  #[test]
  fn test_git_bit() {
    use common::bit_util::get_bit;


  }

  #[test]
  fn test_next_power_2() {
    use common::bit_util::next_power_2;

    assert_eq!(8, next_power_2(6));
    assert_eq!(8, next_power_2(8));

    assert_eq!(1, next_power_2(1));
    assert_eq!(256, next_power_2(131));

    assert_eq!(1024, next_power_2(1000));

    assert_eq!(4096, next_power_2(4000));

    assert_eq!(65536, next_power_2(64000));

    let i = (1 as u64).rotate_left(32) as i64;
    assert_eq!(i, next_power_2(i - 1));
    let i = (1 as u64).rotate_left(31) as i64;
    assert_eq!(i, next_power_2(i - 1));
    let i = (1 as u64).rotate_left(62) as i64;
    assert_eq!(i, next_power_2(i - 1));
  }

  #[test]
  fn test_count_set_bits() {
    use common::bit_util::count_set_bits;

    let k_buf_size = 1000;
    let mut buf: [u8; 1000] = [0; 1000];
    random_bytes(&mut buf);

    let p = buf.as_ptr();

    let num_bits = (k_buf_size * 8) as i64;

    let offsets: Vec<i64> = vec![0, 12, 16, 32, 37, 63, 64, 128, num_bits - 30, num_bits - 64];

    for offset in offsets {
      let result = count_set_bits(p, offset, num_bits - offset);
      let expected = slow_count_bits(p, offset, num_bits - offset);

      assert_eq!(expected, result);
    }
  }

  #[inline]
  fn random_bytes(buf: &mut [u8]) {
    use rand;

    for i in 0..buf.len() {
      buf[i] = rand::random::<u8>();
    }
  }

  #[inline]
  fn slow_count_bits(data: *const u8, bit_offset: i64, len: i64) -> i64 {
    use common::bit_util::get_bit;

    let mut count: i64 = 0;
    for i in bit_offset..bit_offset + len {
      count = if get_bit(data, i) {
        count + 1
      } else {
        count
      };
    }
    count
  }
}