// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::mem::{size_of, transmute_copy};
use std::cmp;

use errors::{Result, ParquetError};
use util::memory::{Buffer, BufferRange};

/// Reads `$size` of bytes from `$src`, and reinterprets them as type `$ty`, in
/// little-endian order. `$ty` must implement the `Default` trait. Otherwise this won't
/// compile.
/// This is copied and modified from byteorder crate.
macro_rules! read_num_bytes {
  ($ty:ty, $size:expr, $src:expr) => ({
    assert!($size <= $src.len());
    let mut data: $ty = Default::default();
    unsafe {
      ::std::ptr::copy_nonoverlapping(
        $src.as_ptr(),
        &mut data as *mut $ty as *mut u8,
        $size);
    }
    data
  });
}

/// Converts value `val` of type `T` to a byte vector, by reading `num_bytes` from `val`.
/// NOTE: if `val` is less than the size of `T` then it can be truncated.
#[inline]
pub fn convert_to_bytes<T>(val: &T, num_bytes: usize) -> Vec<u8> {
  let mut bytes: Vec<u8> = vec![0; num_bytes];
  memcpy_value(val, num_bytes, &mut bytes);
  bytes
}

#[inline]
pub fn memcpy(source: &[u8], target: &mut [u8]) {
  assert!(target.len() >= source.len(),
          "target.len() ({}) < source.len() ({})",
          target.len(), source.len());
  unsafe {
    ::std::ptr::copy_nonoverlapping(
      source.as_ptr(),
      target.as_mut_ptr(),
      source.len())
  }
}

#[inline]
pub fn memcpy_value<T>(source: &T, num_bytes: usize, target: &mut [u8]) {
  assert!(
    target.len() >= num_bytes,
    "Not enough space. Only had {} bytes but need to put {} bytes",
    target.len(), num_bytes);
  unsafe {
    ::std::ptr::copy_nonoverlapping(
      source as *const T as *const u8,
      target.as_mut_ptr(),
      num_bytes
    )
  }
}

/// Returns the ceil of value/divisor
#[inline]
pub fn ceil(value: i64, divisor: i64) -> i64 {
  let mut result = value / divisor;
  if value % divisor != 0 { result += 1 };
  result
}

/// Returns ceil(log2(x))
#[inline]
pub fn log2(mut x: u64) -> i32 {
  if x == 1 {
    return 0;
  }
  x -= 1;
  let mut result = 0;
  while x > 0 {
    x >>= 1;
    result += 1;
  }
  result
}

/// Returns the `num_bits` least-significant bits of `v`
#[inline]
pub fn trailing_bits(v: u64, num_bits: usize) -> u64 {
  if num_bits == 0 { return 0; }
  if num_bits >= 64 { return v; }
  let n = 64 - num_bits;
  (v << n) >> n
}

#[inline]
pub fn set_array_bit(bits: &mut [u8], i: usize) {
  bits[i / 8] |= 1 << (i % 8);
}

#[inline]
pub fn unset_array_bit(bits: &mut [u8], i: usize) {
  bits[i / 8] &= !(1 << (i % 8));
}

/// Returns the minimum number of bits needed to represent the value 'x'
#[inline]
pub fn num_required_bits(x: u64) -> usize {
  for i in (0..64).rev() {
    if x & (1u64 << i) != 0 {
      return i + 1
    }
  }
  0
}


/// Utility class for writing bit/byte streams. This class can write data in either
/// bit packed or byte aligned fashion.
pub struct BitWriter {
  buffer: Buffer,
  max_bytes: usize,
  buffered_values: u64,
  byte_offset: usize,
  bit_offset: usize,
}

impl BitWriter {
  /// Initialize the writer from the existing buffer `buffer`.
  pub fn new(buffer: Buffer) -> Self {
    let len = buffer.len();
    Self {
      buffer: buffer,
      max_bytes: len,
      buffered_values: 0,
      byte_offset: 0,
      bit_offset: 0
    }
  }

  /// Consumes and returns the current buffer.
  #[inline]
  pub fn consume(mut self) -> Buffer {
    self.flush();
    self.buffer.range(..self.byte_offset)
  }

  /// Clears the internal state so the buffer can be reused.
  #[inline]
  pub fn clear(&mut self) {
    self.buffered_values = 0;
    self.byte_offset = 0;
    self.bit_offset = 0;
  }

  /// Flushes the internal buffered bits and the align the buffer to the next byte.
  #[inline]
  pub fn flush(&mut self) {
    let num_bytes = ceil(self.bit_offset as i64, 8) as usize;
    assert!(self.byte_offset + num_bytes <= self.max_bytes);
    memcpy_value(
      &self.buffered_values, num_bytes,
      &mut self.buffer.data_mut()[self.byte_offset..]);
    self.buffered_values = 0;
    self.bit_offset = 0;
    self.byte_offset += num_bytes;
  }

  /// Advances the current offset by skipping `num_bytes`, flushing the internal bit
  /// buffer first.
  /// This is useful when you want to jump over `num_bytes` bytes and come back later
  /// to fill these bytes.
  ///
  /// Returns error if `num_bytes` is beyond the boundary of the internal buffer.
  /// Otherwise, returns the old offset.
  #[inline]
  pub fn skip(&mut self, num_bytes: usize) -> Result<usize> {
    self.flush();
    assert!(self.byte_offset <= self.max_bytes);
    if self.byte_offset + num_bytes > self.max_bytes {
      return Err(general_err!(
        "Not enough bytes left in BitWriter. Need {} but only have {}",
        self.byte_offset + num_bytes, self.max_bytes));
    }
    let result = self.byte_offset;
    self.byte_offset += num_bytes;
    Ok(result)
  }

  /// Returns a slice containing the next `num_bytes` bytes starting from the current
  /// offset, and advances the underlying buffer by `num_bytes`.
  /// This is useful when you want to jump over `num_bytes` bytes and come back later
  /// to fill these bytes.
  #[inline]
  pub fn get_next_byte_ptr(&mut self, num_bytes: usize) -> Result<&mut [u8]> {
    let offset = self.skip(num_bytes)?;
    Ok(&mut self.buffer.data_mut()[offset..offset + num_bytes])
  }

  #[inline]
  pub fn bytes_written(&self) -> usize {
    self.byte_offset + ceil(self.bit_offset as i64, 8) as usize
  }

  #[inline]
  pub fn buffer(&self) -> &[u8] {
    self.buffer.data()
  }

  #[inline]
  pub fn byte_offset(&self) -> usize {
    self.byte_offset
  }

  /// Returns the internal buffer length. This is the maximum number of bytes that this
  /// writer can write. User needs to call `consume` to consume the current buffer before
  /// more data can be written.
  #[inline]
  pub fn buffer_len(&self) -> usize {
    self.max_bytes
  }

  /// Writes the `num_bits` LSB of value `v` to the internal buffer of this writer.
  /// The `num_bits` must not be greater than 64. This is bit packed.
  ///
  /// Returns false if there's not enough room left. True otherwise.
  #[inline]
  pub fn put_value(&mut self, v: u64, num_bits: usize) -> bool {
    assert!(num_bits <= 64);
    assert_eq!(v.checked_shr(num_bits as u32).unwrap_or(0), 0); // covers case v >> 64

    if self.byte_offset * 8 + self.bit_offset + num_bits > self.max_bytes as usize * 8 {
      return false;
    }

    self.buffered_values |= v << self.bit_offset;
    self.bit_offset += num_bits;
    if self.bit_offset >= 64 {
      memcpy_value(
        &self.buffered_values, 8,
        &mut self.buffer.data_mut()[self.byte_offset..]);
      self.byte_offset += 8;
      self.bit_offset -= 64;
      self.buffered_values = 0;
      // Perform checked right shift: v >> offset, where offset < 64, otherwise we shift
      // all bits
      self.buffered_values = v.checked_shr(
        (num_bits - self.bit_offset) as u32).unwrap_or(0);
    }
    assert!(self.bit_offset < 64);
    true
  }

  /// Writes `val` of `num_bytes` bytes to the next aligned byte. If size of `T` is
  /// larger than `num_bytes`, extra higher ordered bytes will be ignored.
  ///
  /// Returns false if there's not enough room left. True otherwise.
  #[inline]
  pub fn put_aligned<T: Copy>(&mut self, val: T, num_bytes: usize) -> bool {
    let result = self.get_next_byte_ptr(num_bytes);
    if result.is_err() {
      // TODO: should we return `Result` for this func?
      return false
    }
    let mut ptr = result.unwrap();
    memcpy_value(&val, num_bytes, &mut ptr);
    true
  }

  /// Writes `val` of `num_bytes` bytes at the designated `offset`. The `offset` is the
  /// offset starting from the beginning of the internal buffer that this writer
  /// maintains. Note that this will overwrite any existing data between `offset` and
  /// `offset + num_bytes`. Also that if size of `T` is larger than `num_bytes`, extra
  /// higher ordered bytes will be ignored.
  ///
  /// Returns false if there's not enough room left, or the `pos` is not valid.
  /// True otherwise.
  #[inline]
  pub fn put_aligned_offset<T: Copy>(
    &mut self, val: T, num_bytes: usize, offset: usize
  ) -> bool {
    if num_bytes + offset > self.max_bytes {
      return false
    }
    memcpy_value(
      &val, num_bytes,
      &mut self.buffer.data_mut()[offset..offset + num_bytes]);
    true
  }

  /// Writes a VLQ encoded integer `v` to this buffer. The value is byte aligned.
  ///
  /// Returns false if there's not enough room left. True otherwise.
  #[inline]
  pub fn put_vlq_int(&mut self, mut v: u64) -> bool {
    let mut result = true;
    while v & 0xFFFFFFFFFFFFFF80 != 0 {
      result &= self.put_aligned::<u8>(((v & 0x7F) | 0x80) as u8, 1);
      v >>= 7;
    }
    result &= self.put_aligned::<u8>((v & 0x7F) as u8, 1);
    result
  }

  /// Writes a zigzag-VLQ encoded (in little endian order) int `v` to this buffer.
  /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive
  /// numbers are encoded in a zigzag fashion.
  /// See: https://developers.google.com/protocol-buffers/docs/encoding
  ///
  /// Returns false if there's not enough room left. True otherwise.
  #[inline]
  pub fn put_zigzag_vlq_int(&mut self, v: i64) -> bool {
    let u: u64 = ((v << 1) ^ (v >> 63)) as u64;
    self.put_vlq_int(u)
  }
}

impl From<Vec<u8>> for BitWriter {
  fn from(v: Vec<u8>) -> BitWriter {
    BitWriter::new(Buffer::from(v))
  }
}


/// Maximum byte length for a VLQ encoded integer
/// MAX_VLQ_BYTE_LEN = 5 for i32, and MAX_VLQ_BYTE_LEN = 10 for i64
pub const MAX_VLQ_BYTE_LEN: usize = 10;

pub struct BitReader {
  // The byte buffer to read from, passed in by client
  buffer: Buffer,

  // Bytes are memcpy'd from `buffer` and values are read from this variable.
  // This is faster than reading values byte by byte directly from `buffer`
  buffered_values: u64,

  //
  // End                                         Start
  // |............|B|B|B|B|B|B|B|B|..............|
  //                   ^          ^
  //                 bit_offset   byte_offset
  //
  // Current byte offset in `buffer`
  byte_offset: usize,

  // Current bit offset in `buffered_values`
  bit_offset: usize,

  // Total number of bytes in `buffer`
  total_bytes: usize
}

/// Utility class to read bit/byte stream. This class can read bits or bytes that are
/// either byte aligned or not.
impl BitReader {
  pub fn new(buffer: Buffer) -> Self {
    let total_bytes = buffer.len();
    let num_bytes = cmp::min(8, total_bytes);
    let buffered_values = read_num_bytes!(u64, num_bytes, buffer.data());
    BitReader {
      buffer: buffer, buffered_values: buffered_values,
      byte_offset: 0, bit_offset: 0, total_bytes: total_bytes
    }
  }

  #[inline]
  pub fn reset(&mut self, buffer: Buffer) {
    self.buffer = buffer;
    self.total_bytes = self.buffer.len();
    let num_bytes = cmp::min(8, self.total_bytes);
    self.buffered_values = read_num_bytes!(u64, num_bytes, self.buffer.data());
    self.byte_offset = 0;
    self.bit_offset = 0;
  }

  /// Gets the current byte offset
  #[inline]
  pub fn get_byte_offset(&self) -> usize {
    self.byte_offset + ceil(self.bit_offset as i64, 8) as usize
  }

  /// Reads a value of type `T` and of size `num_bits`.
  ///
  /// Returns `None` if there's not enough data available. `Some` otherwise.
  #[inline]
  pub fn get_value<T: Default>(&mut self, num_bits: usize) -> Option<T> {
    assert!(num_bits <= 64, "num_bits ({}) must be <= 64", num_bits);
    assert!(num_bits <= size_of::<T>() * 8);

    if self.byte_offset * 8 + self.bit_offset + num_bits > self.total_bytes * 8 {
      return None;
    }

    let mut v = trailing_bits(
      self.buffered_values, self.bit_offset + num_bits
    ) >> self.bit_offset;
    self.bit_offset += num_bits;

    if self.bit_offset >= 64 {
      self.byte_offset += 8;
      self.bit_offset -= 64;

      let bytes_to_read = cmp::min(self.total_bytes - self.byte_offset, 8);
      self.buffered_values = read_num_bytes!(
        u64, bytes_to_read, self.buffer.data()[self.byte_offset..]);

      v |= trailing_bits(self.buffered_values, self.bit_offset)
        .wrapping_shl((num_bits - self.bit_offset) as u32);
    }

    // TODO: better to avoid copying here
    let result: T = unsafe {
      transmute_copy::<u64, T>(&v)
    };
    Some(result)
  }

  /// Reads a `num_bytes`-sized value from this buffer and return it.
  /// `T` needs to be a little-endian native type. The value is assumed to be byte
  /// aligned so the bit reader will be advanced to the start of the next byte before
  /// reading the value.

  /// Returns `Some` if there's enough bytes left to form a value of `T`.
  /// Otherwise `None`.
  #[inline]
  pub fn get_aligned<T: Default>(&mut self, num_bytes: usize) -> Option<T> {
    let bytes_read = ceil(self.bit_offset as i64, 8) as usize;
    if self.byte_offset + bytes_read + num_bytes > self.total_bytes {
      return None;
    }

    // Advance byte_offset to next unread byte and read num_bytes
    self.byte_offset += bytes_read;
    let v = read_num_bytes!(T, num_bytes, self.buffer.data()[self.byte_offset..]);
    self.byte_offset += num_bytes;

    // Reset buffered_values
    self.bit_offset = 0;
    let bytes_remaining = cmp::min(self.total_bytes - self.byte_offset, 8);
    self.buffered_values = read_num_bytes!(
      u64, bytes_remaining, self.buffer.data()[self.byte_offset..]);
    Some(v)
  }

  /// Reads a VLQ encoded (in little endian order) int from the stream.
  /// The encoded int must start at the beginning of a byte.
  ///
  /// Returns `None` if there's not enough bytes in the stream. `Some` otherwise.
  #[inline]
  pub fn get_vlq_int(&mut self) -> Option<i64> {
    let mut shift = 0;
    let mut v: i64 = 0;
    while let Some(byte) = self.get_aligned::<u8>(1) {
      v |= ((byte & 0x7F) as i64) << shift;
      shift += 7;
      assert!(shift <= MAX_VLQ_BYTE_LEN * 7,
        "Num of bytes exceed MAX_VLQ_BYTE_LEN ({})", MAX_VLQ_BYTE_LEN);
      if byte & 0x80 == 0 {
        return Some(v);
      }
    }
    None
  }

  /// Reads a zigzag-VLQ encoded (in little endian order) int from the stream
  /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive numbers are
  /// encoded in a zigzag fashion.
  /// See: https://developers.google.com/protocol-buffers/docs/encoding
  ///
  /// Note: the encoded int must start at the beginning of a byte.
  ///
  /// Returns `None` if the number of bytes there's not enough bytes in the stream.
  /// `Some` otherwise.
  #[inline]
  pub fn get_zigzag_vlq_int(&mut self) -> Option<i64> {
    self.get_vlq_int().map(|v| {
      let u = v as u64;
      ((u >> 1) as i64 ^ -((u & 1) as i64))
    })
  }
}

impl From<Vec<u8>> for BitReader {
  fn from(v: Vec<u8>) -> BitReader {
    BitReader::new(Buffer::from(v))
  }
}


#[cfg(test)]
mod tests {
  use std::fmt::Debug;
  use rand::Rand;

  use super::*;
  use super::super::memory::Buffer;
  use super::super::test_common::*;

  #[test]
  fn test_ceil() {
    assert_eq!(ceil(0, 1), 0);
    assert_eq!(ceil(1, 1), 1);
    assert_eq!(ceil(1, 2), 1);
    assert_eq!(ceil(1, 8), 1);
    assert_eq!(ceil(7, 8), 1);
    assert_eq!(ceil(8, 8), 1);
    assert_eq!(ceil(9, 8), 2);
    assert_eq!(ceil(9, 9), 1);
    assert_eq!(ceil(10000000000, 10), 1000000000);
    assert_eq!(ceil(10, 10000000000), 1);
    assert_eq!(ceil(10000000000, 1000000000), 10);
  }

  #[test]
  fn test_bit_reader_get_byte_offset() {
    let mut v = vec![255; 10];
    let mut bit_reader = BitReader::from(&mut v[..]);
    assert_eq!(bit_reader.get_byte_offset(), 0); // offset (0 bytes, 0 bits)
    bit_reader.get_value::<i32>(6);
    assert_eq!(bit_reader.get_byte_offset(), 1); // offset (0 bytes, 6 bits)
    bit_reader.get_value::<i32>(10);
    assert_eq!(bit_reader.get_byte_offset(), 2); // offset (0 bytes, 16 bits)
    bit_reader.get_value::<i32>(20);
    assert_eq!(bit_reader.get_byte_offset(), 5); // offset (0 bytes, 36 bits)
    bit_reader.get_value::<i32>(30);
    assert_eq!(bit_reader.get_byte_offset(), 9); // offset (8 bytes, 2 bits)
  }

  #[test]
  fn test_bit_reader_get_value() {
    let v = vec![255, 0];
    let mut bit_reader = BitReader::from(v);
    assert_eq!(bit_reader.get_value::<i32>(1), Some(1));
    assert_eq!(bit_reader.get_value::<i32>(2), Some(3));
    assert_eq!(bit_reader.get_value::<i32>(3), Some(7));
    assert_eq!(bit_reader.get_value::<i32>(4), Some(3));
  }

  #[test]
  fn test_bit_reader_get_value_boundary() {
    let v = vec![10, 0, 0, 0, 20, 0, 30, 0, 0, 0, 40, 0];
    let mut bit_reader = BitReader::from(v);
    assert_eq!(bit_reader.get_value::<i64>(32), Some(10));
    assert_eq!(bit_reader.get_value::<i64>(16), Some(20));
    assert_eq!(bit_reader.get_value::<i64>(32), Some(30));
    assert_eq!(bit_reader.get_value::<i64>(16), Some(40));
  }

  // TODO: enable this.
  fn test_bit_reader_get_aligned() {
    // 01110101 11001011
    let v = vec!(0x75, 0xCB);
    let v1 = v.clone();
    let mut bit_reader = BitReader::from(v);
    assert_eq!(bit_reader.get_value::<i32>(3), Some(5));
    assert_eq!(bit_reader.get_aligned::<i32>(1), Some(203));
    assert_eq!(bit_reader.get_value::<i32>(1), None);
    bit_reader.reset(Buffer::from(v1));
    assert_eq!(bit_reader.get_aligned::<i32>(3), None);
  }

  #[test]
  fn test_bit_reader_get_vlq_int() {
    // 10001001 00000001 11110010 10110101 00000110
    let v = vec!(0x89, 0x01, 0xF2, 0xB5, 0x06);
    let mut bit_reader = BitReader::from(v);
    assert_eq!(bit_reader.get_vlq_int(), Some(137));
    assert_eq!(bit_reader.get_vlq_int(), Some(105202));
  }

  #[test]
  fn test_bit_reader_get_zigzag_vlq_int() {
    let v = vec!(0, 1, 2, 3);
    let mut bit_reader = BitReader::from(v);
    assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(0));
    assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-1));
    assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(1));
    assert_eq!(bit_reader.get_zigzag_vlq_int(), Some(-2));
  }

  #[test]
  fn test_set_array_bit() {
    let mut buffer = vec![0, 0, 0];
    set_array_bit(&mut buffer[..], 1);
    assert_eq!(buffer, vec![2, 0, 0]);
    set_array_bit(&mut buffer[..], 4);
    assert_eq!(buffer, vec![18, 0, 0]);
    unset_array_bit(&mut buffer[..], 1);
    assert_eq!(buffer, vec![16, 0, 0]);
    set_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 4, 0]);
    set_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 4, 0]);
    set_array_bit(&mut buffer[..], 11);
    assert_eq!(buffer, vec![16, 12, 0]);
    unset_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 8, 0]);
  }

  #[test]
  fn test_num_required_bits() {
    assert_eq!(num_required_bits(0), 0);
    assert_eq!(num_required_bits(1), 1);
    assert_eq!(num_required_bits(2), 2);
    assert_eq!(num_required_bits(4), 3);
    assert_eq!(num_required_bits(8), 4);
    assert_eq!(num_required_bits(10), 4);
    assert_eq!(num_required_bits(12), 4);
    assert_eq!(num_required_bits(16), 5);
  }

  #[test]
  fn test_log2() {
    assert_eq!(log2(1), 0);
    assert_eq!(log2(2), 1);
    assert_eq!(log2(3), 2);
    assert_eq!(log2(4), 2);
    assert_eq!(log2(5), 3);
    assert_eq!(log2(5), 3);
    assert_eq!(log2(6), 3);
    assert_eq!(log2(7), 3);
    assert_eq!(log2(8), 3);
    assert_eq!(log2(9), 4);
  }

  #[test]
  fn test_skip() {
    let mut v = vec![0; 5];
    let mut writer = BitWriter::from(v);
    let old_offset = writer.skip(1).expect("skip() should return OK");
    writer.put_aligned(42, 4);
    writer.put_aligned_offset(0x10, 1, old_offset);
    let result = writer.consume();
    assert_eq!(result.data(), [0x10, 42, 0, 0, 0]);

    v = vec![0; 4];
    writer = BitWriter::from(v);
    let result = writer.skip(5);
    assert!(result.is_err());
  }

  #[test]
  fn test_get_next_byte_ptr() {
    let v = vec![0; 5];
    let mut writer = BitWriter::from(v);
    {
      let first_byte = writer.get_next_byte_ptr(1)
        .expect("get_next_byte_ptr() should return OK");
      first_byte[0] = 0x10;
    }
    writer.put_aligned(42, 4);
    let result = writer.consume();
    assert_eq!(result.data(), [0x10, 42, 0, 0, 0]);
  }

  #[test]
  fn test_put_get_bool() {
    let v = vec![0; 8];
    let mut writer = BitWriter::from(v);

    for i in 0..8 {
      let result = writer.put_value(i % 2, 1);
      assert!(result);
    }

    writer.flush();
    {
      let buffer = writer.buffer();
      assert_eq!(buffer[0], 0b10101010);
    }

    // Write 00110011
    for i in 0..8 {
      let result = match i {
        0 | 1 | 4 | 5 => writer.put_value(false as u64, 1),
        _ => writer.put_value(true as u64, 1)
      };
      assert!(result);
    }
    writer.flush();
    {
      let buffer = writer.buffer();
      assert_eq!(buffer[0], 0b10101010);
      assert_eq!(buffer[1], 0b11001100);
    }

    let mut reader = BitReader::new(writer.consume());

    for i in 0..8 {
      let val = reader.get_value::<u8>(1)
        .expect("get_value() should return OK");
      assert_eq!(val, i % 2);
    }

    for i in 0..8 {
      let val = reader.get_value::<bool>(1)
        .expect("get_value() should return OK");
      match i {
        0 | 1 | 4 | 5 => assert_eq!(val, false),
        _ => assert_eq!(val, true)
      }
    }
  }

  #[test]
  fn test_put_value_roundtrip() {
    test_put_value_rand_numbers(32, 2);
    test_put_value_rand_numbers(32, 3);
    test_put_value_rand_numbers(32, 4);
    test_put_value_rand_numbers(32, 5);
    test_put_value_rand_numbers(32, 6);
    test_put_value_rand_numbers(32, 7);
    test_put_value_rand_numbers(32, 8);
    test_put_value_rand_numbers(64, 16);
    test_put_value_rand_numbers(64, 24);
    test_put_value_rand_numbers(64, 32);
  }

  fn test_put_value_rand_numbers(total: usize, num_bits: usize) {
    assert!(num_bits < 64);
    let num_bytes = ceil(num_bits as i64, 8);
    let v = vec![0; num_bytes as usize * total];
    let mut writer = BitWriter::from(v);
    let values: Vec<u64> = random_numbers::<u64>(total)
      .iter().map(|v| v & ((1 << num_bits) - 1)).collect();
    for i in 0..total {
      assert!(writer.put_value(values[i] as u64, num_bits),
              "[{}]: put_value() failed", i);
    }

    let mut reader = BitReader::new(writer.consume());
    for i in 0..total {
      let v = reader.get_value::<u64>(num_bits).expect("get_value() should return OK");
      assert_eq!(v, values[i], "[{}]: expected {} but got {}", i, values[i], v);
    }
  }

  #[test]
  fn test_put_aligned_roundtrip() {
    test_put_aligned_rand_numbers::<u8>(4, 3);
    test_put_aligned_rand_numbers::<u8>(16, 5);
    test_put_aligned_rand_numbers::<i16>(32, 7);
    test_put_aligned_rand_numbers::<i16>(32, 9);
    test_put_aligned_rand_numbers::<i32>(32, 11);
    test_put_aligned_rand_numbers::<i32>(32, 13);
    test_put_aligned_rand_numbers::<i64>(32, 17);
    test_put_aligned_rand_numbers::<i64>(32, 23);
  }

  fn test_put_aligned_rand_numbers<T>(total: usize, num_bits: usize)
      where T: Copy + Rand + Default + Debug + PartialEq {
    assert!(num_bits <= 32);
    assert!(total % 2 == 0);

    let aligned_value_byte_width = ::std::mem::size_of::<T>();
    let value_byte_width = ceil(num_bits as i64, 8) as usize;
    let v = vec![0; (total / 2) * (aligned_value_byte_width + value_byte_width)];
    let mut writer = BitWriter::from(v);
    let values: Vec<u32> = random_numbers::<u32>(total / 2)
      .iter().map(|v| v & ((1 << num_bits) - 1)).collect();
    let aligned_values = random_numbers::<T>(total / 2);

    for i in 0..total {
      let j = i / 2;
      if i % 2 == 0 {
        assert!(
          writer.put_value(values[j] as u64, num_bits),
          "[{}]: put_value() failed", i
        );
      } else {
        assert!(writer.put_aligned::<T>(aligned_values[j], aligned_value_byte_width),
                "[{}]: put_aligned() failed", i);
      }
    }

    let mut reader = BitReader::new(writer.consume());
    for i in 0..total {
      let j = i / 2;
      if i % 2 == 0 {
        let v = reader.get_value::<u64>(num_bits).expect("get_value() should return OK");
        assert_eq!(v, values[j] as u64, "[{}]: expected {} but got {}", i, values[j], v);
      } else {
        let v = reader.get_aligned::<T>(aligned_value_byte_width)
          .expect("get_aligned() should return OK");
        assert_eq!(
          v, aligned_values[j],
          "[{}]: expected {:?} but got {:?}", i, aligned_values[j], v);
      }
    }
  }

  #[test]
  fn test_put_vlq_int() {
    let total = 64;
    let v = vec![0; total * 32];
    let mut writer = BitWriter::from(v);
    let values = random_numbers::<u32>(total);
    for i in 0..total {
      assert!(writer.put_vlq_int(values[i] as u64), "[{}]; put_vlq_int() failed", i);
    }

    let mut reader = BitReader::new(writer.consume());
    for i in 0..total {
      let v = reader.get_vlq_int().expect("get_vlq_int() should return OK");
      assert_eq!(v as u32, values[i], "[{}]: expected {} but got {}", i, values[i], v);
    }
  }

  #[test]
  fn test_put_zigzag_vlq_int() {
    let total = 64;
    let v = vec![0; total * 32];
    let mut writer = BitWriter::from(v);
    let values = random_numbers::<i32>(total);
    for i in 0..total {
      assert!(
        writer.put_zigzag_vlq_int(values[i] as i64),
        "[{}]; put_zigzag_vlq_int() failed", i);
    }

    let mut reader = BitReader::new(writer.consume());
    for i in 0..total {
      let v = reader.get_zigzag_vlq_int().expect("get_zigzag_vlq_int() should return OK");
      assert_eq!(v as i32, values[i], "[{}]: expected {} but got {}", i, values[i], v);
    }
  }
}
