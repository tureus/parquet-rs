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

use std::cmp;
use std::io::Write;
use std::marker::PhantomData;
use std::mem;
use std::slice;

use basic::*;
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::ColumnDescPtr;
use util::memory::{Buffer, ResizableBuffer, BufferRange, Arena, ArenaRef};
use util::bit_util::{self, BitWriter, log2, num_required_bits};
use util::hash_util;
use encodings::rle_encoding::RleEncoder;

/// An Parquet encoder for the data type `T`.
///
/// Currently this allocates internal buffers for the encoded values. After done putting
/// values, caller should call `flush_buffer()` to get an immutable buffer pointer.
pub trait Encoder<T: DataType> {
  /// Encodes data from `values`.
  fn put(&mut self, values: &[T::T]) -> Result<()>;

  /// Returns the encoding type of this encoder.
  fn encoding(&self) -> Encoding;

  /// Flushes the underlying byte buffer that's being processed by this encoder, and
  /// return the immutable copy of it. This will also reset the internal state.
  fn flush_buffer(&mut self) -> Result<Buffer>;
}

/// Gets a encoder for the particular data type `T` and encoding `encoding`. Memory usage
/// for the encoder instance is tracked by `mem_tracker`.
pub fn get_encoder<T: DataType>(
  arena: ArenaRef,
  desc: ColumnDescPtr,
  encoding: Encoding
) -> Result<Box<Encoder<T>>> where T: 'static {
  let encoder = match encoding {
    Encoding::PLAIN => {
      Box::new(PlainEncoder::new(arena, desc)) as Box<Encoder<T>>
    },
    Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
      Box::new(DictEncoder::new(arena, desc))
    },
    Encoding::DELTA_BINARY_PACKED => {
      Box::new(DeltaBitPackEncoder::new(arena))
    },
    Encoding::DELTA_LENGTH_BYTE_ARRAY => {
      Box::new(DeltaLengthByteArrayEncoder::new(arena))
    },
    Encoding::DELTA_BYTE_ARRAY => {
      Box::new(DeltaByteArrayEncoder::new(arena))
    },
    e => return Err(nyi_err!("Encoding {} is not supported.", e))
  };
  Ok(encoder)
}


// ----------------------------------------------------------------------
// Plain encoding

pub struct PlainEncoder<T: DataType> {
  arena: ArenaRef,
  buffer: ResizableBuffer,
  bit_writer: BitWriter,
  desc: ColumnDescPtr,
  _phantom: PhantomData<T>
}

const PLAIN_BUF_INIT_CAP: usize = 1024;

impl<T: DataType> PlainEncoder<T> {
  pub fn new(arena: ArenaRef, desc: ColumnDescPtr) -> Self {
    let tracked_buf = Arena::alloc_resizable(&arena, PLAIN_BUF_INIT_CAP);
    let bit_buf = arena.alloc(256);
    Self {
      arena: arena,
      buffer: tracked_buf,
      bit_writer: BitWriter::new(bit_buf),
      desc: desc,
      _phantom: PhantomData
    }
  }
}

impl<T: DataType> Encoder<T> for PlainEncoder<T> {
  default fn put(&mut self, values: &[T::T]) -> Result<()> {
    let bytes = unsafe {
      slice::from_raw_parts(
        values as *const [T::T] as *const u8,
        mem::size_of::<T::T>() * values.len())
    };
    self.buffer.write(bytes)?;
    Ok(())
  }

  fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  #[inline]
  default fn flush_buffer(&mut self) -> Result<Buffer> {
    self.bit_writer.flush();
    {
      let bit_buffer = self.bit_writer.buffer();
      self.buffer.write(&bit_buffer[0..self.bit_writer.byte_offset()])?;
      self.buffer.flush()?;
    }
    self.bit_writer.clear();
    let new_buf = Arena::alloc_resizable(&self.arena, PLAIN_BUF_INIT_CAP);
    let buf = mem::replace(&mut self.buffer, new_buf);
    Ok(buf.to_fixed())
  }
}

impl Encoder<BoolType> for PlainEncoder<BoolType> {
  fn put(&mut self, values: &[bool]) -> Result<()> {
    for v in values {
      self.bit_writer.put_value(*v as u64, 1);
    }
    Ok(())
  }
}

impl Encoder<Int96Type> for PlainEncoder<Int96Type> {
  fn put(&mut self, values: &[Int96]) -> Result<()> {
    for v in values {
      self.buffer.write(v.as_bytes())?;
    }
    self.buffer.flush()?;
    Ok(())
  }
}

impl Encoder<ByteArrayType> for PlainEncoder<ByteArrayType> {
  fn put(&mut self, values: &[ByteArray]) -> Result<()> {
    for v in values {
      self.buffer.write(&(v.len().to_le() as u32).as_bytes())?;
      self.buffer.write(v.data())?;
    }
    self.buffer.flush()?;
    Ok(())
  }
}

impl Encoder<FixedLenByteArrayType> for PlainEncoder<FixedLenByteArrayType> {
  fn put(&mut self, values: &[ByteArray]) -> Result<()> {
    for v in values {
      self.buffer.write(v.data())?;
    }
    self.buffer.flush()?;
    Ok(())
  }
}


// ----------------------------------------------------------------------
// Dictionary encoding

const INITIAL_HASH_TABLE_SIZE: usize = 1024;
const MAX_HASH_LOAD: f32 = 0.7;
const HASH_SLOT_EMPTY: i32 = -1;

pub struct DictEncoder<T: DataType> {
  // Arena used to allocate memory for this encoder.
  arena: ArenaRef,

  // Descriptor for the column to be encoded.
  desc: ColumnDescPtr,

  // Size of the table. **Must be** a power of 2.
  hash_table_size: usize,

  // Store `hash_table_size` - 1, so that `j & mod_bitmask` is equivalent to
  // `j % hash_table_size`, but uses far fewer CPU cycles.
  mod_bitmask: u32,

  // Stores indices which map (many-to-one) to the values in the `uniques` array.
  // Here we are using fix-sized array with linear probing.
  // A slot with `HASH_SLOT_EMPTY` indicates the slot is not currently occupied.
  hash_slots: Vec<i32>,

  // Indices that have not yet be written out by `write_indices()`.
  buffered_indices: Vec<i32>,

  // The unique observed values.
  uniques: Vec<T::T>,

  // The number of bytes needed to encode this dictionary
  dict_encoded_size: u64,
}

/**
impl<T: DataType> DictEncoder<T> {
  pub fn new(desc: ColumnDescPtr, mem_tracker: MemTrackerPtr) -> Self {
    let mut slots = Buffer::new().with_mem_tracker(mem_tracker.clone());
    slots.resize(INITIAL_HASH_TABLE_SIZE, -1);
**/

impl<T: DataType> DictEncoder<T> {
  pub fn new(arena: ArenaRef, desc: ColumnDescPtr) -> Self {
    let mut slots = Vec::new();
    slots.resize(INITIAL_HASH_TABLE_SIZE, HASH_SLOT_EMPTY);
    Self {
      arena: arena,
      desc: desc,
      hash_table_size: INITIAL_HASH_TABLE_SIZE,
      mod_bitmask: (INITIAL_HASH_TABLE_SIZE - 1) as u32,
      hash_slots: slots,
      buffered_indices: Vec::new(),
      uniques: Vec::new(),
      dict_encoded_size: 0
    }
  }

  pub fn num_entries(&self) -> usize {
    self.uniques.len()
  }

  pub fn dict_encoded_size(&self) -> u64 {
    self.dict_encoded_size
  }

  /// Writes out the dictionary values with PLAIN encoding in a byte buffer, and return
  /// the result.
  #[inline]
  pub fn write_dict(&self, _: Buffer) -> Result<Buffer> {
    let mut plain_encoder = PlainEncoder::<T>::new(
      self.arena.clone(), self.desc.clone());
    plain_encoder.put(&self.uniques[..])?;
    plain_encoder.flush_buffer()
  }

  /// Writes out the dictionary values with RLE encoding in a byte buffer, and return the
  /// result.
  #[inline]
  pub fn write_indices(&mut self) -> Result<Buffer> {
    let bit_width = self.bit_width();

    // TODO: the caller should allocate the buffer
    // let buffer_len = 1 + RleEncoder::min_buffer_size(bit_width)
    //   + RleEncoder::max_buffer_size(bit_width, self.buffered_indices.size());
    // let mut buffer: Vec<u8> = vec![0; buffer_len as usize];
    // buffer[0] = bit_width as u8;
    // self.mem_tracker.alloc(buffer.capacity() as i64);

    // // Write bit width in the first byte
    // buffer.write((self.bit_width() as u8).as_bytes())?;
    // let mut encoder = RleEncoder::new_from_buf(self.bit_width(), buffer, 1);
    // for index in self.buffered_indices.data() {

    let buffer_len = 1 + RleEncoder::min_buffer_size(bit_width);
    let buffer = self.arena.alloc_aligned(buffer_len as usize);
    buffer.data_mut()[0] = bit_width as u8;

    let mut encoder = RleEncoder::new(self.bit_width(), buffer.range(1..));
    for index in &self.buffered_indices {
      if !encoder.put(*index as u64)? {
        return Err(general_err!("Encoder doesn't have enough space"));
      }
    }
    self.buffered_indices.clear();
    encoder.flush()?;
    let result = buffer.range(..encoder.len()+1);
    Ok(result)
  }

  #[inline]
  fn put_one(&mut self, value: &T::T) -> Result<()> {
    let mut j = (hash_util::hash(value, 0) & self.mod_bitmask) as usize;
    let mut index = self.hash_slots[j];

    while index != HASH_SLOT_EMPTY && self.uniques[index as usize] != *value {
      j += 1;
      if j == self.hash_table_size {
        j = 0;
      }
      index = self.hash_slots[j];
    }

    if index == HASH_SLOT_EMPTY {
      index = self.uniques.len() as i32;
      self.hash_slots[j] = index;
      self.add_dict_key(value.clone());

      if self.uniques.len() > (self.hash_table_size as f32 * MAX_HASH_LOAD) as usize {
        self.double_table_size();
      }
    }

    self.buffered_indices.push(index);
    Ok(())
  }

  #[inline]
  fn add_dict_key(&mut self, value: T::T) {
    self.uniques.push(value);
    self.dict_encoded_size += mem::size_of::<T::T>() as u64;
  }

  #[inline]
  fn bit_width(&self) -> u8 {
    let num_entries = self.uniques.len();
    if num_entries == 0 { 0 }
    else if num_entries == 1 { 1 }
    else { log2(num_entries as u64) as u8 }
  }

  #[inline]
  fn double_table_size(&mut self) {
    let new_size = self.hash_table_size * 2;
    let mut new_hash_slots = Vec::new();
    new_hash_slots.resize(new_size, HASH_SLOT_EMPTY);
    for i in 0..self.hash_table_size {
      let index = self.hash_slots[i];
      if index == HASH_SLOT_EMPTY {
        continue;
      }
      let value = &self.uniques[index as usize];
      let mut j = (hash_util::hash(value, 0) & ((new_size - 1) as u32)) as usize;
      let mut slot = new_hash_slots[j];
      while slot != HASH_SLOT_EMPTY && self.uniques[slot as usize] != *value {
        j += 1;
        if j == new_size {
          j = 0;
        }
        slot = new_hash_slots[j];
      }

      new_hash_slots[j] = index;
    }

    self.hash_table_size = new_size;
    self.mod_bitmask = (new_size - 1) as u32;
    mem::replace(&mut self.hash_slots, new_hash_slots);
  }
}

impl<T: DataType> Encoder<T> for DictEncoder<T> {
  #[inline]
  fn put(&mut self, values: &[T::T]) -> Result<()> {
    for i in values {
      self.put_one(&i)?
    }
    Ok(())
  }

  #[inline]
  fn encoding(&self) -> Encoding {
    Encoding::PLAIN_DICTIONARY
  }

  #[inline]
  fn flush_buffer(&mut self) -> Result<Buffer> {
    self.write_indices()
  }
}

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encoding

const MAX_PAGE_HEADER_WRITER_SIZE: usize = 32;
const MAX_BIT_WRITER_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_BLOCK_SIZE: usize = 32;
const DEFAULT_NUM_MINI_BLOCKS: usize = 4;

/// Delta-binary-packing:
///   [page-header] [block 1], [block 2], ... [block N]
/// Each page header consists of:
///   [block size] [number of miniblocks in a block] [total value count] [first value]
/// Each block consists of:
///   [min delta] [list of bitwidths of miniblocks] [miniblocks]
///

/// Current implementation writes values in `put` method, multiple calls to `put` add to
/// existing block or start new block if block size is exceeded. Calling `flush_buffer`
/// writes out all data and resets internal state, including page header.
///
/// Supports only Int32Type and Int64Type.
///
pub struct DeltaBitPackEncoder<T: DataType> {
  page_header_writer: BitWriter,
  bit_writer: BitWriter,
  total_values: usize,
  first_value: i64,
  current_value: i64,
  block_size: usize,
  mini_block_size: usize,
  num_mini_blocks: usize,
  values_in_block: usize,
  deltas: Vec<i64>,
  arena: ArenaRef,
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaBitPackEncoder<T> {
  pub fn new(arena: ArenaRef) -> Self {
    let block_size = DEFAULT_BLOCK_SIZE;
    let num_mini_blocks = DEFAULT_NUM_MINI_BLOCKS;
    let mini_block_size = block_size / num_mini_blocks;
    assert!(mini_block_size % 8 == 0);
    Self::assert_supported_type();

    let page_header_buf = arena.alloc_aligned(MAX_PAGE_HEADER_WRITER_SIZE);
    let bit_buf = arena.alloc_aligned(MAX_BIT_WRITER_SIZE);
    DeltaBitPackEncoder {
      page_header_writer: BitWriter::new(page_header_buf),
      bit_writer: BitWriter::new(bit_buf),
      total_values: 0,
      first_value: 0,
      current_value: 0, // current value to keep adding deltas
      block_size: block_size, // can write fewer values than block size for last block
      mini_block_size: mini_block_size,
      num_mini_blocks: num_mini_blocks,
      values_in_block: 0, // will be at most block_size
      deltas: vec![0; block_size],
      arena: arena,
      _phantom: PhantomData
    }
  }

  // Writes page header for blocks, this method is invoked when we are done encoding
  // values. It is also okay to encode when no values have been provided
  fn write_page_header(&mut self) {
    // We ignore the result of each 'put' operation, because MAX_PAGE_HEADER_WRITER_SIZE
    // is chosen to fit all header values and guarantees that writes will not fail.

    // Write the size of each block
    self.page_header_writer.put_vlq_int(self.block_size as u64);
    // Write the number of mini blocks
    self.page_header_writer.put_vlq_int(self.num_mini_blocks as u64);
    // Write the number of all values (including non-encoded first value)
    self.page_header_writer.put_vlq_int(self.total_values as u64);
    // Write first value
    self.page_header_writer.put_zigzag_vlq_int(self.first_value);
  }

  // Write current delta buffer (<= 'block size' values) into bit writer
  fn flush_block_values(&mut self) -> Result<()> {
    if self.values_in_block == 0 {
      return Ok(())
    }

    let mut min_delta = i64::max_value();
    for i in 0..self.values_in_block {
      min_delta = cmp::min(min_delta, self.deltas[i]);
    }

    // Write min delta
    if !self.bit_writer.put_zigzag_vlq_int(min_delta) {
      return Err(general_err!("Error when putting zigzag VLQ int"));
    }

    // Slice to store bit width for each mini block
    // apply unsafe allocation to avoid double mutable borrow
    let mini_block_widths: &mut [u8] = unsafe {
      let tmp_slice = self.bit_writer.get_next_byte_ptr(self.num_mini_blocks)?;
      slice::from_raw_parts_mut(tmp_slice.as_ptr() as *mut u8, self.num_mini_blocks)
    };

    for i in 0..self.num_mini_blocks {
      // Find how many values we need to encode - either block size or whatever values
      // left
      let n = cmp::min(self.mini_block_size, self.values_in_block);
      if n == 0 {
        break;
      }

      // Compute the max delta in current mini block
      let mut max_delta = i64::min_value();
      for j in 0..n {
        max_delta = cmp::max(max_delta, self.deltas[i * self.mini_block_size + j]);
      }

      // Compute bit width to store (max_delta - min_delta)
      let bit_width = num_required_bits(self.subtract_u64(max_delta, min_delta));
      mini_block_widths[i] = bit_width as u8;

      // Encode values in current mini block using min_delta and bit_width
      for j in 0..n {
        let packed_value = self.subtract_u64(
          self.deltas[i * self.mini_block_size + j], min_delta);
        self.bit_writer.put_value(packed_value, bit_width);
      }

      // Pad the last block (n < mini_block_size)
      for _ in n..self.mini_block_size {
        self.bit_writer.put_value(0, bit_width);
      }

      self.values_in_block -= n;
    }

    assert!(
      self.values_in_block == 0,
      "Expected 0 values in block, found {}", self.values_in_block);
    Ok(())
  }
}

// Implementation is shared between Int32Type and Int64Type,
// see `DeltaBitPackEncoderConversion` below for specifics.
impl<T: DataType> Encoder<T> for DeltaBitPackEncoder<T> {
  fn put(&mut self, values: &[T::T]) -> Result<()> {
    if values.is_empty() {
      return Ok(())
    }

    let mut idx;
    // Define values to encode, initialize state
    if self.total_values == 0 {
      self.first_value = self.as_i64(values, 0);
      self.current_value = self.first_value;
      idx = 1;
    } else {
      idx = 0;
    }
    // Add all values (including first value)
    self.total_values += values.len();

    // Write block
    while idx < values.len() {
      let value = self.as_i64(values, idx);
      self.deltas[self.values_in_block] = self.subtract(value, self.current_value);
      self.current_value = value;
      idx += 1;
      self.values_in_block += 1;
      if self.values_in_block == self.block_size {
        self.flush_block_values()?;
      }
    }
    Ok(())
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BINARY_PACKED
  }

  fn flush_buffer(&mut self) -> Result<Buffer> {
    // Write remaining values
    self.flush_block_values()?;
    // Write page header with total values
    self.write_page_header();

    self.page_header_writer.flush();
    self.bit_writer.flush();

    let total_len = self.page_header_writer.byte_offset()
      + self.bit_writer.byte_offset();
    let buffer = self.arena.alloc(total_len);
    {
      let bit_buffer = self.page_header_writer.buffer();
      bit_util::memcpy(
        &bit_buffer[..self.page_header_writer.byte_offset()],
        &mut buffer.data_mut()[..self.page_header_writer.byte_offset()]
      );
    }
    {
      let bit_buffer = self.bit_writer.buffer();
      bit_util::memcpy(
        &bit_buffer[0..self.bit_writer.byte_offset()],
        &mut buffer.data_mut()[self.page_header_writer.byte_offset()..]
      );
    }

    // Reset state
    self.page_header_writer.clear();
    self.bit_writer.clear();
    self.total_values = 0;
    self.first_value = 0;
    self.current_value = 0;
    self.values_in_block = 0;

    Ok(buffer)
  }
}

// Helper trait to define specific conversions and subtractions when computing deltas
trait DeltaBitPackEncoderConversion<T: DataType> {
  // Method should panic if type is not supported, otherwise no-op
  #[inline]
  fn assert_supported_type();

  #[inline]
  fn as_i64(&self, values: &[T::T], index: usize) -> i64;

  #[inline]
  fn subtract(&self, left: i64, right: i64) -> i64;

  #[inline]
  fn subtract_u64(&self, left: i64, right: i64) -> u64;
}

impl<T: DataType> DeltaBitPackEncoderConversion<T> for DeltaBitPackEncoder<T> {
  #[inline]
  default fn assert_supported_type() {
    panic!("DeltaBitPackDecoder only supports Int32Type and Int64Type");
  }

  #[inline]
  default fn as_i64(&self, _values: &[T::T], _index: usize) -> i64 { 0 }

  #[inline]
  default fn subtract(&self, _left: i64, _right: i64) -> i64 { 0 }

  #[inline]
  default fn subtract_u64(&self, _left: i64, _right: i64) -> u64 { 0 }
}

impl DeltaBitPackEncoderConversion<Int32Type> for DeltaBitPackEncoder<Int32Type> {
  #[inline]
  fn assert_supported_type() {
    // no-op: supported type
  }

  #[inline]
  fn as_i64(&self, values: &[i32], index: usize) -> i64 {
    values[index] as i64
  }

  #[inline]
  fn subtract(&self, left: i64, right: i64) -> i64 {
    // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
    (left as i32).wrapping_sub(right as i32) as i64
  }

  #[inline]
  fn subtract_u64(&self, left: i64, right: i64) -> u64 {
    // Conversion of i32 -> u32 -> u64 is to avoid non-zero left most bytes in int
    // representation
    (left as i32).wrapping_sub(right as i32) as u32 as u64
  }
}

impl DeltaBitPackEncoderConversion<Int64Type> for DeltaBitPackEncoder<Int64Type> {
  #[inline]
  fn assert_supported_type() {
    // no-op: supported type
  }

  #[inline]
  fn as_i64(&self, values: &[i64], index: usize) -> i64 {
    values[index]
  }

  #[inline]
  fn subtract(&self, left: i64, right: i64) -> i64 {
    // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
    left.wrapping_sub(right)
  }

  #[inline]
  fn subtract_u64(&self, left: i64, right: i64) -> u64 {
    left.wrapping_sub(right) as u64
  }
}

/// Encoding for byte arrays to separate the length values and the data.
/// The lengths are encoded using DELTA_BINARY_PACKED encoding, data is
/// stored as raw bytes.
pub struct DeltaLengthByteArrayEncoder<T: DataType> {
  arena: ArenaRef,
  // length encoder
  len_encoder: DeltaBitPackEncoder<Int32Type>,
  // byte array data
  data: Vec<ByteArray>,
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaLengthByteArrayEncoder<T> {
  pub fn new(arena: ArenaRef) -> Self {
    Self {
      arena: arena.clone(),
      len_encoder: DeltaBitPackEncoder::new(arena),
      data: vec!(),
      _phantom: PhantomData
    }
  }
}

impl<T: DataType> Encoder<T> for DeltaLengthByteArrayEncoder<T> {
  default fn put(&mut self, _values: &[T::T]) -> Result<()> {
    panic!("DeltaLengthByteArrayEncoder only supports ByteArrayType");
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_LENGTH_BYTE_ARRAY
  }

  default fn flush_buffer(&mut self) -> Result<Buffer> {
    panic!("DeltaLengthByteArrayEncoder only supports ByteArrayType");
  }
}

impl Encoder<ByteArrayType> for DeltaLengthByteArrayEncoder<ByteArrayType> {
  fn put(&mut self, values: &[ByteArray]) -> Result<()> {
    let lengths: Vec<i32> =
      values.iter().map(|byte_array| byte_array.len() as i32).collect();
    self.len_encoder.put(&lengths)?;
    for byte_array in values {
      self.data.push(byte_array.clone());
    }
    Ok(())
  }

  fn flush_buffer(&mut self) -> Result<Buffer> {
    // TODO: allocate from arena
    let mut total_bytes = vec!();
    let lengths = self.len_encoder.flush_buffer()?;
    total_bytes.extend_from_slice(lengths.data());
    self.data.iter().for_each(|byte_array| {
      total_bytes.extend_from_slice(byte_array.data());
    });
    self.data.clear();
    Ok(Buffer::from(total_bytes))
  }
}

/// Encoding for byte arrays, prefix lengths are encoded using DELTA_BINARY_PACKED
/// encoding, followed by suffixes with DELTA_LENGTH_BYTE_ARRAY encoding.
pub struct DeltaByteArrayEncoder<T: DataType> {
  arena: ArenaRef,
  prefix_len_encoder: DeltaBitPackEncoder<Int32Type>,
  suffix_writer: DeltaLengthByteArrayEncoder<T>,
  previous: Vec<u8>,
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaByteArrayEncoder<T> {
  pub fn new(arena: ArenaRef) -> Self {
    Self {
      arena: arena.clone(),
      prefix_len_encoder: DeltaBitPackEncoder::<Int32Type>::new(arena.clone()),
      suffix_writer: DeltaLengthByteArrayEncoder::<T>::new(arena),
      previous: vec!(),
      _phantom: PhantomData
    }
  }
}

impl<T: DataType> Encoder<T> for DeltaByteArrayEncoder<T> {
  default fn put(&mut self, _values: &[T::T]) -> Result<()> {
    panic!("DeltaByteArrayEncoder only supports ByteArrayType");
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BYTE_ARRAY
  }

  default fn flush_buffer(&mut self) -> Result<Buffer> {
    panic!("DeltaByteArrayEncoder only supports ByteArrayType");
  }
}

impl Encoder<ByteArrayType> for DeltaByteArrayEncoder<ByteArrayType> {
  fn put(&mut self, values: &[ByteArray]) -> Result<()> {
    let mut prefix_lengths: Vec<i32> = vec!();
    let mut suffixes: Vec<ByteArray> = vec!();

    for byte_array in values {
      let current = byte_array.data();
      // Maximum prefix length that is shared between previous value and current value
      let prefix_len = cmp::min(self.previous.len(), current.len());
      let mut match_len = 0;
      while match_len < prefix_len && self.previous[match_len] == current[match_len] {
        match_len += 1;
      }
      prefix_lengths.push(match_len as i32);
      suffixes.push(byte_array.slice(match_len, byte_array.len() - match_len));
      // Update previous for the next prefix
      self.previous.clear();
      self.previous.extend_from_slice(current);
    }
    self.prefix_len_encoder.put(&prefix_lengths)?;
    self.suffix_writer.put(&suffixes)?;
    Ok(())
  }

  fn flush_buffer(&mut self) -> Result<Buffer> {
    // TODO: investigate if we can merge lengths and suffixes
    // without copying data into new vector.
    // TODO: allocate from arena
    let mut total_bytes = vec!();
    // Insert lengths ...
    let lengths = self.prefix_len_encoder.flush_buffer()?;
    total_bytes.extend_from_slice(lengths.data());
    // ... followed by suffixes
    let suffixes = self.suffix_writer.flush_buffer()?;
    total_bytes.extend_from_slice(suffixes.data());

    Ok(Buffer::from(total_bytes))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use super::super::decoding::*;
  use std::rc::Rc;
  use rand::Rand;

  use schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
  use util::memory::{Arena};
  use util::test_common::RandGen;

  const TEST_SET_SIZE: usize = 1024;

  #[test]
  fn test_bool() {
    // BoolType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    BoolType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i32() {
    // Int32Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    // Int32Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
    Int32Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i64() {
    Int64Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    Int64Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
    Int64Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i96() {
    Int96Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    Int96Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_float() {
    FloatType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    FloatType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_double() {
    DoubleType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    DoubleType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  // #[test]
  // fn test_byte_array() {
  //   ByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
  //   ByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  //   ByteArrayType::test(Encoding::DELTA_LENGTH_BYTE_ARRAY, TEST_SET_SIZE, -1);
  //   ByteArrayType::test(Encoding::DELTA_BYTE_ARRAY, TEST_SET_SIZE, -1);
  // }

  // #[test]
  // fn test_fixed_lenbyte_array() {
  //   FixedLenByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, 100);
  //   FixedLenByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, 100);
  // }

  trait EncodingTester<T: DataType> {
    fn test(enc: Encoding, total: usize, type_length: i32) {
      let result = match enc {
        Encoding::PLAIN_DICTIONARY => Self::test_dict_internal(total, type_length),
        enc @ _ => Self::test_internal(enc, total, type_length)
      };

      assert!(
        result.is_ok(),
        "Expected result to be OK but got err:\n {}", result.unwrap_err());
    }

    fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()>;

    fn test_dict_internal(total: usize, type_length: i32) -> Result<()>;
  }

  impl<T: DataType> EncodingTester<T> for T where T: 'static, T::T: Rand {
    fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()> {
      let mut encoder = create_test_encoder::<T>(type_length, enc);
      let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..])?;

      let mut data = encoder.flush_buffer()?;
      let mut decoder = create_test_decoder::<T>(type_length, enc);
      let mut result_data = vec![T::T::default(); total];
      decoder.set_data(data, total)?;
      let mut actual_total = decoder.get(&mut result_data)?;

      assert_eq!(actual_total, total);
      assert_eq!(result_data, values);

      // Encode more data after flush and test with decoder

      values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..])?;
      data = encoder.flush_buffer()?;

      decoder.set_data(data, total)?;
      actual_total = decoder.get(&mut result_data)?;

      assert_eq!(actual_total, total);
      assert_eq!(result_data, values);

      Ok(())
    }

    fn test_dict_internal(total: usize, type_length: i32) -> Result<()> {
      println!("XXX: start testing dict internal");
      let arena = Rc::new(Arena::new());
      let mut encoder = create_test_dict_encoder::<T>(arena.clone(), type_length);
      let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..])?;
      println!("XXX: finished put");

      let mut data = encoder.flush_buffer()?;
      println!("XXX: created dict decoder");

      let mut decoder = create_test_dict_decoder::<T>();
      let mut dict_decoder = PlainDecoder::<T>::new(type_length);


      let buffer = arena.alloc(encoder.dict_encoded_size() as usize);
      let dict_buffer = encoder.write_dict(buffer)?;
      println!("XXX: finished writing dict");

      dict_decoder.set_data(dict_buffer, encoder.num_entries())?;
      decoder.set_dict(Box::new(dict_decoder))?;
      let mut result_data = vec![T::T::default(); total];
      decoder.set_data(data, total)?;
      let mut actual_total = decoder.get(&mut result_data)?;

      assert_eq!(actual_total, total);
      assert_eq!(result_data, values);

      // Encode more data after flush and test with decoder

      values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..])?;
      data = encoder.flush_buffer()?;

      let mut dict_decoder = PlainDecoder::<T>::new(type_length);
      let buffer = arena.alloc(encoder.dict_encoded_size() as usize);
      dict_decoder.set_data(encoder.write_dict(buffer)?, encoder.num_entries())?;
      decoder.set_dict(Box::new(dict_decoder))?;
      decoder.set_data(data, total)?;
      actual_total = decoder.get(&mut result_data)?;

      assert_eq!(actual_total, total);
      assert_eq!(result_data, values);

      Ok(())
    }
  }

  fn create_test_col_desc(type_len: i32, t: Type) -> ColumnDescriptor {
    let ty = SchemaType::primitive_type_builder("t", t)
      .with_length(type_len)
      .build()
      .unwrap();
    ColumnDescriptor::new(Rc::new(ty), None, 0, 0, ColumnPath::new(vec!()))
  }

  fn create_test_encoder<T: DataType>(
    type_len: i32, enc: Encoding
  ) -> Box<Encoder<T>> where T: 'static {
    let desc = Rc::new(create_test_col_desc(type_len, T::get_physical_type()));
    let arena = Rc::new(Arena::new());
    let encoder = match enc {
      Encoding::PLAIN => {
        Box::new(PlainEncoder::<T>::new(arena, desc))
      },
      Encoding::PLAIN_DICTIONARY => {
        Box::new(DictEncoder::<T>::new(arena, desc)) as Box<Encoder<T>>
      },
      Encoding::DELTA_BINARY_PACKED => {
        Box::new(DeltaBitPackEncoder::<T>::new(arena))
      },
      Encoding::DELTA_LENGTH_BYTE_ARRAY => {
        Box::new(DeltaLengthByteArrayEncoder::<T>::new(arena))
      },
      Encoding::DELTA_BYTE_ARRAY => {
        Box::new(DeltaByteArrayEncoder::<T>::new(arena))
      },
      _ => {
        panic!("Not implemented yet.");
      }
    };
    encoder
  }

  fn create_test_decoder<T: DataType>(
    type_len: i32,
    enc: Encoding
  ) -> Box<Decoder<T>> where T: 'static {
    let desc = create_test_col_desc(type_len, T::get_physical_type());
    let arena = Rc::new(Arena::new());
    let decoder = match enc {
      Encoding::PLAIN => {
        Box::new(PlainDecoder::<T>::new(desc.type_length()))
      },
      Encoding::PLAIN_DICTIONARY => {
        Box::new(DictDecoder::<T>::new()) as Box<Decoder<T>>
      },
      Encoding::DELTA_BINARY_PACKED => {
        Box::new(DeltaBitPackDecoder::<T>::new())
      },
      Encoding::DELTA_LENGTH_BYTE_ARRAY => {
        Box::new(DeltaLengthByteArrayDecoder::<T>::new(arena))
      },
      Encoding::DELTA_BYTE_ARRAY => {
        Box::new(DeltaByteArrayDecoder::<T>::new(arena))
      },
      _ => {
        panic!("Not implemented yet.");
      }
    };
    decoder
  }

  fn create_test_dict_encoder<T: DataType>(
    arena: ArenaRef,
    type_len: i32
  ) -> DictEncoder<T> {
    let desc = create_test_col_desc(type_len, T::get_physical_type());
    DictEncoder::<T>::new(arena, Rc::new(desc))
  }

  fn create_test_dict_decoder<T: DataType>() -> DictDecoder<T> {
    DictDecoder::<T>::new()
  }
}
