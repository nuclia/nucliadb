// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};

use crate::config::{VectorConfig, VectorType};

use super::usize_utils::*;

// A data store schema.
// The data is arrange in a way such that the following operations can be performed:
// -> Given an id, find its value. O(1).
// -> Get the total number of values, or valid ids.
// N = Address size in bytes.
// Serialization schema:
//
// [ number of values: N bytes in little endian.            ] --> Header
// [ sequence of pointers to slots, stored in little endian)]
// [ slots: variable-sized data                             ]
//
// Ids are a normalized representation of pointers.

const N: usize = USIZE_LEN;
const HEADER_LEN: usize = N;
const POINTER_LEN: usize = N;
pub type Pointer = usize;

// Given an index i, the start of its element on idx is: [i*IDXE_LEN + HEADER_LEN]
pub fn get_pointer(x: &[u8], i: usize) -> Pointer {
    let start = (i * POINTER_LEN) + HEADER_LEN;
    let end = start + POINTER_LEN;
    let mut buff = [0; POINTER_LEN];
    buff.copy_from_slice(&x[start..end]);
    usize::from_le_bytes(buff)
}

pub trait Interpreter {
    // Is a mistake to assume that x does not have trailing bytes at the end.
    // Is safe to assume that there is an i such that x[0],.., x[i] is a valid
    // Slot.
    fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8];
    // The function should split x at i, being i the index where
    // x[0],..,x[i] is a valid representation of a slot value.
    // i is ensured to exists.
    fn read_exact<'a>(&self, x: &'a [u8]) -> (/* head */ &'a [u8], /* tail */ &'a [u8]);
    // Is a mistake to assume that x does not have trailing bytes at the end.
    // Is safe to assume that there is an i such that x[0],.., x[i] is a valid
    // Slot.
    fn keep_in_merge(&self, _: &[u8]) -> bool {
        true
    }
}

pub trait IntoBuffer {
    fn serialize_into<W: io::Write>(self, w: W, vector_type: &VectorType) -> io::Result<()>;
}

#[cfg(test)]
impl<T: AsRef<[u8]>> IntoBuffer for T {
    fn serialize_into<W: io::Write>(self, mut w: W, _: &VectorType) -> io::Result<()> {
        w.write_all(self.as_ref())
    }
}

// O(1)
// Returns how many elements are in x, alive or deleted.
pub fn stored_elements(x: &[u8]) -> usize {
    usize_from_slice_le(&x[..HEADER_LEN])
}

// O(1)
pub fn get_value<I: Interpreter>(interpreter: I, src: &[u8], id: usize) -> &[u8] {
    let pointer = get_pointer(src, id);
    interpreter.read_exact(&src[pointer..]).0
}

use lazy_static::lazy_static;
use libc;

#[cfg(not(target_os = "windows"))]
pub fn will_need(src: &[u8], id: usize, vector_len: usize) {
    lazy_static! {
        static ref PAGE_SIZE: usize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    };

    // Calculate node struct size without reading anything, so make some estimates
    // Will only load up to the vectors section, it ignores the key/labels because
    // they are harder to estimate and less useful (only when filtering, similarity is always used)
    let metadata_len = 16; // Estimate
    let node_size = HEADER_LEN + metadata_len + vector_len;

    // Align node pointer to the start page, as required by madvise
    let start = src.as_ptr().wrapping_add(get_pointer(src, id));
    let offset = start.align_offset(*PAGE_SIZE);
    let (start_page, advise_size) = if offset > 0 {
        (start.wrapping_add(offset).wrapping_sub(*PAGE_SIZE), node_size + *PAGE_SIZE - offset)
    } else {
        (start, node_size)
    };

    unsafe { libc::madvise(start_page as *mut libc::c_void, advise_size, libc::MADV_WILLNEED) };
}

#[cfg(target_os = "windows")]
pub fn will_need(src: &[u8], id: usize, vector_len: usize) {}

pub fn create_key_value<D: IntoBuffer>(
    recipient: &mut File,
    slots: Vec<D>,
    vector_type: &VectorType,
) -> io::Result<()> {
    let fixed_size = (HEADER_LEN + (POINTER_LEN * slots.len())) as u64;
    recipient.set_len(fixed_size)?;

    let number_of_values: [u8; POINTER_LEN] = slots.len().to_le_bytes();
    let mut recipient_buffer = BufWriter::new(recipient);

    // Writing the storage header
    recipient_buffer.seek(SeekFrom::Start(0))?;
    recipient_buffer.write_all(&number_of_values)?;

    // Serializing values into the recipient. Each slot is serialized at the end of the
    // loop and its address pointer is written in the pointer section.
    let mut pointer_section_cursor = HEADER_LEN as u64;
    let alignment = vector_type.vector_alignment();
    for slot in slots {
        // slot serialization
        let mut slot_address = recipient_buffer.seek(SeekFrom::End(0))?;
        if slot_address as usize % alignment > 0 {
            let pad = alignment - (slot_address as usize % alignment);
            recipient_buffer.seek(SeekFrom::Current(pad as i64))?;
            slot_address += pad as u64;
        }
        slot.serialize_into(&mut recipient_buffer, vector_type)?;

        // The slot address needs to be written in the pointer section
        recipient_buffer.seek(SeekFrom::Start(pointer_section_cursor))?;
        let slot_address_as_bytes = slot_address.to_le_bytes();
        recipient_buffer.write_all(&slot_address_as_bytes)?;

        // The cursor need to be moved forward
        pointer_section_cursor += POINTER_LEN as u64;
    }

    recipient_buffer.flush()
}

// Merge algorithm. Returns the number of elements merged into the file.
pub fn merge<S: Interpreter + Copy>(
    recipient: &mut File,
    producers: &[(S, &[u8])],
    config: &VectorConfig,
) -> io::Result<bool> {
    // Number of elements, deleted or alive.
    let mut prologue_section_size = HEADER_LEN;
    // To know the range of valid ids per producer
    let mut lengths = Vec::with_capacity(producers.len());

    // Computing lengths and total sizes
    for (_, store) in producers.iter().copied() {
        let producer_elements = stored_elements(store);
        lengths.push(producer_elements);
        prologue_section_size += POINTER_LEN * producer_elements;
    }

    // Reserving space for the prologue, formed by the header
    // and the id section.
    recipient.set_len(prologue_section_size as u64)?;

    // Lengths of the producers have been computed and
    // should not be modified anymore.
    let lengths = lengths;
    // Producer being traversed
    let mut producer_cursor = 0;
    // Next element to be visited in the current producer.
    let mut element_cursor = 0;
    // Number of elements that are actually written in the recipient.
    let mut written_elements: usize = 0;
    // Using a buffered writer for efficient transferring.
    let mut recipient_buffer = BufWriter::new(recipient);
    // Pointer to the next unused id slot
    let mut id_section_cursor = HEADER_LEN;
    let mut has_deletions = false;

    let alignment = config.vector_type.vector_alignment();
    while producer_cursor < producers.len() {
        // If the end of the current producer was reached we move
        // to the start of the next producer.
        if element_cursor == lengths[producer_cursor] {
            element_cursor = 0;
            producer_cursor += 1;
            continue;
        }

        // We check if the current element is deleted and, in that case, it
        // is transferred to the recipient.
        let (interpreter, store) = producers[producer_cursor];
        let element_pointer = get_pointer(store, element_cursor);
        let element_slice = &store[element_pointer..];
        if interpreter.keep_in_merge(element_slice) {
            // Moving to the end of the file to write the current element.
            let (exact_element, _) = interpreter.read_exact(element_slice);
            let mut element_pointer = recipient_buffer.seek(SeekFrom::End(0))?;
            if element_pointer as usize % alignment > 0 {
                let pad = alignment - (element_pointer as usize % alignment);
                recipient_buffer.seek(SeekFrom::Current(pad as i64))?;
                element_pointer += pad as u64;
            }
            recipient_buffer.write_all(exact_element)?;
            // Moving to the next free slot in the section id to write
            // the offset where the element was written.
            recipient_buffer.seek(SeekFrom::Start(id_section_cursor as u64))?;
            recipient_buffer.write_all(&element_pointer.to_le_bytes())?;
            id_section_cursor += POINTER_LEN;
            written_elements += 1;
        } else {
            has_deletions = true;
        }

        // Moving to the next element of this producer.
        element_cursor += 1;
    }

    // Write the number of elements
    recipient_buffer.seek(SeekFrom::Start(0))?;
    recipient_buffer.write_all(&written_elements.to_le_bytes())?;
    recipient_buffer.seek(SeekFrom::Start(0))?;
    recipient_buffer.flush()?;
    Ok(has_deletions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DeleteLog;

    const ZERO: [u8; 4] = [0, 0, 0, 0];
    const ONE: [u8; 4] = [0, 0, 0, 1];
    const THREE: [u8; 4] = [0, 0, 0, 3];
    const TWO: [u8; 4] = [0, 0, 0, 2];
    const SIX: [u8; 4] = [0, 0, 0, 6];
    const TEN: [u8; 4] = [0, 0, 0, 10];

    #[derive(Clone, Copy)]
    struct GreaterThan([u8; 4]);
    impl DeleteLog for GreaterThan {
        fn is_deleted(&self, x: &[u8]) -> bool {
            x.cmp(&self.0).is_le()
        }
    }

    // u32 numbers in big-endian (so cmp is faster)
    #[derive(Clone, Copy)]
    struct TElem;
    impl Interpreter for TElem {
        fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8] {
            &x[0..4]
        }
        fn read_exact<'a>(&self, x: &'a [u8]) -> (&'a [u8], &'a [u8]) {
            x.split_at(4)
        }
    }

    #[test]
    fn store_test() {
        let interpreter = TElem;
        let elems: [u32; 5] = [0, 1, 2, 3, 4];
        let expected: Vec<_> = elems.iter().map(|x| x.to_be_bytes()).collect();
        let mut buf = tempfile::tempfile().unwrap();
        create_key_value(&mut buf, expected.clone(), &VectorType::DenseF32Unaligned).unwrap();

        let buf_map = unsafe { memmap2::Mmap::map(&buf).unwrap() };
        let no_values = stored_elements(&buf_map);
        assert_eq!(no_values, expected.len());
        for (id, expected_value) in expected.iter().enumerate() {
            let actual_value = get_value(interpreter, &buf_map, id);
            let (head, tail) = interpreter.read_exact(actual_value);
            assert_eq!(actual_value, head);
            assert_eq!(tail, &[] as &[u8]);
            assert_eq!(actual_value, expected_value);
        }
    }

    #[test]
    fn merge_test() {
        let v0: Vec<_> = [0u32, 2, 4].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [1u32, 5, 7].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [8u32, 9, 10, 11].iter().map(|x| x.to_be_bytes()).collect();
        let expected = [0u32, 1, 2, 4, 5, 7, 8, 9, 10, 11];

        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let elems = vec![(TElem, v0_map.as_ref()), (TElem, v1_map.as_ref()), (TElem, v2_map.as_ref())];

        merge(&mut merge_store, &elems, &VectorConfig::default()).unwrap();
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_map, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_some_deleted_different_length() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5, 11].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8, 9, 10].iter().map(|x| x.to_be_bytes()).collect();

        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let interpreter = (GreaterThan(ONE), TElem);
        let mut merge_store = tempfile::tempfile().unwrap();
        let elems = vec![
            // zero and 1 will be removed
            (interpreter, v0_map.as_ref()),
            // no element is removed
            (interpreter, v1_map.as_ref()),
            // no element is removed
            (interpreter, v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut merge_store, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_map, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_some_elements_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let interpreter = (GreaterThan(ONE), TElem);
        let mut merge_store = tempfile::tempfile().unwrap();
        let elems = vec![
            // zero and 1 will be removed
            (interpreter, v0_map.as_ref()),
            // no element is removed
            (interpreter, v1_map.as_ref()),
            // no element is removed
            (interpreter, v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut merge_store, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_map, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_first_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let elems = vec![
            // The first element is deleted
            ((GreaterThan(ZERO), TElem), v0_map.as_ref()),
            // The first element is deleted
            ((GreaterThan(THREE), TElem), v1_map.as_ref()),
            // The first element is deleted
            ((GreaterThan(SIX), TElem), v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut merge_store, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![1, 2, 4, 5, 7, 8];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_map, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_one_store_empty() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let interpreter = (GreaterThan(TWO), TElem);
        let mut merge_storage = tempfile::tempfile().unwrap();
        let elems = vec![
            // all the elements are deleted
            (interpreter, v0_map.as_ref()),
            // no element is removed
            (interpreter, v1_map.as_ref()),
            // no element is removed
            (interpreter, v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut merge_storage, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![3, 4, 5, 6, 7, 8];
        let merge_store = unsafe { memmap2::Mmap::map(&merge_storage).unwrap() };
        let number_of_elements = stored_elements(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_with_different_interpreters() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let greater_than_2 = (GreaterThan(TWO), TElem);
        let greater_than_10 = (GreaterThan(TEN), TElem);
        let mut merge_storage = tempfile::tempfile().unwrap();
        let elems = vec![
            // all the elements are removed
            (greater_than_2, v0_map.as_ref()),
            // no element are removed
            (greater_than_2, v1_map.as_ref()),
            // all the elements are removed
            (greater_than_10, v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut merge_storage, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![3, 4, 5];
        let merge_store = unsafe { memmap2::Mmap::map(&merge_storage).unwrap() };
        let number_of_elements = stored_elements(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_all_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v1_store, v1, &VectorType::DenseF32Unaligned).unwrap();
        create_key_value(&mut v2_store, v2, &VectorType::DenseF32Unaligned).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let interpreter = (GreaterThan(TEN), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // all the elements are removed
            (interpreter, v0_map.as_ref()),
            // all the elements are removed
            (interpreter, v1_map.as_ref()),
            // all the elements are removed
            (interpreter, v2_map.as_ref()),
        ];

        merge::<(GreaterThan, TElem)>(&mut file, elems.as_slice(), &VectorConfig::default()).unwrap();
        let expected: Vec<u32> = vec![];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = stored_elements(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }
}
