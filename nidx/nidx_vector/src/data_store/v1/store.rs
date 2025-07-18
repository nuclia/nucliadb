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

use super::node::Node;
use crate::config::{VectorConfig, VectorType};
use crate::data_store::ParagraphAddr;
use crate::data_types::usize_utils::*;
use lazy_static::lazy_static;
use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};

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
pub fn get_value(src: &[u8], id: usize) -> Node {
    let pointer = get_pointer(src, id);
    Node::new(&src[pointer..])
}

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
        (
            start.wrapping_add(offset).wrapping_sub(*PAGE_SIZE),
            node_size + *PAGE_SIZE - offset,
        )
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
pub fn merge(
    recipient: &mut File,
    segments: &mut [(impl Iterator<Item = ParagraphAddr>, &[u8])],
    config: &VectorConfig,
) -> io::Result<bool> {
    // Number of elements, deleted or alive.
    let mut prologue_section_size = HEADER_LEN;
    // To know the range of valid ids per producer
    let mut lengths = Vec::with_capacity(segments.len());

    // Computing lengths and total sizes
    for (_, store) in segments.iter() {
        let producer_elements = stored_elements(store);
        lengths.push(producer_elements);
        prologue_section_size += POINTER_LEN * producer_elements;
    }

    // Reserving space for the prologue, formed by the header
    // and the id section.
    recipient.set_len(prologue_section_size as u64)?;

    // Number of elements that are actually written in the recipient.
    let mut written_elements: usize = 0;
    // Using a buffered writer for efficient transferring.
    let mut recipient_buffer = BufWriter::new(recipient);
    // Pointer to the next unused id slot
    let mut id_section_cursor = HEADER_LEN;

    let alignment = config.vector_type.vector_alignment();
    for (alive_ids, store) in segments {
        for element_cursor in alive_ids {
            let element_cursor = element_cursor.0 as usize;
            let element_pointer = get_pointer(store, element_cursor);
            let element_slice = &store[element_pointer..];
            // Moving to the end of the file to write the current element.
            let exact_element = Node::new(element_slice);
            let mut element_pointer = recipient_buffer.seek(SeekFrom::End(0))?;
            if element_pointer as usize % alignment > 0 {
                let pad = alignment - (element_pointer as usize % alignment);
                recipient_buffer.seek(SeekFrom::Current(pad as i64))?;
                element_pointer += pad as u64;
            }
            recipient_buffer.write_all(exact_element.bytes())?;
            // Moving to the next free slot in the section id to write
            // the offset where the element was written.
            recipient_buffer.seek(SeekFrom::Start(id_section_cursor as u64))?;
            recipient_buffer.write_all(&element_pointer.to_le_bytes())?;
            id_section_cursor += POINTER_LEN;
            written_elements += 1;
        }
    }

    // Write the number of elements
    recipient_buffer.seek(SeekFrom::Start(0))?;
    recipient_buffer.write_all(&written_elements.to_le_bytes())?;
    recipient_buffer.seek(SeekFrom::Start(0))?;
    recipient_buffer.flush()?;
    Ok(written_elements < lengths.iter().sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::VectorCardinality, segment::Elem};

    const VECTOR_CONFIG: VectorConfig = VectorConfig {
        vector_type: VectorType::DenseF32 { dimension: 3 },
        similarity: crate::config::Similarity::Dot,
        normalize_vectors: false,
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

    #[test]
    fn store_test() {
        let elems = [0, 1, 2, 3, 4];
        let expected: Vec<_> = elems.iter().map(create_elem).collect();
        let mut buf = tempfile::tempfile().unwrap();
        create_key_value(&mut buf, expected.clone(), &VECTOR_CONFIG.vector_type).unwrap();

        let buf_map = unsafe { memmap2::Mmap::map(&buf).unwrap() };
        let no_values = stored_elements(&buf_map);
        assert_eq!(no_values, expected.len());
        for (id, expected_value) in expected.iter().enumerate() {
            let actual_value = get_value(&buf_map, id);
            assert_eq!(actual_value.key(), expected_value.key);
        }
    }

    fn create_elem(num: &usize) -> Elem {
        Elem::new(num.to_string(), [*num as f32; 16].to_vec(), vec![], None)
    }

    #[test]
    fn merge_test() {
        let v0: Vec<_> = [0, 2, 4].iter().map(create_elem).collect();
        let v1: Vec<_> = [1, 5, 7].iter().map(create_elem).collect();
        let v2: Vec<_> = [8, 9, 10, 11].iter().map(create_elem).collect();
        let expected = [0, 1, 2, 4, 5, 7, 8, 9, 10, 11];

        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let mut elems = vec![
            ((0..3).map(ParagraphAddr), v0_map.as_ref()),
            ((0..3).map(ParagraphAddr), v1_map.as_ref()),
            ((0..4).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut merge_store, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_map, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_some_deleted_different_length() {
        let v0: Vec<_> = [0, 1, 2].iter().map(create_elem).collect();
        let v1: Vec<_> = [3, 4, 5, 11].iter().map(create_elem).collect();
        let v2: Vec<_> = [6, 7, 8, 9, 10].iter().map(create_elem).collect();

        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let mut elems = vec![
            // zero and 1 will be removed
            ((2..3).map(ParagraphAddr), v0_map.as_ref()),
            // no element is removed
            ((0..4).map(ParagraphAddr), v1_map.as_ref()),
            // no element is removed
            ((0..5).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut merge_store, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_map, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_some_elements_deleted() {
        let v0: Vec<_> = [0, 1, 2].iter().map(create_elem).collect();
        let v1: Vec<_> = [3, 4, 5].iter().map(create_elem).collect();
        let v2: Vec<_> = [6, 7, 8].iter().map(create_elem).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let mut elems = vec![
            // zero and 1 will be removed
            ((2..3).map(ParagraphAddr), v0_map.as_ref()),
            // no element is removed
            ((0..3).map(ParagraphAddr), v1_map.as_ref()),
            // no element is removed
            ((0..3).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut merge_store, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_map, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_first_deleted() {
        let v0: Vec<_> = [0, 1, 2].iter().map(create_elem).collect();
        let v1: Vec<_> = [3, 4, 5].iter().map(create_elem).collect();
        let v2: Vec<_> = [6, 7, 8].iter().map(create_elem).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_store = tempfile::tempfile().unwrap();
        let mut elems = vec![
            // The first element is deleted
            ((1..3).map(ParagraphAddr), v0_map.as_ref()),
            // The first element is deleted
            ((1..3).map(ParagraphAddr), v1_map.as_ref()),
            // The first element is deleted
            ((1..3).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut merge_store, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let expected: Vec<u32> = vec![1, 2, 4, 5, 7, 8];
        let merge_map = unsafe { memmap2::Mmap::map(&merge_store).unwrap() };
        let number_of_elements = stored_elements(&merge_map);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_map, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_one_store_empty() {
        let v0: Vec<_> = [0, 1, 2].iter().map(create_elem).collect();
        let v1: Vec<_> = [3, 4, 5].iter().map(create_elem).collect();
        let v2: Vec<_> = [6, 7, 8].iter().map(create_elem).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut merge_storage = tempfile::tempfile().unwrap();
        let mut elems = vec![
            // all the elements are deleted
            ((0..0).map(ParagraphAddr), v0_map.as_ref()),
            // no element is removed
            ((0..3).map(ParagraphAddr), v1_map.as_ref()),
            // no element is removed
            ((0..3).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut merge_storage, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let expected: Vec<u32> = vec![3, 4, 5, 6, 7, 8];
        let merge_store = unsafe { memmap2::Mmap::map(&merge_storage).unwrap() };
        let number_of_elements = stored_elements(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_store, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }

    #[test]
    fn merge_all_deleted() {
        let v0: Vec<_> = [0, 1, 2].iter().map(create_elem).collect();
        let v1: Vec<_> = [3, 4, 5].iter().map(create_elem).collect();
        let v2: Vec<_> = [6, 7, 8].iter().map(create_elem).collect();
        let mut v0_store = tempfile::tempfile().unwrap();
        let mut v1_store = tempfile::tempfile().unwrap();
        let mut v2_store = tempfile::tempfile().unwrap();

        create_key_value(&mut v0_store, v0, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v1_store, v1, &VECTOR_CONFIG.vector_type).unwrap();
        create_key_value(&mut v2_store, v2, &VECTOR_CONFIG.vector_type).unwrap();

        let v0_map = unsafe { memmap2::Mmap::map(&v0_store).unwrap() };
        let v1_map = unsafe { memmap2::Mmap::map(&v1_store).unwrap() };
        let v2_map = unsafe { memmap2::Mmap::map(&v2_store).unwrap() };

        let mut file = tempfile::tempfile().unwrap();

        let mut elems = vec![
            // all the elements are removed
            ((0..0).map(ParagraphAddr), v0_map.as_ref()),
            // all the elements are removed
            ((0..0).map(ParagraphAddr), v1_map.as_ref()),
            // all the elements are removed
            ((0..0).map(ParagraphAddr), v2_map.as_ref()),
        ];

        merge(&mut file, elems.as_mut_slice(), &VECTOR_CONFIG).unwrap();
        let expected: Vec<u32> = vec![];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = stored_elements(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(&merge_store, i))
            .map(|s| s.key().parse().unwrap())
            .collect();

        assert_eq!(number_of_elements, values.len());
        assert_eq!(values.len(), expected.len());
        assert!(values.iter().all(|i| expected.contains(i)));
    }
}
