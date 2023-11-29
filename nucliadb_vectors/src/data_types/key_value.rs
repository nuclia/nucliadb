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

use std::cmp::Ordering;
use std::io::{self, Seek, SeekFrom, Write};

use super::usize_utils::*;

// A key-value store schema.
// The data is arrange in a way such that the following operations can be performed:
// -> Given a key, find its value. O(log(n))
// -> Given an id, find its value. O(1).
// N = Address size in bytes.
// Serialization schema:
//
// [ number of values: N bytes in little endian.   ] --> Header
// [ sequence of pointers sorted by the keys.      ]
// [ slots: in sequence.                           ]
//
// A pointer in idx is the start of a value, stored N bytes in little endian)
// It might not be obvious why the list of pointers is needed. It is because the slots may be of
// variable length, which would render a binary search imposible. Idx elements do have a fixed
// length (N bytes).

const N: usize = USIZE_LEN;
const HEADER_LEN: usize = N;
const POINTER_LEN: usize = N;
pub type Pointer = usize;
pub type HeaderE = usize;

// Given an index i, the start of its element on idx is:
//              [i*IDXE_LEN + HEADER_LEN]
pub fn get_pointer(x: &[u8], i: usize) -> Pointer {
    let start = (i * POINTER_LEN) + HEADER_LEN;
    let end = start + POINTER_LEN;
    let mut buff = [0; POINTER_LEN];
    buff.copy_from_slice(&x[start..end]);
    usize::from_le_bytes(buff)
}

// O(1)
// Returns how many elements are in x, alive or deleted.
pub fn elements_in_total(x: &[u8]) -> HeaderE {
    usize_from_slice_le(&x[..HEADER_LEN])
}

pub trait Slot {
    // Is a mistake to assume that x does not have trailing bytes at the end.
    // Is safe to assume that there is an i such that x[0],.., x[i] is a valid
    // Slot.
    fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8];
    // Is a mistake to assume that x does not have trailing bytes at the end.
    // Is safe to assume that there is an i such that x[0],.., x[i] is a valid
    // Slot.
    fn cmp_keys(&self, x: &[u8], key: &[u8]) -> Ordering;
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
    // Is a mistake to assume that x and y do not have trailing bytes at the end.
    // Is safe to assume that there is an i such that x[0],.., x[i] is a valid Slot.
    // Is safe to assume that there is an j such that y[0],.., y[k] is a valid Slot.
    fn cmp_slot(&self, x: &[u8], y: &[u8]) -> Ordering {
        let y_key = self.get_key(y);
        self.cmp_keys(x, y_key)
    }
}

pub trait KVElem {
    fn serialized_len(&self) -> usize;
    fn serialize_into<W: io::Write>(self, w: W) -> io::Result<()>;
}

impl<T: AsRef<[u8]>> KVElem for T {
    fn serialized_len(&self) -> usize {
        self.as_ref().len()
    }
    fn serialize_into<W: io::Write>(self, mut w: W) -> io::Result<()> {
        w.write_all(self.as_ref())
    }
}

#[allow(unused)]
pub fn new_key_value<S>(slots: Vec<S>) -> Vec<u8>
where S: KVElem {
    let mut buf = vec![];
    create_key_value(&mut buf, slots).unwrap();
    buf
}
// Slots are expected to be sorted by their key.
pub fn create_key_value<S, W>(mut at: W, slots: Vec<S>) -> io::Result<()>
where
    S: KVElem,
    W: Write,
{
    let no_values: [u8; POINTER_LEN] = slots.len().to_le_bytes();
    at.write_all(&no_values)?;
    slots.iter().try_fold::<_, _, io::Result<_>>(
        HEADER_LEN + (slots.len() * POINTER_LEN),
        |pos, slot| {
            let pbytes: [u8; POINTER_LEN] = pos.to_le_bytes();
            at.write_all(&pbytes)?;
            Ok(pos + slot.serialized_len())
        },
    )?;
    slots
        .into_iter()
        .try_for_each(|slot| slot.serialize_into(&mut at))
}

// O(log n) where n is the number of slots in src.
#[allow(unused)]
pub fn search_by_key<S: Slot>(interface: S, src: &[u8], key: &[u8]) -> Option<usize> {
    let number_of_values = elements_in_total(src);
    let mut start = 0;
    let mut end = number_of_values;
    let mut found = None;
    while start < end && found.is_none() {
        let m = start + ((end - start) / 2);
        let slot_start = get_pointer(src, m);
        let slot = &src[slot_start..];
        match interface.cmp_keys(slot, key) {
            Ordering::Equal => {
                found = Some(m);
            }
            Ordering::Less => {
                start = m + 1;
            }
            Ordering::Greater => {
                end = m;
            }
        }
    }
    found
}

// O(1)
pub fn get_value<S: Slot>(interface: S, src: &[u8], id: usize) -> &[u8] {
    let pointer = get_pointer(src, id);
    interface.read_exact(&src[pointer..]).0
}

// Returns all the keys stored at the serialized key-value 'x'
// O(1)
pub fn get_keys<'a, S: Slot + Copy + 'a>(
    interface: S,
    x: &'a [u8],
) -> impl Iterator<Item = &'a [u8]> {
    (0..elements_in_total(x))
        .map(move |i| get_value(interface, x, i))
        .map(move |v| interface.get_key(v))
}

// Returns the number of alive elements in the store and the space they consume.
fn get_metrics<S: Slot>(interface: S, source: &[u8]) -> (usize, usize) {
    let len = elements_in_total(source);
    let mut value_space = 0;
    let mut no_elems = 0;
    for id in 0..len {
        let ptr = get_pointer(source, id);
        let (elem, _) = interface.read_exact(&source[ptr..]);
        if interface.keep_in_merge(elem) {
            value_space += elem.len();
            no_elems += 1;
        }
    }
    (no_elems, value_space)
}

// Merge algorithm

/// Given the ids of unfinished producers, this function will find the minimum
/// alive element.
fn minimum_alive_value<'a, S: Slot + Copy>(
    unfinished: &[usize],
    status: &[usize],
    producers: &[(S, &'a [u8])],
) -> Option<&'a [u8]> {
    let mut minimum = None;
    for producer in unfinished.iter().copied() {
        let next_elem = status[producer];
        let (interface, store) = producers[producer];
        let element_pointer = get_pointer(store, next_elem);
        let (element_slice, _) = interface.read_exact(&store[element_pointer..]);
        match minimum {
            _ if !interface.keep_in_merge(element_slice) => (),
            None => {
                minimum = Some(element_slice);
            }
            Some(minimum_slice) => {
                minimum = Some(std::cmp::min_by(minimum_slice, element_slice, |i, j| {
                    interface.cmp_slot(i, j)
                }));
            }
        }
    }
    minimum
}

/// Moves the cursor to the next alive element.
fn advance_producer<S: Slot + Copy>(interface: S, cursor: &mut usize, length: usize, store: &[u8]) {
    while *cursor < length {
        // cursor points to an in-range element.
        let element_pointer = get_pointer(store, *cursor);
        let element_slice = &store[element_pointer..];
        if interface.keep_in_merge(element_slice) {
            // The element is alive, we can stop
            break;
        }

        // The element is not alive,
        // the cursor is moved to the next one.
        *cursor += 1;
    }
}

/// The status of every producer pointing to the current minimum or to a deleted element
/// will be changed to the next element.
fn advance_merge<S: Slot + Copy>(
    minimum: Option<&[u8]>,
    unfinished: &mut Vec<usize>,
    status: &mut [usize],
    producers: &[(S, &[u8])],
    lengths: &[usize],
) {
    let current_work = std::mem::take(unfinished);
    for producer in current_work.iter().copied() {
        let next_elem = status[producer];
        let (interface, store) = producers[producer];
        let element_pointer = get_pointer(store, next_elem);
        let element_slice = &store[element_pointer..];

        if minimum.map_or(false, |m| interface.cmp_slot(m, element_slice).is_eq()) {
            status[producer] += 1;
        }

        advance_producer(interface, &mut status[producer], lengths[producer], store);

        if status[producer] < lengths[producer] {
            unfinished.push(producer)
        }
    }
}

// Entry point for the merge algorithm. Returns the number of elements merged into the file.
// WARNING: In case of keys duplicatied keys it favors the contents of the first slot.
pub fn merge<S, R>(recipient: &mut R, producers: &[(S, &[u8])]) -> io::Result<usize>
where
    S: Slot + Copy,
    R: Write + Seek,
{
    // Number of elements that will be transferred to the recipient.
    let mut number_of_elements = 0;
    // Space required to perform the merge.
    let mut value_space = 0;
    // Index of the producers that have alive elements.
    let mut non_empty_producers = Vec::with_capacity(producers.len());
    // Number of total elements (alive or deleted) each producer has
    let mut lens = Vec::with_capacity(producers.len());
    // Per producer, which element should be visited next.
    let mut status = Vec::with_capacity(producers.len());

    // Initializing merge loop parameters
    for (id, (interface, store)) in producers.iter().copied().enumerate() {
        let total_elements = elements_in_total(store);
        let (alive_elements, space) = get_metrics::<S>(interface, store);
        number_of_elements += alive_elements;
        value_space += space;

        let mut cursor = total_elements;
        if alive_elements > 0 {
            cursor = 0;
            advance_producer(interface, &mut cursor, total_elements, store);
        }
        if cursor < total_elements {
            non_empty_producers.push(id);
        }

        status.push(cursor);
        lens.push(total_elements);
    }

    // Taking deletions into account, how many elements will be on the recipient.
    let number_of_elements = number_of_elements;
    // Taking deletions into account, the number of bytes required to fit them.
    let value_space = value_space;
    // Per producer, the number of elements it has (deleted included).
    let producer_lengths = lens;
    // Unfinished producers, initialized to every non-empty producer.
    let mut unfinished = non_empty_producers;
    // At most, the merge will need total_space bytes.
    let total_space = HEADER_LEN + (POINTER_LEN * number_of_elements) + value_space;
    // Number of elements that have been written into the recipient.
    let mut written_elements = 0;
    // Length of the recipient's written part
    let mut recipient_length = HEADER_LEN + (POINTER_LEN * number_of_elements);

    // Reserving enough space to fit the merge result.
    for _ in 0..total_space {
        recipient.write_all(&[0])?;
    }

    // Merge loop, transfers the contents of each producer into the recipient in order
    while !unfinished.is_empty() {
        let current_minimum = minimum_alive_value(&unfinished, &status, producers);
        if let Some(minimum) = current_minimum {
            // There is a minimum that needs to be transferred to the recipient.
            let idx_slot = (HEADER_LEN + (written_elements * POINTER_LEN)) as u64;
            recipient.seek(SeekFrom::Start(idx_slot))?;
            recipient.write_all(&recipient_length.to_le_bytes())?;
            recipient.seek(SeekFrom::Start(recipient_length as u64))?;
            recipient.write_all(minimum)?;
            recipient_length += minimum.len();
            written_elements += 1;
        }

        // Every producer is  advanced.
        advance_merge(
            current_minimum,
            &mut unfinished,
            &mut status,
            producers,
            &producer_lengths,
        );
    }

    // Write the number of elements
    recipient.seek(SeekFrom::Start(0))?;
    recipient.write_all(&written_elements.to_le_bytes())?;
    recipient.seek(SeekFrom::Start(0)).unwrap();
    recipient.flush()?;
    Ok(written_elements)
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
            TElem.cmp_keys(x, &self.0).is_le()
        }
    }

    // u32 numbers in big-endian (so cmp is faster)
    #[derive(Clone, Copy)]
    struct TElem;
    impl Slot for TElem {
        fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8] {
            &x[0..4]
        }
        fn cmp_keys(&self, x: &[u8], key: &[u8]) -> Ordering {
            x[0..4].cmp(&key[0..4])
        }
        fn read_exact<'a>(&self, x: &'a [u8]) -> (&'a [u8], &'a [u8]) {
            x.split_at(4)
        }
    }

    fn store_checks(expected: &[impl AsRef<[u8]>], buf: &[u8]) {
        let no_values = elements_in_total(buf);
        assert_eq!(no_values, expected.len());
        for (i, item) in expected.iter().enumerate() {
            let value_ptr = get_pointer(buf, i);
            let (exact, _) = TElem.read_exact(&buf[value_ptr..]);
            assert_eq!(exact, item.as_ref());
        }
    }

    fn retrieval_checks(expected: &[impl AsRef<[u8]>], buf: &[u8]) {
        let interface = TElem;
        let mut expected_keys = vec![];
        for i in 0..expected.len() {
            let id =
                search_by_key(interface, buf, interface.get_key(expected[i].as_ref())).unwrap();
            let value = get_value(interface, buf, id);
            let key = interface.get_key(value);
            let (head, tail) = interface.read_exact(value);
            expected_keys.push(key);
            assert_eq!(id, i);
            assert_eq!(value, head);
            assert_eq!(tail, &[] as &[u8]);
            assert_eq!(value, expected[id].as_ref());
        }
        let got_keys = get_keys(interface, buf).collect::<Vec<_>>();
        assert_eq!(expected_keys, got_keys);
    }
    #[test]
    fn store_test() {
        let elems: [u32; 5] = [0, 1, 2, 3, 4];
        let encoded: Vec<_> = elems.iter().map(|x| x.to_be_bytes()).collect();
        let mut buf = Vec::new();
        create_key_value(&mut buf, encoded.clone()).unwrap();
        store_checks(&encoded, &buf);
    }
    #[test]
    fn retrieval_test() {
        let elems: [u32; 5] = [0, 1, 2, 3, 4];
        let encoded: Vec<_> = elems.iter().map(|x| x.to_be_bytes()).collect();
        let mut buf = Vec::new();
        create_key_value(&mut buf, encoded.clone()).unwrap();
        store_checks(&encoded, &buf);
        retrieval_checks(&encoded, &buf);
    }
    #[test]
    fn merge_test() {
        use std::io::Read;
        let v0: Vec<_> = [0u32, 2, 4].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [1u32, 2, 5, 7].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [8u32, 9, 10, 11].iter().map(|x| x.to_be_bytes()).collect();
        let v3: Vec<_> = [1u32, 2, 5, 7].iter().map(|x| x.to_be_bytes()).collect();
        let expected: Vec<_> = [0u32, 1, 2, 4, 5, 7, 8, 9, 10, 11]
            .iter()
            .map(|x| x.to_be_bytes())
            .collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        let mut v3_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();
        create_key_value(&mut v3_store, v3).unwrap();
        let mut file = tempfile::tempfile().unwrap();
        let elems: Vec<_> = [&v0_store, &v1_store, &v2_store, &v3_store]
            .into_iter()
            .map(|e| (TElem, e.as_slice()))
            .collect();
        merge(&mut file, &elems).unwrap();
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        store_checks(&expected, &buf);
        retrieval_checks(&expected, &buf);
    }

    #[test]
    fn merge_some_deleted_different_length() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5, 11].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8, 9, 10]
            .iter()
            .map(|x| x.to_be_bytes())
            .collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let interface = (GreaterThan(ONE), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // zero and 1 will be removed
            (interface, v0_store.as_slice()),
            // no element is removed
            (interface, v1_store.as_slice()),
            // no element is removed
            (interface, v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn merge_some_elements_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let interface = (GreaterThan(ONE), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // zero and 1 will be removed
            (interface, v0_store.as_slice()),
            // no element is removed
            (interface, v1_store.as_slice()),
            // no element is removed
            (interface, v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![2, 3, 4, 5, 6, 7, 8];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn merge_first_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // The first element is deleted
            ((GreaterThan(ZERO), TElem), v0_store.as_slice()),
            // The first element is deleted
            ((GreaterThan(THREE), TElem), v1_store.as_slice()),
            // The first element is deleted
            ((GreaterThan(SIX), TElem), v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![1, 2, 4, 5, 7, 8];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn merge_one_store_empty() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let interface = (GreaterThan(TWO), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // all the elements are deleted
            (interface, v0_store.as_slice()),
            // no element is removed
            (interface, v1_store.as_slice()),
            // no element is removed
            (interface, v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![3, 4, 5, 6, 7, 8];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn merge_with_different_interfaces() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let greater_than_2 = (GreaterThan(TWO), TElem);
        let greater_than_10 = (GreaterThan(TEN), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // all the elements are removed
            (greater_than_2, v0_store.as_slice()),
            // no element are removed
            (greater_than_2, v1_store.as_slice()),
            // all the elements are removed
            (greater_than_10, v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![3, 4, 5];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn merge_all_deleted() {
        let v0: Vec<_> = [0u32, 1, 2].iter().map(|x| x.to_be_bytes()).collect();
        let v1: Vec<_> = [3u32, 4, 5].iter().map(|x| x.to_be_bytes()).collect();
        let v2: Vec<_> = [6u32, 7, 8].iter().map(|x| x.to_be_bytes()).collect();
        let mut v0_store = vec![];
        let mut v1_store = vec![];
        let mut v2_store = vec![];
        create_key_value(&mut v0_store, v0).unwrap();
        create_key_value(&mut v1_store, v1).unwrap();
        create_key_value(&mut v2_store, v2).unwrap();

        let interface = (GreaterThan(TEN), TElem);
        let mut file = tempfile::tempfile().unwrap();

        let elems = vec![
            // all the elements are removed
            (interface, v0_store.as_slice()),
            // all the elements are removed
            (interface, v1_store.as_slice()),
            // all the elements are removed
            (interface, v2_store.as_slice()),
        ];
        merge::<(GreaterThan, TElem), std::fs::File>(&mut file, elems.as_slice()).unwrap();
        let expected: Vec<u32> = vec![];
        let merge_store = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let number_of_elements = elements_in_total(&merge_store);
        let values: Vec<u32> = (0..number_of_elements)
            .map(|i| get_value(TElem, &merge_store, i))
            .map(|s| u32::from_be_bytes(s.try_into().unwrap()))
            .collect();
        assert_eq!(values, expected);
    }
}
