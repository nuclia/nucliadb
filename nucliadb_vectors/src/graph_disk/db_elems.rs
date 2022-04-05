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

pub trait DBElem {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Self;
}

impl DBElem for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        std::str::from_utf8(bytes).unwrap().to_string()
    }
}

impl DBElem for crate::graph_elems::NodeId {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl DBElem for crate::graph_elems::LabelId {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl DBElem for super::LogField {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl DBElem for super::DiskNode {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl DBElem for super::Label {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

impl DBElem for Vec<u8> {
    fn serialize(&self) -> Vec<u8> {
        self.clone()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

impl DBElem for usize {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}
