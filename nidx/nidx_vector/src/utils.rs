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

use std::hash::Hash;

use tracing::warn;

pub fn normalize_vector(vector: &[f32]) -> Vec<f32> {
    let magnitude = f32::sqrt(vector.iter().fold(0.0, |acc, x| acc + x.powi(2)));
    vector.iter().map(|x| *x / magnitude).collect()
}

/// Represents a field id as encoded in the indexes:
///   Resource ID (UUID): encoded in binary
///   Field type (string): e.g: t, a
///   Separator `/`
///   Field name (string): e.g: title
///
/// For deletions, this can also be just the resource ID, in which case only the UUID is encoded.
#[derive(Clone, Eq)]
pub enum FieldKey<'a> {
    Owned(Vec<u8>),
    Borrowed(&'a [u8]),
}

impl<'a> PartialEq for FieldKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<'a> Hash for FieldKey<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes().hash(state)
    }
}

impl<'a> bincode::Encode for FieldKey<'a> {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.bytes().encode(encoder)
    }
}

impl<'a, 'de: 'a, Context> bincode::BorrowDecode<'de, Context> for FieldKey<'a> {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = <&[u8]>::borrow_decode(decoder)?;
        Ok(Self::Borrowed(bytes))
    }
}

impl<'a> FieldKey<'a> {
    pub fn from_field_id(field_id: &str) -> Option<Self> {
        let mut parts = field_id.split('/');
        let Some(uuid) = parts.next() else {
            warn!(?field_id, "Unable to parse field id: empty");
            return None;
        };
        let Ok(rid) = uuid::Uuid::parse_str(uuid) else {
            warn!(?field_id, "Unable to parse field id: invalid UUID");
            return None;
        };
        if let Some(field_type) = parts.next() {
            if let Some(field_name) = parts.next() {
                Some(FieldKey::Owned(
                    [
                        rid.as_bytes(),
                        field_type.as_bytes(),
                        "/".as_bytes(),
                        field_name.as_bytes(),
                    ]
                    .concat(),
                ))
            } else {
                warn!(?field_id, "Unable to parse field id: has field type but no name");
                None
            }
        } else {
            Some(FieldKey::Owned(
                uuid::Uuid::parse_str(uuid).unwrap().as_bytes().to_vec(),
            ))
        }
    }

    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Self::Borrowed(bytes)
    }

    pub fn resource_id(&self) -> &[u8] {
        &self.bytes()[0..8]
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            FieldKey::Owned(d) => d,
            FieldKey::Borrowed(d) => d,
        }
    }

    pub fn to_owned(&self) -> FieldKey<'static> {
        match self {
            FieldKey::Owned(d) => FieldKey::Owned(d.clone()),
            FieldKey::Borrowed(d) => FieldKey::Owned(d.to_vec()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_normalization() {
        let normal = normalize_vector(&[]);
        assert!(normal.is_empty());

        let normal = normalize_vector(&[3.0, 0.0, 4.0, 0.0]);
        assert_eq!(normal, vec![3.0 / 5.0, 0.0, 4.0 / 5.0, 0.0]);

        let normal = normalize_vector(&[-1.0, -1.0, 0.0, 1.0, 1.0]);
        assert_eq!(normal, vec![-0.5, -0.5, 0.0, 0.5, 0.5]);

        // try it out with big vectors
        let normal = normalize_vector(&vec![100.0; 10000]);
        assert_eq!(normal[0], 0.01);
        assert_eq!(normal, vec![0.01; 10000]);
    }
}
