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

pub fn normalize_vector(vector: &[f32]) -> Vec<f32> {
    let magnitude = f32::sqrt(vector.iter().fold(0.0, |acc, x| acc + x.powi(2)));
    vector.iter().map(|x| *x / magnitude).collect()
}

pub mod field_id {
    use tracing::warn;

    /// The key for the field index. [uuid_as_bytes, field_type/field_name]
    pub fn key(field_id: &str) -> Option<Vec<u8>> {
        let mut parts = field_id.split('/');
        if let Some(uuid) = parts.next() {
            if let Some(field_type) = parts.next() {
                if let Some(field_name) = parts.next() {
                    return Some(
                        [
                            uuid::Uuid::parse_str(uuid).unwrap().as_bytes(),
                            field_type.as_bytes(),
                            "/".as_bytes(),
                            field_name.as_bytes(),
                        ]
                        .concat(),
                    );
                }
            } else {
                return Some(uuid::Uuid::parse_str(uuid).unwrap().as_bytes().to_vec());
            }
        }
        warn!(?field_id, "Unable to parse field id from str");
        None
    }

    /// Returns the resource part of the key, the first 128 bits (uuid size)
    pub fn resource_part(key: &[u8]) -> &[u8] {
        &key[0..8]
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
