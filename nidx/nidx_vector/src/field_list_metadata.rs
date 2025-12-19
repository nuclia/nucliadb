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

use std::collections::HashSet;

use crate::{data_store::ParagraphRef, utils::FieldKey};

pub fn encode_field_list_metadata(encoded_fields: &[FieldKey]) -> Vec<u8> {
    bincode::encode_to_vec(encoded_fields, bincode::config::standard()).unwrap()
}

pub fn decode_field_list_metadata(data: &[u8]) -> Vec<FieldKey<'_>> {
    let (fields, _) = bincode::borrow_decode_from_slice(data, bincode::config::standard()).unwrap();
    fields
}

pub fn paragraph_alive_fields<'a>(
    paragraph: &'a ParagraphRef,
    keys: &HashSet<FieldKey<'a>>,
) -> impl Iterator<Item = FieldKey<'a>> {
    let fields = decode_field_list_metadata(paragraph.metadata());
    fields
        .into_iter()
        .filter(|f| !keys.contains(f) && !keys.contains(&FieldKey::from_bytes(f.resource_id())))
}

pub fn paragraph_is_deleted(paragraph: &ParagraphRef, keys: &HashSet<FieldKey>) -> bool {
    paragraph_alive_fields(paragraph, keys).next().is_none()
}
