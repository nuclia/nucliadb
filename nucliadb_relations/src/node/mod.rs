use std::fmt::Debug;

use nucliadb_byte_rpr::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResourceData {
    pub name: String,
}
impl ByteRpr for ResourceData {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut name_encoding = self.name.as_byte_rpr();
        let len = name_encoding.len() as u64;
        let mut len_encoding = len.as_byte_rpr();
        let mut encoding = vec![];
        encoding.append(&mut len_encoding);
        encoding.append(&mut name_encoding);
        encoding
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let len_start = 0;
        let len_end = len_start + u64::segment_len();
        let len = u64::from_byte_rpr(&bytes[len_start..len_end]);
        let name_start = len_end;
        let name_end = name_start + (len as usize);
        ResourceData {
            name: String::from_byte_rpr(&bytes[name_start..name_end]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityData {
    pub name: String,
}

impl ByteRpr for EntityData {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut name_encoding = self.name.as_byte_rpr();
        let len = name_encoding.len() as u64;
        let mut len_encoding = len.as_byte_rpr();
        let mut encoding = vec![];
        encoding.append(&mut len_encoding);
        encoding.append(&mut name_encoding);
        encoding
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let len_start = 0;
        let len_end = len_start + u64::segment_len();
        let len = u64::from_byte_rpr(&bytes[len_start..len_end]);
        let name_start = len_end;
        let name_end = name_start + (len as usize);
        EntityData {
            name: String::from_byte_rpr(&bytes[name_start..name_end]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LabelData {
    pub name: String,
}
impl ByteRpr for LabelData {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut name_encoding = self.name.as_byte_rpr();
        let len = name_encoding.len() as u64;
        let mut len_encoding = len.as_byte_rpr();
        let mut encoding = vec![];
        encoding.append(&mut len_encoding);
        encoding.append(&mut name_encoding);
        encoding
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let len_start = 0;
        let len_end = len_start + u64::segment_len();
        let len = u64::from_byte_rpr(&bytes[len_start..len_end]);
        let name_start = len_end;
        let name_end = name_start + (len as usize);
        LabelData {
            name: String::from_byte_rpr(&bytes[name_start..name_end]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColabData {
    pub name: String,
}
impl ByteRpr for ColabData {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut name_encoding = self.name.as_byte_rpr();
        let len = name_encoding.len() as u64;
        let mut len_encoding = len.as_byte_rpr();
        let mut encoding = vec![];
        encoding.append(&mut len_encoding);
        encoding.append(&mut name_encoding);
        encoding
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let len_start = 0;
        let len_end = len_start + u64::segment_len();
        let len = u64::from_byte_rpr(&bytes[len_start..len_end]);
        let name_start = len_end;
        let name_end = name_start + (len as usize);
        ColabData {
            name: String::from_byte_rpr(&bytes[name_start..name_end]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn node_serialization() {
        let name = "Some generic name that goes everywhere".to_string();
        let resource = ResourceData { name: name.clone() };
        let entity = EntityData { name: name.clone() };
        let label = LabelData { name: name.clone() };
        let colaborator = ColabData { name: name.clone() };
        assert_eq!(
            resource,
            ResourceData::from_byte_rpr(&resource.as_byte_rpr())
        );
        assert_eq!(
            colaborator,
            ColabData::from_byte_rpr(&colaborator.as_byte_rpr())
        );
        assert_eq!(entity, EntityData::from_byte_rpr(&entity.as_byte_rpr()));
        assert_eq!(label, LabelData::from_byte_rpr(&label.as_byte_rpr()));
    }
}
