use core::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Default)]
pub struct VectorIdentifier {
    pub doc_id: Uuid,
    pub field: String,
    pub paragraph_id: Uuid,
    pub start: i32,
    pub end: i32,
}

impl FromStr for VectorIdentifier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('/').collect();

        let start_end: Vec<_> = parts.last().unwrap().split('-').collect();

        Ok(VectorIdentifier {
            doc_id: Uuid::from_str(parts[0]).unwrap(),
            field: parts[1].to_string(),
            paragraph_id: Uuid::from_str(parts[2]).unwrap(),
            start: start_end[0].parse().unwrap(),
            end: start_end[1].parse().unwrap(),
        })
    }
}

// f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
// EX: UUID/field1/UUID/20-200
impl fmt::Display for VectorIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}-{}",
            self.doc_id,
            self.field,
            self.paragraph_id,
            self.start,
            self.end
        )
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use uuid::Uuid;

    use crate::vectors::VectorIdentifier;

    #[test]
    fn test_from_str() {
        // Format: f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
        // Format: UUID/field1/UUID/20-200

        let input =
            "4ffa4021-0932-4835-bd92-19e92c047293/body/250c7835-1736-4776-afa0-08490c647cb0/10-20";
        let result = VectorIdentifier::from_str(input).unwrap();

        assert_eq!(
            result,
            VectorIdentifier {
                doc_id: Uuid::from_str("4ffa4021-0932-4835-bd92-19e92c047293").unwrap(),
                field: "body".to_string(),
                paragraph_id: Uuid::from_str("250c7835-1736-4776-afa0-08490c647cb0").unwrap(),
                start: 10,
                end: 20
            }
        );
    }
}
