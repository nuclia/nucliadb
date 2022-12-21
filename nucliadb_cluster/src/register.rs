use std::collections::HashSet;

use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};

/// A register to keep track of user-defined keys in node state.
#[derive(
    Debug, Display, Default, Clone, PartialEq, Eq, Serialize, Deserialize, Deref, DerefMut,
)]
#[display(fmt = "{}", "self.encode()")]
pub(crate) struct Register(HashSet<String>);

impl Register {
    /// Constructs a `Register` with the given list of keys.
    pub fn with_keys(keys: HashSet<String>) -> Self {
        Self(keys)
    }

    /// Encodes the register into a string representation.
    ///
    /// Note that if encoding fails for some reason, the resulting
    /// serial will be empty.
    pub fn encode(&self) -> String {
        // this `unwrap` call is safe since
        serde_json::to_string(&self).unwrap_or_default()
    }

    /// Decodes the register from the given string.
    ///
    /// Note that if the decoding fails for some reason, the register
    /// will end up empty.
    pub fn decode(s: &str) -> Self {
        serde_json::from_str(s).unwrap_or_default()
    }
}

impl From<&str> for Register {
    fn from(s: &str) -> Self {
        Self::decode(s)
    }
}
