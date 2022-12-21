use std::marker::PhantomData;

use derive_more::Display;

/// An abstraction over string literal to bind a type to a key.
#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[display(fmt = "{}", "self.name")]
pub struct Key<T> {
    name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> Key<T> {
    /// Contructs a new key.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }

    /// Returns the name of the key.
    pub const fn as_str(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_constructs_key_in_const_context() {
        const _KEY: Key<u32> = Key::new("a key");
    }

    #[test]
    fn it_returns_key_as_str() {
        let key: Key<u32> = Key::new("a key");

        assert_eq!(key.as_str(), "a key");
    }
}
