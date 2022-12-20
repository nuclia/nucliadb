use std::marker::PhantomData;

use derive_more::Display;

/// An abstraction over string literal to bind a type to a key.
#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[display(fmt = "{}", name)]
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
    pub const fn name(&self) -> &'static str {
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
    fn it_returns_key_name() {
        let key: Key<u32> = Key::new("a key");

        assert_eq!(key.name(), "a key");
    }
}
