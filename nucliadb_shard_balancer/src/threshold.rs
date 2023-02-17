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

use std::str::FromStr;

use bounded_integer::BoundedU8;
use thiserror::Error;

/// The errors that may occur when parsing threshold string.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid plain value: {0}")]
    InvalidPlainValue(#[from] std::num::ParseIntError),
    #[error("invalid percentage: {0}")]
    InvalidPercentage(#[from] bounded_integer::ParseError),
    #[error("invalid threshold format: expected '([0-100]%|<unsigned integer>)' (got '{0}')")]
    InvalidFormat(String),
}

/// A `Thresholding` is the result of a difference between two values and a [`Threshold`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Thresholding {
    /// A thresholding where the difference value is below the threshold.
    Below,
    /// A thresholding where the difference value is above the threshold.
    Above,
}

impl Thresholding {
    /// Returns `true` is the thresholding is the `Above` variant.
    pub fn is_above(&self) -> bool {
        *self == Self::Above
    }

    /// Returns `true` is the thresholding is the `Below` variant.
    pub fn is_below(&self) -> bool {
        *self == Self::Below
    }
}

/// A type to represent a threshold with a plain value or a percentage.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Threshold {
    /// A percentage threshold.
    ///
    /// Note that the percentage by itself means nothing and makes sense only in
    /// [`Threshold::diff`] method.
    Percentage(BoundedU8<0, 100>),
    /// A plain value threshold.
    PlainValue(u64),
}

impl Threshold {
    /// Computes the difference between two values and the threshold.
    pub fn diff(&self, a: u64, b: u64) -> Thresholding {
        let (min, max) = (a.min(b), a.max(b));
        let diff = max - min;

        let threshold = match self {
            Self::Percentage(percentage) => max * u64::from(percentage.get()) / 100,
            Self::PlainValue(value) => *value,
        };

        if diff > threshold {
            Thresholding::Above
        } else {
            Thresholding::Below
        }
    }
}

impl FromStr for Threshold {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens = s.split('%').collect::<Vec<_>>();

        match tokens.len() {
            n if n > 2 || n == 0 || !tokens[0].chars().all(|c| c.is_ascii_digit()) => {
                Err(ParseError::InvalidFormat(s.to_string()))
            }
            1 => Ok(Self::PlainValue(tokens[0].parse()?)),
            2 => Ok(Self::Percentage(tokens[0].parse()?)),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_as_plain_value() {
        let value_threshold = "42".parse::<Threshold>().unwrap();

        assert_eq!(value_threshold, Threshold::PlainValue(42));
    }

    #[test]
    fn it_parses_as_percentage() {
        let value_threshold = "42%".parse::<Threshold>().unwrap();

        assert_eq!(
            value_threshold,
            Threshold::Percentage(BoundedU8::new(42).unwrap())
        );
    }

    #[test]
    fn it_rejects_invalid_percentage() {
        let e = "101%".parse::<Threshold>().unwrap_err();

        assert!(matches!(e, ParseError::InvalidPercentage(_)));
    }

    #[test]
    fn it_rejects_invalid_format() {
        let tests = ["42%%", "asflksdjgalkj", "492,45"];

        for s in tests {
            let e = s.parse::<Threshold>().unwrap_err();

            assert!(matches!(e, ParseError::InvalidFormat(_)));
        }
    }

    #[test]
    fn it_calculates_diff() {
        let tests = [
            (50, 100, Threshold::PlainValue(40), Thresholding::Above),
            (70, 100, Threshold::PlainValue(40), Thresholding::Below),
            (
                40,
                100,
                Threshold::Percentage(BoundedU8::new(50).unwrap()),
                Thresholding::Above,
            ),
            (
                60,
                100,
                Threshold::Percentage(BoundedU8::new(50).unwrap()),
                Thresholding::Below,
            ),
        ];

        for (a, b, threshold, expected_thresholding) in tests {
            assert_eq!(threshold.diff(a, b), expected_thresholding);
            assert_eq!(threshold.diff(b, a), expected_thresholding);
        }
    }
}
