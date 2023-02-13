use std::str::FromStr;

use bounded_integer::BoundedU8;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid plain value: {0}")]
    InvalidPlainValue(#[from] std::num::ParseFloatError),
    #[error("invalid percentage: {0}")]
    InvalidPercentage(#[from] bounded_integer::ParseError),
    #[error("invalid threshold format: expected '([1-100]%|<unsigned integer>)' (got '{0}')")]
    InvalidFormat(String),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Thresholding {
    Below,
    Above,
}

impl Thresholding {
    pub fn is_above(&self) -> bool {
        *self == Self::Above
    }

    pub fn is_below(&self) -> bool {
        *self == Self::Below
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Threshold {
    Percentage(BoundedU8<1, 100>),
    PlainValue(f32),
}

impl Threshold {
    pub fn diff(&self, a: f32, b: f32) -> Thresholding {
        let (min, max) = (a.min(b), a.max(b));
        let diff = max - min;

        let threshold = match self {
            Self::Percentage(percentage) => max * (percentage.get() as f32 / 100f32),
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens = s.split('%').collect::<Vec<_>>();

        match tokens.len() {
            n if n > 2 || n == 0 || !tokens[0].chars().all(|c| c.is_ascii_digit()) => {
                Err(Error::InvalidFormat(s.to_string()))
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
        let value_threshold = "34123947".parse::<Threshold>().unwrap();

        assert_eq!(value_threshold, Threshold::PlainValue(34123947.0));
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

        assert!(matches!(e, Error::InvalidPercentage(_)));
    }

    #[test]
    fn it_rejects_invalid_format() {
        let tests = ["42%%", "asflksdjgalkj", "-44%", "-24932472"];

        for s in tests {
            let e = s.parse::<Threshold>().unwrap_err();

            assert!(matches!(e, Error::InvalidFormat(_)));
        }
    }

    #[test]
    fn it_calculates_diff() {
        let tests = [
            (
                50.0,
                100.0,
                Threshold::PlainValue(40f32),
                Thresholding::Above,
            ),
            (
                70.0,
                100.0,
                Threshold::PlainValue(40f32),
                Thresholding::Below,
            ),
            (
                40.0,
                100.0,
                Threshold::Percentage(BoundedU8::new(50).unwrap()),
                Thresholding::Above,
            ),
            (
                60.0,
                100.0,
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
