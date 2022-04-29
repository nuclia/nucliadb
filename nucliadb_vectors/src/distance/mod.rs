mod cosine_distance;
mod simple_avx;
mod simple_neon;
mod simple_sse;

pub use crate::distance::cosine_distance::cosine_distance;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::cosine_distance::cosine_distance;

    #[test]
    fn test_cosine_distance() {
        fn power_sqrt(v: &Vec<f32>) -> f32 {
            f32::sqrt(v.iter().cloned().fold(0.0, |acc, x| acc + (x * x)))
        }

        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 2.0, 6.0];

        assert_eq!(power_sqrt(&a), f32::sqrt(1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0));

        let distance = (1.0 * 4.0 + 2.0 * 2.0 + 3.0 * 6.0) / (power_sqrt(&a) * power_sqrt(&b));
        assert_eq!(distance, cosine_distance(&a, &b));
    }
}
