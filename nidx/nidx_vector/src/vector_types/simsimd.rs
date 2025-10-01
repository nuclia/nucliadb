#[cfg(all(target_os = "linux", not(target_arch = "aarch64")))]
pub use simsimd::SpatialSimilarity;

/// Fallback implementation of simsimd. This is used because manylinux for ARM64
/// uses GCC 7.5.0 which is not able to compile simsimd (missing arm_sve.h headers)
/// Once manylinux with GCC 8+ is released, we could delete this.
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
mod simsimd {
    type Distance = f64;
    pub trait SpatialSimilarity
    where
        Self: Sized,
    {
        fn cosine(a: &[Self], b: &[Self]) -> Option<Distance>;
        fn dot(a: &[Self], b: &[Self]) -> Option<Distance>;
    }

    impl SpatialSimilarity for f32 {
        fn cosine(a: &[Self], b: &[Self]) -> Option<Distance> {
            let len = a.len();
            let mut sum = 0.0;
            let mut dem_x = 0.0;
            let mut dem_y = 0.0;
            for i in 0..len {
                sum += a[i] * b[i];
                dem_x += a[i] * a[i];
                dem_y += b[i] * b[i];
            }
            Some(1.0 - (sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))) as f64)
        }

        fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
            let len = a.len();
            let mut sum = 0.0;
            for i in 0..len {
                sum += a[i] * b[i];
            }
            Some(sum as f64)
        }
    }
}
