//! Set of parameters needed by the HNSW algorithm
//! as named and used in the paper.

pub fn level_factor() -> f64 {
    1.0 / (m() as f64).ln()
}
pub const fn m_max() -> usize {
    30
}
pub const fn m() -> usize {
    30
}
pub const fn ef_construction() -> usize {
    100
}
