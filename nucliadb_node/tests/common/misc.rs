pub fn linear_backoff(value: u64, factor: u64) -> u64 {
    value * factor
}
