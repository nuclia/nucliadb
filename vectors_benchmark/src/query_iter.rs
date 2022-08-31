pub struct QueryIter(usize);
impl QueryIter {
    pub fn new(dim: usize) -> QueryIter {
        QueryIter(dim)
    }
}
impl Iterator for QueryIter {
    type Item = Vec<f32>;
    fn next(&mut self) -> Option<Self::Item> {
        let new = (0..self.0)
            .into_iter()
            .map(|_| rand::random::<f32>())
            .collect();
        Some(new)
    }
}
