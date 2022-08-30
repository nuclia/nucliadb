use super::VectorEngine;
use nucliadb_vectors2::data_point::{DataPoint, Elem, LabelDictionary};
use nucliadb_vectors2::data_point_provider::*;

struct Request<'a>(usize, &'a [f32]);
impl<'a> SearchRequest for Request<'a> {
    fn get_query(&self) -> &[f32] {
        self.1
    }

    fn get_labels(&self) -> &[String] {
        &[]
    }

    fn no_results(&self) -> usize {
        self.0
    }
}
impl VectorEngine for Index {
    fn add_batch(&mut self, batch_id: String, keys: Vec<String>, embeddings: Vec<Vec<f32>>) {
        let mut elems = vec![];
        for (key, vector) in keys.into_iter().zip(embeddings.into_iter()) {
            let elem = Elem::new(key, vector, LabelDictionary::new(vec![]));
            elems.push(elem);
        }
        let new_dp = DataPoint::new(self.get_location(), elems).unwrap();
        let lock = self.get_elock().unwrap();
        self.add(batch_id, new_dp, &lock);
        self.commit(lock).unwrap();
    }

    fn search(&self, no_results: usize, query: &[f32]) {
        let lock = self.get_slock().unwrap();
        self.search(&Request(no_results, query), &lock).unwrap();
    }
}
