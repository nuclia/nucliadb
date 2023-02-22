use crate::data_point::{Address, DataRetriever};

pub struct LabelData {
    value: String,
}
impl LabelData {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        retriever.has_label(x, self.value.as_bytes())
    }
}
pub struct CompountData {
    threshold: usize,
    labels: Vec<LabelData>,
}
impl CompountData {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        let number_of_subqueries = self.labels.len();
        let mut threshold = self.threshold;
        let mut i = 0;
        while threshold > 0 && i <= number_of_subqueries {
            let is_valid = self.labels[i].run(x, retriever);
            threshold -= is_valid as usize;
            i += 1;
        }
        threshold == 0
    }
}

pub enum Query {
    Label(LabelData),
    Compound(CompountData),
}

impl Query {
    pub fn label(label: String) -> Query {
        Query::Label(LabelData { value: label })
    }
    pub fn compound(threshold: usize, labels: Vec<LabelData>) -> Query {
        Query::Compound(CompountData { threshold, labels })
    }
    pub fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Query::Compound(q) => q.run(x, retriever),
            Query::Label(q) => q.run(x, retriever),
        }
    }
}
