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

use std::collections::HashSet;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct HNSWParams {
    pub no_layers: usize,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub k_neighbours: usize,
}
impl Default for HNSWParams {
    fn default() -> Self {
        HNSWParams {
            no_layers: 4,
            m_max: 16,
            m: 16,
            ef_construction: 100,
            k_neighbours: 10,
        }
    }
}

pub trait Distance {
    fn cosine(i: &Self, j: &Self) -> f32;
}

#[derive(
    Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct NodeId(u128);
impl NodeId {
    pub fn new() -> NodeId {
        NodeId(0)
    }
    pub fn null() -> NodeId {
        NodeId(u128::MAX)
    }
    pub fn fresh(&mut self) -> NodeId {
        let prev = *self;
        self.0 += 1;
        prev
    }
}

#[derive(
    Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct EdgeId(usize);
impl EdgeId {
    pub fn new() -> EdgeId {
        EdgeId(0)
    }
    pub fn fresh(&mut self) -> EdgeId {
        let prev = *self;
        self.0 += 1;
        prev
    }
}

#[derive(
    Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct LabelId(usize);
impl LabelId {
    pub fn new() -> LabelId {
        LabelId(0)
    }
    pub fn fresh(&mut self) -> LabelId {
        let prev = *self;
        self.0 += 1;
        prev
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub key: String,
    pub vector: GraphVector,
    pub labels: HashSet<LabelId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge {
    pub dist: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Label {
    pub my_id: LabelId,
    pub value: String,
    pub reached_by: usize,
}
impl Label {
    pub fn new(my_id: LabelId, value: String) -> Label {
        Label {
            my_id,
            value,
            reached_by: 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphVector {
    value: Vec<f32>,
    power_sqrt: f32,
}
impl GraphVector {
    pub fn new(value: Vec<f32>, power_sqrt: f32) -> GraphVector {
        GraphVector { value, power_sqrt }
    }
    pub fn value(&self) -> &Vec<f32> {
        &self.value
    }
}

impl From<Vec<f32>> for GraphVector {
    fn from(x: Vec<f32>) -> Self {
        let power = x.iter().cloned().fold(0.0, |p, c| p + (c * c));
        GraphVector::new(x, f32::sqrt(power))
    }
}
impl From<GraphVector> for Vec<f32> {
    fn from(GraphVector { value, .. }: GraphVector) -> Self {
        value
    }
}
impl Distance for GraphVector {
    fn cosine(i: &Self, j: &Self) -> f32 {
        let x = &i.value;
        let y = &j.value;
        let dot_ij: f32 = x
            .par_iter()
            .enumerate()
            .map(|(index, v)| y[index] * (*v))
            .sum();

        dot_ij / (i.power_sqrt * j.power_sqrt)
    }
}
