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

/*
    Work chains are data structures used to postpone and organize work
    that should be done at some point in the future.
    Each work is assigned a unique WorkID and they form blocks.
    Work chains are used by two parties:
        - Work generators.
        - Work consumers.
    The work generator will add work units to the chain, action that may
    trigger the insertion of blocks at the front of the chain.
    The work consumer takes blocks from the back of the chain and reduces
    them  (if applicable) to single units of work.
*/
use serde::{Deserialize, Serialize};
use std::collections::LinkedList;
use std::path::PathBuf;
use std::time::SystemTime;
use uuid::Uuid;

pub type WorkID = Uuid;

/*

*/
#[derive(Serialize, Deserialize)]
pub struct Block {
    pub age: SystemTime,
    pub load: Vec<WorkID>,
}
impl Block {
    pub fn new() -> Block {
        Block {
            age: SystemTime::now(),
            load: vec![],
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Chain {
    current: Block,
    chain: LinkedList<Block>,
}
impl Default for Chain {
    fn default() -> Self {
        Chain::new()
    }
}
impl Chain {
    pub fn new() -> Chain {
        Chain {
            current: Block::new(),
            chain: LinkedList::new(),
        }
    }
}
