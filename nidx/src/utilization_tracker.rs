// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::time::{Duration, Instant};

use tokio::{sync::mpsc::*, time::sleep};

pub struct UtilizationTracker {
    channel: Sender<bool>,
}

impl UtilizationTracker {
    pub fn new(publish: impl Fn(bool, Duration) + Send + 'static) -> Self {
        let (tx, rx) = channel(10);
        tokio::spawn(run(publish, rx));
        Self { channel: tx }
    }

    pub async fn busy(&self) {
        let _ = self.channel.send(true).await;
    }

    pub async fn idle(&self) {
        let _ = self.channel.send(false).await;
    }
}

async fn run(publish: impl Fn(bool, Duration), mut channel: Receiver<bool>) {
    let mut busy = false;
    let mut since = Instant::now();
    loop {
        tokio::select! {
            rxed = channel.recv() => {
                let Some(rxed) = rxed else { break };
                publish(busy, since.elapsed());
                busy = rxed;
                since = Instant::now();
            },
            _ = sleep(Duration::from_secs(30)) => {
                publish(busy, since.elapsed());
                since = Instant::now();
            }
        }
    }
}
