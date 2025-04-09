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
