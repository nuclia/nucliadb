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

use std::borrow::Cow;

use prometheus::BasicAuthentication;

use super::{Error, Report};

/// `Credentials`, a tiny wrapper around `prometheus::BasicAuthentication` with cloneable ability.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Credentials {
    username: String,
    password: String,
}

impl From<Credentials> for BasicAuthentication {
    fn from(Credentials { username, password }: Credentials) -> Self {
        BasicAuthentication { username, password }
    }
}

/// `Publisher`, a layer of abstraction over prometheus crate.
pub struct Publisher {
    job: Cow<'static, str>,
    url: Cow<'static, str>,
    credentials: Option<Credentials>,
}

impl Publisher {
    // Creates a new `Publisher` bound to the given Prometheus URL.
    pub fn new(job: impl Into<Cow<'static, str>>, url: impl Into<Cow<'static, str>>) -> Self {
        Self {
            job: job.into(),
            url: url.into(),
            credentials: None,
        }
    }

    /// Adds authority to the `Publisher`.
    pub fn with_credentials(mut self, username: String, password: String) -> Self {
        self.credentials = Some(Credentials { username, password });

        self
    }

    /// Publish any report to the Prometheus.
    pub async fn publish<R: Report>(&self, report: &R) -> Result<(), Error> {
        tokio::task::block_in_place(|| {
            prometheus::push_metrics(
                &self.job,
                report.labels(),
                &self.url,
                report.metrics(),
                self.credentials.clone().map(Into::into),
            )
        })?;

        Ok(())
    }
}
