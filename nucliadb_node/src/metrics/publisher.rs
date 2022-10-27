use std::borrow::Cow;

use prometheus::BasicAuthentication;

use super::{Report, Error};

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
    pub fn publish<R: Report>(&self, report: &R) -> Result<(), Error> {
        prometheus::push_metrics(
            &self.job,
            report.labels(),
            &self.url,
            report.metrics(),
            self.credentials.clone().map(Into::into),
        )?;

        Ok(())
    }
}
