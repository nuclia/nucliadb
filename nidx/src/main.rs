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

use clap::Parser;
use nidx::{
    Settings, api,
    control::{ControlRequest, ControlServer, control_client},
    indexer, metrics, scheduler, searcher,
    settings::{EnvSettings, IndexerSettings},
    telemetry,
    tool::{ToolCommand, run_tool},
    worker,
};
use prometheus_client::registry::Registry;
use sentry::IntoDsn;
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpListener, signal, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::*;

#[derive(Debug, clap::Parser)]
struct CommandLine {
    #[command(subcommand)]
    cmd: Option<Ctl>,
    components: Vec<Component>,
}

#[derive(Debug, clap::Subcommand)]
enum Ctl {
    #[command(subcommand)]
    Ctl(ControlRequest),
    #[command(subcommand)]
    Tool(ToolCommand),
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Component {
    Indexer,
    Worker,
    Scheduler,
    Searcher,
    Api,
}

// Main wrapper needed to initialize Sentry before Tokio
fn main() -> anyhow::Result<()> {
    let env_settings = EnvSettings::from_env();
    let cmdline = CommandLine::parse();

    if let Some(Ctl::Ctl(cmd_request)) = cmdline.cmd {
        return control_client(&env_settings, cmd_request);
    }

    if let Some(Ctl::Tool(tool_request)) = cmdline.cmd {
        return tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_tool(&env_settings, tool_request));
    }

    let sentry_options = if let Some(sentry) = &env_settings.telemetry.sentry {
        sentry::ClientOptions {
            dsn: sentry.dsn.clone().into_dsn()?,
            environment: Some(sentry.environment.clone().into()),
            release: sentry::release_name!(),
            ..Default::default()
        }
    } else {
        // No Sentry config provided, fallback to printing traces to logs
        sentry::ClientOptions {
            dsn: "http://disabled:disabled@disabled/disabled".into_dsn()?,
            before_send: Some(Arc::new(|event| {
                trace!("Sentry error {event:#?}");
                None
            })),
            ..Default::default()
        }
    };
    let _guard = sentry::init(sentry_options);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(do_main(env_settings, cmdline.components))
}

async fn do_main(env_settings: EnvSettings, components: Vec<Component>) -> anyhow::Result<()> {
    telemetry::init(&env_settings.telemetry)?;

    let settings = Settings::from_env_settings(env_settings).await?;
    let mut metrics = Registry::with_prefix("nidx");
    metrics::common::register(&mut metrics);

    let shutdown = CancellationToken::new();

    let needs_nats = components
        .iter()
        .any(|c| matches!(c, Component::Indexer | Component::Scheduler));
    let nats_client = if needs_nats {
        if let Some(IndexerSettings {
            nats_server: Some(nats_server),
            ..
        }) = settings.indexer.as_ref()
        {
            Some(async_nats::connect(&nats_server).await?)
        } else {
            None
        }
    } else {
        None
    };

    let mut task_ids = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut has_searcher = false;
    components.iter().for_each(|component| {
        let task = match component {
            Component::Indexer => {
                metrics::indexer::register(&mut metrics);
                tasks.spawn(indexer::run(settings.clone(), shutdown.clone(), nats_client.clone()))
            }
            Component::Worker => {
                metrics::worker::register(&mut metrics);
                tasks.spawn(worker::run(settings.clone(), shutdown.clone()))
            }
            Component::Scheduler => {
                metrics::scheduler::register(&mut metrics);
                tasks.spawn(scheduler::run(settings.clone(), shutdown.clone(), nats_client.clone()))
            }
            Component::Searcher => {
                has_searcher = true;
                metrics::searcher::register(&mut metrics);
                metrics::searcher::SYNC_DELAY.set(100_000.0); // Initialize to something large since we don't know yet, but it's not 0
                tasks.spawn(searcher::run(settings.clone(), shutdown.clone()))
            }
            Component::Api => tasks.spawn(api::run(settings.clone(), shutdown.clone())),
        };
        task_ids.insert(task.id(), component);
    });

    // Purposely not tracked, we want metrics until the process finally dies
    tokio::spawn(metrics_server(metrics));
    if let Some(control_socket) = &settings.control_socket {
        let control_socket = control_socket.clone();
        let mut control = ControlServer::new(settings.metadata.clone(), has_searcher, nats_client);
        tokio::spawn(async move { control.run(&control_socket).await });
    }

    let termination_listener = async {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
        let mut sigquit = signal::unix::signal(signal::unix::SignalKind::quit()).unwrap();

        tokio::select! {
            _ = sigterm.recv() => info!("Terminating on SIGTERM"),
            _ = sigquit.recv() => info!("Terminating on SIGQUIT"),
            _ = signal::ctrl_c() => info!("Terminating on ctrl-c"),
        }
    };

    // Wait for termination signal or any task to die
    tokio::select! {
        Some(join_result) = tasks.join_next_with_id() => {
            let (id, result) = join_result.unwrap();
            let task_name = task_ids.get(&id).map(|x| format!("{x:?}")).unwrap_or("<unknown>".to_string());
            if let Err(e) = result {
                panic!("Task {task_name} finished with error, stopping: {e:?}");
            } else {
                panic!("Task {task_name} unexpectedly exited without error");
            }
        }
        _ = termination_listener => {}
    }

    // Initiate shutdown
    info!("Initiating graceful shutdown");
    shutdown.cancel();

    while let Some(join_result) = tasks.join_next_with_id().await {
        let (id, task_result) = join_result.unwrap();
        let task_name = task_ids
            .get(&id)
            .map(|x| format!("{x:?}"))
            .unwrap_or("<unknown>".to_string());
        if let Err(e) = task_result {
            panic!("Task {task_name} finished with error, stopping: {e:?}");
        } else {
            debug!("Task {task_name} exited without error");
        }
    }

    info!("Successfully shutted down");

    Ok(())
}

async fn metrics_server(metrics: Registry) -> anyhow::Result<()> {
    let metrics = Arc::new(metrics);
    let router = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(|| async move {
                let mut buffer = String::new();
                if prometheus_client::encoding::text::encode(&mut buffer, &metrics).is_err() {
                    Err("Error encoding metrics")
                } else {
                    Ok(buffer)
                }
            }),
        )
        .into_make_service();

    axum::serve(TcpListener::bind("0.0.0.0:10010").await?, router).await?;
    Ok(())
}
