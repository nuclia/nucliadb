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
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;

#[derive(Serialize)]
struct Traces {
    trace: String,
}

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use std::env;
#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use std::process::Command;

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use rstack_self;

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
pub async fn thread_dump_service() -> Response {
    let exe = env::current_exe().unwrap();
    let trace = rstack_self::trace(Command::new(exe).arg("child"));

    match trace {
        Ok(res) => Json(Traces {
            trace: format!("{:#?}", res),
        })
        .into_response(),
        Err(err) => Json(Traces {
            trace: format!("Could not retrieve thread dump {:?}", err),
        })
        .into_response(),
    }
}

#[cfg(not(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl"))))]
pub async fn thread_dump_service() -> Response {
    Json(Traces {
        trace: String::from("Not supported on this platform"),
    })
    .into_response()
}
