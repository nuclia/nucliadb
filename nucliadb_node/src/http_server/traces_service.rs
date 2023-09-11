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
use std::process::Command;
use std::{env, process};

use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;

#[derive(Serialize)]
struct Traces {
    trace: String,
}

pub async fn thread_dump_service() -> Response {
    let pid = process::id();
    let mut node_trace = env::current_exe().unwrap();
    node_trace.pop();
    node_trace.push("rust-spy");
    let mut cmd = Command::new(node_trace);
    let args = [format!("{}", pid)];

    cmd.args(&args);

    let output = match cmd.output() {
        Ok(output) => output,
        Err(e) => {
            return Json(Traces {
                trace: format!("Failed to execute {:?} command: {}", cmd, e),
            })
            .into_response();
        }
    };

    let result = if output.status.success() {
        // Convert the stdout bytes into a String
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        // Print the error message
        format!("Command failed with error: {:?}", output.status)
    };

    Json(Traces { trace: result }).into_response()
}
