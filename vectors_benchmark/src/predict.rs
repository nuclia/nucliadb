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
use reqwest::blocking::Client;
use std::env;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PredictResults {
    pub data: Vec<f32>,
}

/// Calls the predict service to convert the query as a vector set
pub fn get_vectorset(query: &str, model: &str) -> PredictResults {
    let client = Client::new();

    let nua_key = env::var("NUA_KEY").unwrap_or_else(|_| {
        panic!("You need to set your NUA_KEY environment variable to call the predict service");
    });

    let response = client
        .get("https://europe-1.stashify.cloud/api/v1/predict/sentence")
        .query(&[("text", query), ("model", model)])
        .header("X-STF-NUAKEY", format!("Bearer {nua_key}"))
        .send()
        .unwrap();

    if response.status().as_u16() > 299 {
        panic!("[predict] Got a {} response from nua", response.status());
    }

    serde_json::from_str(response.text().unwrap().as_str()).unwrap()
}
