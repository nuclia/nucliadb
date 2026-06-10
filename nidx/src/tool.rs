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

use std::{fs::File, path::PathBuf};

use crate::{Settings, import_export, settings::EnvSettings};

#[derive(Debug, clap::Subcommand)]
pub enum ToolCommand {
    Import { file: PathBuf },
}

#[derive(Debug, clap::Subcommand)]
pub enum ImportCommand {
    File { file: PathBuf },
    Stdin,
}

pub async fn run_tool(settings: &EnvSettings, cmd: ToolCommand) -> anyhow::Result<()> {
    match cmd {
        ToolCommand::Import { file } => run_import(settings, file).await,
    }
}

async fn run_import(settings: &EnvSettings, file: PathBuf) -> anyhow::Result<()> {
    let settings = Settings::from_env_settings(settings.clone()).await?;

    if file.as_os_str() == "-" {
        import_export::import_shard(
            settings.metadata.clone(),
            settings.storage.as_ref().unwrap().object_store.clone(),
            std::io::stdin(),
        )
        .await?;
    } else {
        import_export::import_shard(
            settings.metadata.clone(),
            settings.storage.as_ref().unwrap().object_store.clone(),
            File::open(file)?,
        )
        .await?;
    };
    println!("Import successful");

    Ok(())
}
