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

use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=nidx.proto");
    println!("cargo:rerun-if-changed=nodereader.proto");
    println!("cargo:rerun-if-changed=noderesources.proto");
    println!("cargo:rerun-if-changed=nodewriter.proto");
    println!("cargo:rerun-if-changed=../../nucliadb_protos");
    println!("cargo:rerun-if-changed=src");
    tonic_prost_build::configure()
        .emit_rerun_if_changed(false)
        .compile_protos(
            &["nidx_protos/nidx.proto", "../../nucliadb_protos/kb_usage.proto"],
            &["../../", ".."],
        )?;
    Ok(())
}
