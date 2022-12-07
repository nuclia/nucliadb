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
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=nucliadb_protos/knowledgebox.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/resources.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/noderesources.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/utils.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/writer.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/nodewriter.proto");
    println!("cargo:rerun-if-changed=nucliadb_protos/nodereader.proto");

    let mut prost_config = prost_build::Config::default();

    prost_config
        .out_dir("src/")
        .compile_protos(
            &[
                "nucliadb_protos/utils.proto",
                "nucliadb_protos/knowledgebox.proto",
                "nucliadb_protos/resources.proto",
                "nucliadb_protos/noderesources.proto",
                "nucliadb_protos/writer.proto",
                "nucliadb_protos/nodewriter.proto",
                "nucliadb_protos/nodereader.proto",
            ],
            &["../../"],
        )
        .unwrap();

    tonic_build::configure()
        .build_server(true)
        .out_dir("src") // you can change the generated code's location
        .compile(
            &[
                "nucliadb_protos/nodewriter.proto",
                "nucliadb_protos/nodereader.proto",
            ],
            &["../../"],
        )
        .unwrap();

    Ok(())
}
