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

use nucliadb_procs::{do_nothing, measure};

#[test]
fn test_measure_simple_function() {
    #[measure(actor = "vectors", metric = "add_to")]
    #[do_nothing]
    fn add_two(value: usize) -> usize {
        #[do_nothing]
        fn a() {}
        a();
        value + 2
    }

    assert_eq!(add_two(2), 4);
}

#[test]
fn test_metric_not_defined() {
    #[measure(actor = "vectors")]
    #[do_nothing]
    fn add_two(value: usize) -> usize {
        #[do_nothing]
        fn a() {}
        a();
        value + 2
    }

    assert_eq!(add_two(2), 4);
}
