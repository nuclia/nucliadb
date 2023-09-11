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

//! `nucliadb_procs` is the procedural macro crate for nucliadb

mod measure;

use measure::MeasureArgs;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemFn};

/// Measure time elapsed during the wrapped function call and export it to the
/// configured `nucliadb_core` `Meter`.
///
/// This macro records a `nucliadb_core` request time metric. To use, apply over
/// a function to record how much it took to execute in the current meter
/// configured in nucliadb_core.
///
/// ATENTION! `nucliadb_core` **must** be available when using this macro.
///
/// # Arguments
///
/// * `actor` - `RequestTimeKey` actor (`RequestActor`) as a literal string in
/// lower case.
///
/// * `metric` - name of the metric to record. This value is part of the metric
/// key and it's used to distinguish operations. For example, `count`,
/// `set_resource`... If not set, defaults to the wrapped function name.
///
/// # Example
///
/// ```rust
/// use nucliadb_procs::measure;
///
/// #[measure(actor = "vectors", metric = "my_function")]
/// fn my_function() {
///     // do something
/// }
/// ```
///
/// this will measure how much `my_function` takes to execute and automatically
/// export `vectors/my_function` metric to the corresponding meter.
#[proc_macro_attribute]
pub fn measure(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let MeasureArgs { actor, metric } = parse_macro_input!(args as MeasureArgs);

    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(input as ItemFn);

    let actor = format_ident!("{}", actor);
    let metric = match metric {
        Some(s) => s,
        None => sig.ident.to_string(),
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let time = std::time::SystemTime::now();

            // execute function body
            let return_value = #block;

            let took = time.elapsed().map(|elapsed| elapsed.as_secs_f64()).unwrap_or(f64::NAN);
            let metrics = nucliadb_core::metrics::get_metrics();
            let metric = nucliadb_core::metrics::request_time::RequestTimeKey::#actor(#metric.to_string());
            metrics.record_request_time(metric, took);

            return_value
        }
    };

    proc_macro::TokenStream::from(expanded)
}

/// Useless proc macro for testing purposes. It literally does nothing to the
/// code
#[proc_macro_attribute]
pub fn do_nothing(
    _args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    item
}
