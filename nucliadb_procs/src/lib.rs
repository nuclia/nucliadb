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

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, ItemFn, LitStr, Token};

/// Measure time elapsed during the wrapped function call.
///
/// It records a `nucliadb_core` request time metric, so `nucliadb_core` *must*
/// be available when using this macro.
#[proc_macro_attribute]
pub fn measure(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    println!("Args: {}", args);
    println!("Item: {}", item);

    let MeasureArgs { actor, metric } = parse_macro_input!(args as MeasureArgs);

    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(item as ItemFn);

    let actor = format_ident!("{}", actor);
    let metric = match metric {
        Some(s) => s,
        None => sig.ident.to_string(),
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            use std::time::SystemTime;
            use nucliadb_core::metrics;
            use nucliadb_core::metrics::request_time::RequestTimeKey;

            let time = SystemTime::now();

            // execute function body
            let return_value = #block;

            let took = time.elapsed().map(|elapsed| elapsed.as_secs_f64()).unwrap_or(f64::NAN);
            let metrics = metrics::get_metrics();
            let metric = RequestTimeKey::#actor(#metric.to_string());
            metrics.record_request_time(metric, took);

            return_value
        }
    };

    println!("Expanded: {}", expanded);

    TokenStream::from(expanded)
}

mod kw {
    syn::custom_keyword!(actor);
    syn::custom_keyword!(metric);
}

struct MeasureArgs {
    actor: String,
    metric: Option<String>,
}

impl Parse for MeasureArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut actor = None;
        let mut metric = None;
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            println!("Parsing input: {:?}", input);
            println!("Actor: {:?} and Metric: {:?}", actor, metric);
            if lookahead.peek(kw::actor) {
                if actor.is_some() {
                    return Err(input.error("Argument `actor` is expected only once"));
                }
                let _ = input.parse::<kw::actor>()?;
                let _ = input.parse::<Token![=]>()?;
                let value = input.parse::<LitStr>()?.value().to_string();
                // ignore comma if found
                let _ = input.parse::<Token![,]>();
                actor = Some(value)
            } else if lookahead.peek(kw::metric) {
                if metric.is_some() {
                    return Err(input.error("Argument `metric` is expected only once"));
                }
                let _ = input.parse::<kw::metric>()?;
                let _ = input.parse::<Token![=]>()?;
                let value = input.parse::<LitStr>()?.value().to_string();
                // ignore comma if found
                let _ = input.parse::<Token![,]>();
                metric = Some(value)
            } else {
                return Err(lookahead.error());
            }
        }

        if actor.is_none() {
            return Err(input.error("Argument `actor` must be defined"));
        }

        Ok(MeasureArgs {
            actor: actor.unwrap(),
            metric,
        })
    }
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
