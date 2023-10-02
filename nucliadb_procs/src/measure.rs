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

use syn::parse::{Parse, ParseStream};
use syn::{LitStr, Token};

/// `measure` macro arguments
pub struct MeasureArgs {
    pub actor: String,
    pub metric: Option<String>,
}

/// Custom syn keywords for argument parsing
mod kw {
    syn::custom_keyword!(actor);
    syn::custom_keyword!(metric);
}

impl Parse for MeasureArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut actor = None;
        let mut metric = None;
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(kw::actor) {
                if actor.is_some() {
                    return Err(input.error("Argument `actor` is expected only once"));
                }
                let _ = input.parse::<kw::actor>()?;
                let _ = input.parse::<Token![=]>()?;
                let value = input.parse::<LitStr>()?.value().to_string();
                // parse and ignore comma if found
                let _ = input.parse::<Token![,]>();
                actor = Some(value)
            } else if lookahead.peek(kw::metric) {
                if metric.is_some() {
                    return Err(input.error("Argument `metric` is expected only once"));
                }
                let _ = input.parse::<kw::metric>()?;
                let _ = input.parse::<Token![=]>()?;
                let value = input.parse::<LitStr>()?.value().to_string();
                // parse and ignore comma if found
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_compilation_errors() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/dont_compile/*.rs")
    }
}
