# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
import inspect
import typing
from typing import Any, Callable, List, Optional, Tuple, Type, Union

import httpx
from pydantic import BaseModel


class Docstring(BaseModel):
    doc: str
    examples: Optional[str] = None


SEARCH = Docstring(
    doc="""Search in your Knowledge Box""",
    examples="""
Advanced search on the full text index
>>> resp = sdk.search(kbid="mykbid", advanced_query="text:SRE OR text:DevOps", features=["document"])
>>> rid = resp.fulltext.results[0].rid
>>> resp.resources[rid].title
The Site Reliability Workbook.pdf
""",
)

FIND = Docstring(
    doc="""Find documents in your Knowledge Box""",
    examples="""
Find documents matching a query
>>> resp = sdk.find(kbid="mykbid", query="Very experienced candidates with Rust experience")
>>> resp.resources.popitem().title
Graydon_Hoare.cv.pdf

Filter down by country and increase accuracy of results
>>> content = FindRequest(query="Very experienced candidates with Rust experience", filters=["/l/country/Spain"], min_score=2.5)
>>> resp = sdk.find(kbid="mykbid", content=content)
>>> resp.resources.popitem().title
http://github.com/hermeGarcia
""",  # noqa
)

CHAT = Docstring(
    doc="""Chat with your Knowledge Box""",
    examples="""
Get an answer for a question that is part of the data in the Knowledge Box
>>> resp = sdk.chat(kbid="mykbid", query="Will France be in recession in 2023?")
>>> print(resp.answer)
Yes, according to the provided context, France is expected to be in recession in 2023.

You can use the `content` parameter to pass a `ChatRequest` object
>>> content = ChatRequest(query="Who won the 2018 football World Cup?")
>>> resp = sdk.chat(kbid="mykbid", content=content)
>>> print(resp.answer)
France won the 2018 football World Cup.
""",
)


def inject_documentation(
    func,
    name: str,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
    docstring: Optional[Docstring] = None,
):
    func.__name__ = name
    _inject_signature(func, path_params, request_type, response_type)
    _inject_docstring(func, method, path_template, path_params, request_type, docstring)


def _inject_signature(
    func,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
):
    # TODO: Fix signature for methods of NucliaDBAsync
    parameters = []
    # The first parameter is always self
    parameters.append(
        inspect.Parameter(
            "self", kind=inspect.Parameter.POSITIONAL_ONLY, annotation="NucliaSDK"
        )
    )

    # Path params
    for path_param in path_params:
        parameters.append(
            inspect.Parameter(
                path_param, kind=inspect.Parameter.KEYWORD_ONLY, annotation=str
            )
        )

    # Body params
    if request_type is not None:
        if isinstance(request_type, type) and issubclass(request_type, BaseModel):
            for field in request_type.__fields__.values():
                parameters.append(
                    inspect.Parameter(
                        field.name,
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        annotation=field.outer_type_,
                        default=field.default,
                    )
                )
        parameters.append(
            inspect.Parameter(
                "content",
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=request_type,
                default=None,
            )
        )
    # Response type
    if inspect.isroutine(response_type):
        return_annotation = typing.get_type_hints(response_type).get("return")
    else:
        return_annotation = response_type

    func.__signature__ = inspect.Signature(
        parameters=parameters, return_annotation=return_annotation
    )


def _inject_docstring(
    func,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    docstring: Optional[Docstring] = None,
):
    # Initial description section
    func_doc = f"Wrapper around the api endpoint {method.upper()} {path_template}\n\n"
    if docstring:
        func_doc += docstring.doc + "\n\n"

    # Add params section
    params = []
    for path_param in path_params:
        params.append(f":param <class 'str'> {path_param}:")
    if request_type is not None:
        if isinstance(request_type, type) and issubclass(request_type, BaseModel):
            for field in request_type.__fields__.values():
                try:
                    type_name = field.outer_type_.__name__
                except AttributeError:
                    type_name = typing.get_origin(field.outer_type_).__name__
                type_name = field.outer_type_
                params.append(
                    f":param {type_name} {field.name}: {field.field_info.description or ''}"
                )
    func_doc += "\n".join(params) + "\n\n"

    # Examples section
    if docstring and docstring.examples:
        func_doc += "Example usage:\n" + docstring.examples
    func.__doc__ = func_doc
