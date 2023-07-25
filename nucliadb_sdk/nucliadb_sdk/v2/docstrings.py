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
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import httpx
from pydantic import BaseModel


class Example(BaseModel):
    description: str
    code: str


class Docstring(BaseModel):
    doc: str
    examples: List[Example] = []
    path_param_doc: Dict[str, str] = {}


CREATE_RESOURCE = Docstring(
    doc="""Create a resource in your Knowledge Box""",
    examples=[
        Example(
            description="You can create a resource with a single text field",
            code=""">>> from nucliadb_sdk import *
>>> from nucliadb_models.text import TextField
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> text = TextField(body="This is the content of my file")
>>> resp = sdk.create_resource(kbid="mykbid", texts={"text1": text})
""",
        ),
        Example(
            description="Other formats like json, rst, html or markdown are also supported",
            code=""">>> from nucliadb_sdk import *
>>> from nucliadb_models.text import TextField, TextFormat
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> text = TextField(format=TextFormat.JSON, body="{'this_is_a': 'json_document'}")
>>> resp = sdk.create_resource(kbid="mykbid", title="Myfile.json", slug="my-resource" texts={"text1": text})
""",
        ),
    ],
)

UPDATE_RESOURCE = Docstring(
    doc="""Update a Resource from your Knowledge Box""",
    examples=[
        Example(
            description="You can update any of the resource attributes with this method",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resp = sdk.update_resoure(kbid="mykbid", rid="cf54a55", title="My new title")
""",
        ),
    ],
)

GET_RESOURCE_BY_ID = Docstring(
    doc="""Get Resource by id""",
    examples=[
        Example(
            description="Endpoint query parameters can be specified as follows:",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resource = sdk.get_resource_by_id(kbid="mykbid", rid="cf54a55", query_params={"show": ["values", "basic"]})
>>> resource.title
'Highlights - Stage 2 - Tour de France 2023'
""",
        ),
    ],
)

GET_RESOURCE_BY_SLUG = Docstring(
    doc="""Get Resource by slug""",
    examples=[
        Example(
            description="Get the resource by its slug, which you can set upon creation",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resource = sdk.get_resource_by_slug(kbid="mykbid", rslug="letourstage2")
>>> resource.title
'Highlights - Stage 2 - Tour de France 2023'
""",
        ),
    ],
)

LIST_RESOURCES = Docstring(
    doc="""List the resources in your Knowledge Box""",
    examples=[
        Example(
            description="Paginate through all the resources in your Knowledge Box",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> all_resources = []
>>> page = 0
>>> while True:
...    result = sdk.list_resources(kbid="mykbid", query_params={"page": page})
...    if result.pagination.last:
...        break
...    all_resources.extend(result.resources)
...    page += 1
>>> len(all_resources)
45
""",
        ),
    ],
)


SEARCH = Docstring(
    doc="""Search in your Knowledge Box""",
    examples=[
        Example(
            description="Search on the full text index",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resp = sdk.search(kbid="mykbid", query="Site Reliability", features=["document"])
>>> rid = resp.fulltext.results[0].rid
>>> resp.resources[rid].title
'The Site Reliability Workbook.pdf'
""",
        )
    ],
)

FIND = Docstring(
    doc="""Find documents in your Knowledge Box""",
    examples=[
        Example(
            description="Find documents matching a query",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resp = sdk.find(kbid="mykbid", query="Very experienced candidates with Rust experience")
>>> resp.resources.popitem().title
'Graydon_Hoare.cv.pdf'
""",
        ),
        Example(
            description="Filter down by country and increase accuracy of results",
            code=""">>> content = FindRequest(query="Very experienced candidates with Rust experience", filters=["/l/country/Spain"], min_score=2.5)
>>> resp = sdk.find(kbid="mykbid", content=content)
>>> resp.resources.popitem().title
'http://github.com/hermeGarcia'
""",  # noqa
        ),
    ],
)

CHAT = Docstring(
    doc="""Chat with your Knowledge Box""",
    examples=[
        Example(
            description="Get an answer for a question that is part of the data in the Knowledge Box",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> sdk.chat(kbid="mykbid", query="Will France be in recession in 2023?").answer
'Yes, according to the provided context, France is expected to be in recession in 2023.'
""",
        ),
        Example(
            description="You can use the `content` parameter to pass a `ChatRequest` object",
            code=""">>> content = ChatRequest(query="Who won the 2018 football World Cup?")
>>> sdk.chat(kbid="mykbid", content=content).answer
'France won the 2018 football World Cup.'
""",
        ),
    ],
)

RESOURCE_CHAT = Docstring(
    doc="""Chat with your document""",
    examples=[
        Example(
            description="Have a chat with your document. Generated answers are scoped to the context of the document.",
            code=""">>> sdk.chat_on_resource(kbid="mykbid", query="What is the coldest season in Sevilla?").answer
'January is the coldest month.'
""",
        ),
        Example(
            description="You can use the `content` parameter to pass previous context to the query",
            code=""">>> from nucliadb_models.search import ChatRequest, Message
>>> content = ChatRequest()
>>> content.query = "What is the average temperature?"
>>> content.context.append(Messate(author="USER", text="What is the coldest season in Sevilla?"))
>>> content.context.append(Messate(author="NUCLIA", text="January is the coldest month."))
>>> sdk.chat(kbid="mykbid", content=content).answer
'According to the context, the average temperature in January in Sevilla is 15.9 °C and 5.2 °C.'
""",
        ),
    ],
)


DELETE_LABELSET = Docstring(
    doc="Delete a specific set of labels",
    path_param_doc={"labelset": "Id of the labelset to delete"},
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
    _inject_signature_and_annotations(func, path_params, request_type, response_type)
    _inject_docstring(
        func, method, path_template, path_params, request_type, response_type, docstring
    )


def _inject_signature_and_annotations(
    func,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
) -> None:
    """Dynamically generate and inject the function signature and its annotations"""
    parameters = []
    annotations = {}

    # The first parameter is always self
    parameters.append(
        inspect.Parameter("self", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
    )

    # Path params
    for path_param in path_params:
        parameters.append(
            inspect.Parameter(
                path_param, kind=inspect.Parameter.KEYWORD_ONLY, annotation=str
            )
        )
        annotations[path_param] = str  # type: ignore

    # Body params
    if request_type is not None:
        if isinstance(request_type, type) and issubclass(request_type, BaseModel):
            for field in request_type.__fields__.values():
                parameters.append(
                    inspect.Parameter(
                        field.name,
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        annotation=field.annotation,
                        default=field.default,
                    )
                )
                annotations[field.name] = field.annotation  # type: ignore
        parameters.append(
            inspect.Parameter(
                "content",
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=request_type,
                default=None,
            )
        )
        annotations["content"] = request_type  # type: ignore

    # Response type
    if inspect.isroutine(response_type):
        return_annotation = typing.get_type_hints(response_type).get("return")
    else:
        return_annotation = response_type
    annotations["return"] = return_annotation  # type: ignore

    func.__signature__ = inspect.Signature(
        parameters=parameters, return_annotation=return_annotation
    )
    func.__annotations__.update(annotations)


def _inject_docstring(
    func,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[Type[BaseModel], Callable[[httpx.Response], BaseModel]]
    ],
    docstring: Optional[Docstring] = None,
):
    """
    Dynamically generate the docstring of the sdk method based on the ._request_build parameters.
    """
    # Initial description section
    func_doc = f"Wrapper around the api endpoint {method.upper()} {path_template}\n\n"
    if docstring:
        func_doc += docstring.doc + "\n\n"

    # Add params section
    params = []
    for path_param in path_params:
        description = ""
        if path_param == "kbid":
            description = "The id of the knowledge box"
        elif path_param == "rid":
            description = "The id of the resource"
        elif path_param == "field_id":
            description = "The id of the field"
        elif docstring and path_param in docstring.path_param_doc:
            description = docstring.path_param_doc[path_param]
        params.append(f":param {path_param}: {description}")
        params.append(f":type {path_param}: <class 'str'>")
    if request_type is not None:
        if isinstance(request_type, type) and issubclass(request_type, BaseModel):
            for field in request_type.__fields__.values():
                params.append(
                    f":param {field.name}: {field.field_info.description or ''}"
                )
                params.append(f":type {field.name}: {field.outer_type_}")
            params.append(f":param content: the request content model")
            params.append(f":type content: {request_type}")
        elif typing.get_origin(request_type) == list:
            model = typing.get_args(request_type)[0]
            params.append(f":param content: the request content model")
            params.append(f":type content: <class '{model}'>")

    if response_type is not None:
        if inspect.isroutine(response_type):
            return_annotation = typing.get_type_hints(response_type).get("return")
            return_doc = (
                f"Parsed response for the {method.upper()} {path_template} endpoint"
            )
        else:
            return_annotation = response_type
            if response_type.__doc__:
                return_doc = response_type.__doc__.strip("\n").strip()
            else:
                return_doc = f"The response model for the {method.upper()} {path_template} endpoint"
        params.append(f":return: {return_doc}")
        params.append(f":rtype: {return_annotation}")

    func_doc += "\n".join(params)
    func_doc += "\n"
    # Add examples section
    if docstring:
        for example in docstring.examples or []:
            description = example.description or ""
            code = example.code or ""
            func_doc += f"\n{description}\n\n{code}\n"
    func.__doc__ = func_doc
