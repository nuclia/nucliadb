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
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union

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

UPDATE_RESOURCE_BY_SLUG = Docstring(
    doc="""Update a Resource from your Knowledge Box by Slug""",
    examples=[
        Example(
            description="You can update any of the resource attributes with this method",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> resp = sdk.update_resoure_by_slug(kbid="mykbid", rslug="data_log_56", title="My new title")
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


REINDEX_RESOURCE = Docstring(
    doc="""Reindex resource""",
    examples=[
        Example(
            description="Reindex resource",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> sdk.reindex_resource(kbid="mykbid", rid="rid")
""",
        ),
    ],
)


REINDEX_RESOURCE_BY_SLUG = Docstring(
    doc="""Reindex resource by slug""",
    examples=[
        Example(
            description="Reindex resource by slug instead of id",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> sdk.reindex_resource_by_slug(kbid="mykbid", slug="slug")
""",
        ),
    ],
)

REPROCESS_RESOURCE = Docstring(
    doc="""Reprocess resource""",
    examples=[
        Example(
            description="Send resource to processing again.",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> sdk.reprocess_resource(kbid="mykbid", rid="rid")
""",
        ),
    ],
)


REPROCESS_RESOURCE_BY_SLUG = Docstring(
    doc="""Reindex resource by slug""",
    examples=[
        Example(
            description="Send resource to processing again by slug.",
            code=""">>> from nucliadb_sdk import *
>>> sdk = NucliaDBSDK(api_key="api-key")
>>> sdk.reprocess_resource_by_slug(kbid="mykbid", slug="slug")
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
            code=""">>> from nucliadb_models.search import ChatRequest, ChatContextMessage
>>> content = ChatRequest()
>>> content.query = "What is the average temperature?"
>>> content.context.append(ChatContextMessage(author="USER", text="What is the coldest season in Sevilla?"))
>>> content.context.append(ChatContextMessage(author="NUCLIA", text="January is the coldest month."))
>>> sdk.chat(kbid="mykbid", content=content).answer
'According to the context, the average temperature in January in Sevilla is 15.9 °C and 5.2 °C.'
""",
        ),
    ],
)

SUMMARIZE = Docstring(
    doc="""Summarize your documents""",
    examples=[
        Example(
            description="Get a summary of a document or a list of documents",
            code=""">>> summary = sdk.summarize(kbid="mykbid", resources=["uuid1"]).summary
>>> print(summary)
'The document talks about Seville and its temperature. It also mentions the coldest month of the year, which is January.'
""",  # noqa: E501
        ),
    ],
)


DELETE_LABELSET = Docstring(
    doc="Delete a specific set of labels",
    path_param_doc={"labelset": "Id of the labelset to delete"},
)

START_EXPORT = Docstring(
    doc="Start a new export for a Knowledge Box",
    examples=[
        Example(
            description="Start an export on a KB",
            code=""">>> from nucliadb_sdk import *
export_id = sdk.start_export(kbid="mykbid").export_id
print(export_id)
'925796f7-3820-44c9-b306-6e69148d02c3'
""",
        )
    ],
)

EXPORT_STATUS = Docstring(
    doc="Check the status of an export",
    path_param_doc={"export_id": "Id of the export to check"},
    examples=[
        Example(
            description="Check the status of an export",
            code=""">>> from nucliadb_sdk import *
resp = sdk.export_status(kbid="mykbid", export_id=export_id)
print(resp.status.value)
'finished'
""",
        )
    ],
)

DOWNLOAD_EXPORT = Docstring(
    doc="Download the export of the Knowledge Box",
    path_param_doc={"export_id": "Id of the export to check"},
    examples=[
        Example(
            description="Download the export content once it has finished",
            code=""">>> from nucliadb_sdk import *
resp = sdk.export_status(kbid="mykbid", export_id=export_id)
assert resp.status.value == 'finished'
resp_bytes_gen = sdk.download_export(kbid="mykbid", export_id=export_id)
with open("my-kb.export", "wb") as f:
    for chunk in resp_bytes_gen(chunk_size=1024):
        f.write(chunk)
""",
        )
    ],
)

START_IMPORT = Docstring(
    doc="Start a new import for a Knowledge Box",
    examples=[
        Example(
            description="Import to a KB from an export file",
            code=""">>> from nucliadb_sdk import *
resp =  sdk.start_import(kbid="mykbid", content=open("my-kb.export", "rb"))
import_id = resp.import_id
print(import_id)
'b17337ca-5b1b-431b-b9b7-5ecb2241c78a'
""",
        )
    ],
)

IMPORT_STATUS = Docstring(
    doc="Check the status of an import",
    path_param_doc={"import_id": "Id of the import to check"},
    examples=[
        Example(
            description="Check the status of an import",
            code=""">>> from nucliadb_sdk import *
resp = sdk.import_status(kbid="mykbid", import_id=import_id)
print(resp.status.value)
'finished'
""",
        )
    ],
)

TRAINSET_PARTITIONS = Docstring(
    doc="Check partitions available to download",
    examples=[
        Example(
            description="Check partitions",
            code=""">>> from nucliadb_sdk import *
resp = sdk.trainset(kbid="mykbid")
print(resp.status.value)
'9481939a99sd9a99asda'
""",
        )
    ],
)


def inject_documentation(
    func,
    name: str,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[
            Type[BaseModel],
            Callable[[httpx.Response], BaseModel],
            Callable[[httpx.Response], Iterator[bytes]],
        ]
    ],
    docstring: Optional[Docstring] = None,
):
    func.__name__ = name
    _inject_signature_and_annotations(func, path_params, request_type, response_type)
    _inject_docstring(func, method, path_template, path_params, request_type, response_type, docstring)


def _inject_signature_and_annotations(
    func,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[
            Type[BaseModel],
            Callable[[httpx.Response], BaseModel],
            Callable[[httpx.Response], Iterator[bytes]],
        ]
    ],
) -> None:
    """Dynamically generate and inject the function signature and its annotations"""
    parameters = []
    annotations = {}

    # The first parameter is always self
    parameters.append(inspect.Parameter("self", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD))

    # Path params
    for path_param in path_params:
        parameters.append(
            inspect.Parameter(path_param, kind=inspect.Parameter.KEYWORD_ONLY, annotation=str)
        )
        annotations[path_param] = str

    # Body params
    if request_type is not None:
        if isinstance(request_type, type) and issubclass(request_type, BaseModel):
            for field_name, field in request_type.model_fields.items():
                parameters.append(
                    inspect.Parameter(
                        field_name,
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        annotation=field.annotation,
                        default=field.default,
                    )
                )
                annotations[field_name] = field.annotation  # type: ignore
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

    func.__signature__ = inspect.Signature(parameters=parameters, return_annotation=return_annotation)
    func.__annotations__.update(annotations)


def _inject_docstring(
    func,
    method: str,
    path_template: str,
    path_params: Tuple[str, ...],
    request_type: Optional[Union[Type[BaseModel], List[Any]]],
    response_type: Optional[
        Union[
            Type[BaseModel],
            Callable[[httpx.Response], BaseModel],
            Callable[[httpx.Response], Iterator[bytes]],
        ]
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
            for field_name, field in request_type.model_fields.items():
                params.append(f":param {field_name}: {field.description or ''}")
                params.append(f":type {field_name}: {field.annotation}")
            params.append(f":param content: the request content model")
            params.append(f":type content: {request_type}")
        elif typing.get_origin(request_type) == list:
            model = typing.get_args(request_type)[0]
            params.append(f":param content: the request content model")
            params.append(f":type content: <class '{model}'>")

    if response_type is not None:
        if inspect.isroutine(response_type):
            return_annotation = typing.get_type_hints(response_type).get("return")
            return_doc = f"Parsed response for the {method.upper()} {path_template} endpoint"
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
