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
#

from google.protobuf.json_format import ParseDict
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import get_storage
from tests.ndbfixtures.resources._vectors import (
    adjust_kb_vectorsets,
    datafusion_vector_0_101,
    datafusion_vector_101_3025,
    datafusion_vector_3025_5646,
    datafusion_vector_5646_6629,
    datafusion_vector_6652_6696,
    datafusion_vector_6724_6784,
)
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync


async def datafusion_resource(
    kbid: str,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
) -> str:
    """Resource with a link field extracted from https://datafusion.apache.org/
    (extracting only .bd-article CSS tag, the main article).

    TL;DR

    In order to test with more realistic examples, this processor broker message
    has been generated using stage's learning processor.

    To recreate, use a local nucliadb with a breakpoint in ingest Processor.
    POST to /resources (as above) and wait for the processing broker message to
    arrive. Once received, use google.protobuf.json_format.MessageToDict to
    generate a Python dict for the message. Copy and paste it here.

    In this test we then use google.protobuf.json_format.ParseDict to build the
    protobuffer again.

    With time, BrokerMessage fields may change, feel free to replace this
    messages with new ones if needed.

    """

    slug = "datafusion"
    link_field_id = "datafusion"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": slug,
            "title": "https://datafusion.apache.org/",
            "links": {
                link_field_id: {
                    "uri": "https://datafusion.apache.org/",
                    "css_selector": ".bd-article",
                    "xpath": None,
                }
            },
            "usermetadata": {"classifications": []},
            "icon": "application/stf-link",
            "origin": {"url": "https://datafusion.apache.org/"},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Processing stuff is a bit tricky, we must "upload" some files that have
    # been extracted from the link in order to ingest it. Each extracted
    # filename is in one of the JWT in link extracted data
    storage = await get_storage()
    link_extracted_data = [
        ("link_thumbnail", b"datafusion link JPEG thumbnail"),
        ("link_preview", b"datafusion link PDF preview"),
        ("link_image", b"datafusion link JPEG image"),
        ("generated/294ebe6279dd4a1ea5d228fb0ec637ae.svg", b"Apache DataFusion logo (dark)"),
        ("generated/2d4a3557ba85414b87d4eed6aeeb417c.svg", b"Apache DataFusion logo (light)"),
    ]
    for bucket_path, payload in link_extracted_data:
        sf = storage.file_extracted(kbid, rid, "l", link_field_id, bucket_path)
        await storage.chunked_upload_object(sf.bucket, sf.key, payload=payload)

    # Now, we can parse the broker message and ingest it
    processor_bm = BrokerMessage()
    ParseDict(
        {
            "kbid": kbid,
            "uuid": rid,
            "linkExtractedData": [
                {
                    "date": "2025-12-05T09:33:44.162051Z",
                    "language": "en",
                    "title": "Apache DataFusion — Apache DataFusion documentation",
                    "linkThumbnail": {
                        "source": "LOCAL",  # manual annotation to fake processing uploads
                        "uri": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1cm46cHJvY2Vzc2luZ19zbG93Iiwic3ViIjoiZmlsZSIsImF1ZCI6InVybjpwcm94eSIsImV4cCI6MTc2NTUzMjAzMSwiaWF0IjoxNzY0OTI3MjMxLCJqdGkiOiIwNDhhMjFlMTQ0N2I0MDY1ODYyMTFkNjY5YTIxNWM1MyIsImJ1Y2tldF9uYW1lIjoibmNsLXByb3h5LXN0b3JhZ2UtZ2NwLXN0YWdlLTEiLCJmaWxlbmFtZSI6InRodW1ibmFpbC5qcGciLCJ1cmkiOiJrYnMvZTc3YmYzZTEtOWIyYy00ZTZhLTkzYmItODY0NGZhODRjODgyL3IvY2FhODJmYWExZWQ2NDQ1OTk0NDI4MGMxOTI5MThkOTcvZS91L2xpbmsvbGlua190aHVtYm5haWwiLCJzaXplIjoxNzkwNTQsImNvbnRlbnRfdHlwZSI6ImltYWdlL2pwZWcifQ.oWq_UnrZYry3h6IZC55TDXaF1PXV88vV3J7__kK9v6I",
                    },
                    "linkPreview": {
                        "source": "LOCAL",  # manual annotation to fake processing uploads
                        "uri": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1cm46cHJvY2Vzc2luZ19zbG93Iiwic3ViIjoiZmlsZSIsImF1ZCI6InVybjpwcm94eSIsImV4cCI6MTc2NTUzMjAzMSwiaWF0IjoxNzY0OTI3MjMxLCJqdGkiOiI0MDEzNDlmZWQwNjU0MDJiODkzMjkxMzE3NTJkYWFhMCIsImJ1Y2tldF9uYW1lIjoibmNsLXByb3h5LXN0b3JhZ2UtZ2NwLXN0YWdlLTEiLCJmaWxlbmFtZSI6InByZXZpZXcucGRmIiwidXJpIjoia2JzL2U3N2JmM2UxLTliMmMtNGU2YS05M2JiLTg2NDRmYTg0Yzg4Mi9yL2NhYTgyZmFhMWVkNjQ0NTk5NDQyODBjMTkyOTE4ZDk3L2UvdS9saW5rL2xpbmtfcHJldmlldyIsInNpemUiOjM4NDU0NSwiY29udGVudF90eXBlIjoiYXBwbGljYXRpb24vcGRmIn0.SzrlfQO4eGCi_RhwRjL7s7bntcvlMrUH3krCscvOlbI",
                    },
                    "field": "link",
                    "linkImage": {
                        "source": "LOCAL",  # manual annotation to fake processing uploads
                        "uri": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1cm46cHJvY2Vzc2luZ19zbG93Iiwic3ViIjoiZmlsZSIsImF1ZCI6InVybjpwcm94eSIsImV4cCI6MTc2NTUzMjAzMSwiaWF0IjoxNzY0OTI3MjMxLCJqdGkiOiI0ODk2OTc0NzNjYTU0Y2M3ODE2MDgxMjEyOTBmZjkyMSIsImJ1Y2tldF9uYW1lIjoibmNsLXByb3h5LXN0b3JhZ2UtZ2NwLXN0YWdlLTEiLCJmaWxlbmFtZSI6InByZXZpZXcucG5nIiwidXJpIjoia2JzL2U3N2JmM2UxLTliMmMtNGU2YS05M2JiLTg2NDRmYTg0Yzg4Mi9yL2NhYTgyZmFhMWVkNjQ0NTk5NDQyODBjMTkyOTE4ZDk3L2UvdS9saW5rL2xpbmtfaW1hZ2UiLCJzaXplIjozMjI4MTEsImNvbnRlbnRfdHlwZSI6ImltYWdlL3BuZyJ9.qOR1Nk_APnvcl0bgku0aF2-Xd_RD-RHAANdtmc_F7iE",
                    },
                    "fileGenerated": {
                        "294ebe6279dd4a1ea5d228fb0ec637ae.svg": {
                            "source": "LOCAL",  # manual annotation to fake processing uploads
                            "uri": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1cm46cHJvY2Vzc2luZ19zbG93Iiwic3ViIjoiZmlsZSIsImF1ZCI6InVybjpwcm94eSIsImV4cCI6MTc2NTUzMjAzMSwiaWF0IjoxNzY0OTI3MjMxLCJqdGkiOiJlYzY3ZGRiNjBlOTE0MDJmYjAxMTc4ZDhjNTk1Nzk0OCIsImJ1Y2tldF9uYW1lIjoibmNsLXByb3h5LXN0b3JhZ2UtZ2NwLXN0YWdlLTEiLCJmaWxlbmFtZSI6IjI5NGViZTYyNzlkZDRhMWVhNWQyMjhmYjBlYzYzN2FlLnN2ZyIsInVyaSI6Imticy9lNzdiZjNlMS05YjJjLTRlNmEtOTNiYi04NjQ0ZmE4NGM4ODIvci9jYWE4MmZhYTFlZDY0NDU5OTQ0MjgwYzE5MjkxOGQ5Ny9lL3UvbGluay9nZW5lcmF0ZWQvMjk0ZWJlNjI3OWRkNGExZWE1ZDIyOGZiMGVjNjM3YWUuc3ZnIiwic2l6ZSI6OTA3MiwiY29udGVudF90eXBlIjoiaW1hZ2Uvc3ZnK3htbCJ9.dwwpJZfVD0ySsAINOmdQp7GQk-niUBCVtRipVlyhlH8",
                        },
                        "2d4a3557ba85414b87d4eed6aeeb417c.svg": {
                            "source": "LOCAL",  # manual annotation to fake processing uploads
                            "uri": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1cm46cHJvY2Vzc2luZ19zbG93Iiwic3ViIjoiZmlsZSIsImF1ZCI6InVybjpwcm94eSIsImV4cCI6MTc2NTUzMjAzMSwiaWF0IjoxNzY0OTI3MjMxLCJqdGkiOiJiNzZmZWY1M2IwMjc0YWFjYjlkYjdkMjY2Yjc1ZTc5NCIsImJ1Y2tldF9uYW1lIjoibmNsLXByb3h5LXN0b3JhZ2UtZ2NwLXN0YWdlLTEiLCJmaWxlbmFtZSI6IjJkNGEzNTU3YmE4NTQxNGI4N2Q0ZWVkNmFlZWI0MTdjLnN2ZyIsInVyaSI6Imticy9lNzdiZjNlMS05YjJjLTRlNmEtOTNiYi04NjQ0ZmE4NGM4ODIvci9jYWE4MmZhYTFlZDY0NDU5OTQ0MjgwYzE5MjkxOGQ5Ny9lL3UvbGluay9nZW5lcmF0ZWQvMmQ0YTM1NTdiYTg1NDE0Yjg3ZDRlZWQ2YWVlYjQxN2Muc3ZnIiwic2l6ZSI6OTAyOCwiY29udGVudF90eXBlIjoiaW1hZ2Uvc3ZnK3htbCJ9.eRbGSc_tkZAIjUhL9dA7NHhCRRHdYnkbuMJlsuZ1cVM",
                        },
                    },
                }
            ],
            "extractedText": [
                {
                    "body": {
                        "text": """Apache DataFusion — Apache DataFusion documentation .\n\n Image DataFusion Logo\nImage DataFusion Logo\n\n# Apache DataFusion[#](#apache-datafusion "Link to this heading")\n\n[Star](https://github.com/apache/datafusion)\n\n[Fork](https://github.com/apache/datafusion/fork)\n\nDataFusion is an extensible query engine written in [Rust](http://rustlang.org) that\nuses [Apache Arrow](https://arrow.apache.org) as its in-memory format.\n\nThe documentation on this site is for the [core DataFusion project](https://github.com/apache/datafusion), which contains\nlibraries and binaries for developers building fast and feature rich database and analytic systems,\ncustomized to particular workloads. See [use cases](https://datafusion.apache.org/user-guide/introduction.html#use-cases) for examples.\n\nThe following related subprojects target end users and have separate documentation.\n\n* [DataFusion Python](https://datafusion.apache.org/python/) offers a Python interface for SQL and DataFrame\n  queries.\n* [DataFusion Comet](https://datafusion.apache.org/comet/) is an accelerator for Apache Spark based on\n  DataFusion.\n* [DataFusion Ballista](https://datafusion.apache.org/ballista/) is distributed processing extension for DataFusion.\n\n"Out of the box," DataFusion offers [SQL](https://datafusion.apache.org/user-guide/sql/index.html)\nand [Dataframe](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html) APIs,\nexcellent [performance](https://benchmark.clickhouse.com/), built-in support for CSV, Parquet, JSON, and Avro,\nextensive customization, and a great community.\n[Python Bindings](https://github.com/apache/datafusion-python) are also available.\n[Ballista](https://datafusion.apache.org/ballista/) is Apache DataFusion extension enabling the parallelized execution of workloads across multiple nodes in a distributed environment.\n\nDataFusion features a full query planner, a columnar, streaming, multi-threaded,\nvectorized execution engine, and partitioned data sources. You can\ncustomize DataFusion at almost all points including additional data sources,\nquery languages, functions, custom operators and more.\nSee the [Architecture](https://datafusion.apache.org/contributor-guide/architecture.html) section for more details.\n\nTo get started, see\n\n* The [example usage](user-guide/example-usage.html) section of the user guide and the [datafusion-examples](https://github.com/apache/datafusion/tree/main/datafusion-examples) directory.\n* The [library user guide](library-user-guide/index.html) for examples of using DataFusion's extension APIs\n* The [developer's guide](contributor-guide/index.html#developer-s-guide) for contributing and [communication](contributor-guide/communication.html) for getting in touch with us.\n\nASF Links\n\n* [Apache Software Foundation](https://apache.org)\n* [License](https://www.apache.org/licenses/)\n* [Donate](https://www.apache.org/foundation/sponsorship.html)\n* [Thanks](https://www.apache.org/foundation/thanks.html)\n* [Security](https://www.apache.org/security/)\n\nLinks\n\n* [GitHub and Issue Tracker](https://github.com/apache/datafusion)\n* [crates.io](https://crates.io/crates/datafusion)\n* [API Docs](https://docs.rs/datafusion/latest/datafusion/)\n* [Blog](https://datafusion.apache.org/blog/)\n* [Code of conduct](https://github.com/apache/datafusion/blob/main/CODE_OF_CONDUCT.md)\n* [Download](download.html)\n\nUser Guide\n\n* [Introduction](user-guide/introduction.html)\n* [Example Usage](user-guide/example-usage.html)\n* [Features](user-guide/features.html)\n* [Concepts, Readings, Events](user-guide/concepts-readings-events.html)\n* [🌎 Community Events](user-guide/concepts-readings-events.html#community-events)\n* [Crate Configuration](user-guide/crate-configuration.html)\n* [DataFusion CLI](user-guide/cli/index.html)\n* [DataFrame API](user-guide/dataframe.html)\n* [Gentle Arrow Introduction](user-guide/arrow-introduction.html)\n* [Expression API](user-guide/expressions.html)\n* [SQL Reference](user-guide/sql/index.html)\n* [Configuration Settings](user-guide/configs.html)\n* [Runtime Configuration Settings](user-guide/configs.html#runtime-configuration-settings)\n* [Tuning Guide](user-guide/configs.html#tuning-guide)\n* [Join Algorithm Optimizer Configurations](user-guide/configs.html#join-algorithm-optimizer-configurations)\n* [Reading Explain Plans](user-guide/explain-usage.html)\n* [Metrics](user-guide/metrics.html)\n* [Frequently Asked Questions](user-guide/faq.html)\n* [How does DataFusion Compare with `XYZ`?](user-guide/faq.html#how-does-datafusion-compare-with-xyz)\n\nLibrary User Guide\n\n* [Introduction](library-user-guide/index.html)\n* [Upgrade Guides](library-user-guide/upgrading.html)\n* [Extensions List](library-user-guide/extensions.html)\n* [Using the SQL API](library-user-guide/using-the-sql-api.html)\n* [Working with `Expr`s](library-user-guide/working-with-exprs.html)\n* [Using the DataFrame API](library-user-guide/using-the-dataframe-api.html)\n* [Write DataFrame to Files](library-user-guide/using-the-dataframe-api.html#write-dataframe-to-files)\n* [Building Logical Plans](library-user-guide/building-logical-plans.html)\n* [Catalogs, Schemas, and Tables](library-user-guide/catalogs.html)\n* [Functions](library-user-guide/functions/index.html)\n* [Custom Table Provider](library-user-guide/custom-table-providers.html)\n* [Table Constraint Enforcement](library-user-guide/table-constraints.html)\n* [Extending DataFusion's operators: custom LogicalPlan and Execution Plans](library-user-guide/extending-operators.html)\n* [Profiling Cookbook](library-user-guide/profiling.html)\n* [DataFusion Query Optimizer](library-user-guide/query-optimizer.html)\n\nContributor Guide\n\n* [Introduction](contributor-guide/index.html)\n* [Developer's guide](contributor-guide/index.html#developer-s-guide)\n* [Communication](contributor-guide/communication.html)\n* [Development Environment](contributor-guide/development_environment.html)\n* [Architecture](contributor-guide/architecture.html)\n* [Testing](contributor-guide/testing.html)\n* [API health policy](contributor-guide/api-health.html)\n* [HOWTOs](contributor-guide/howtos.html)\n* [Roadmap and Improvement Proposals](contributor-guide/roadmap.html)\n* [Governance](contributor-guide/governance.html)\n* [Inviting New Committers and PMC Members](contributor-guide/inviting.html)\n* [Specifications](contributor-guide/specification/index.html)\n* [Google Summer of Code (GSOC)](contributor-guide/gsoc/index.html)\n\nDataFusion Subprojects\n\n* [DataFusion Ballista](https://datafusion.apache.org/ballista/)\n* [DataFusion Comet](https://datafusion.apache.org/comet/)\n* [DataFusion Python](https://datafusion.apache.org/python/)APACHE\n\nDATAFUSION \n\n\n Apache Datafusion logo on a black background \n\n\n APACHE\n\nDATAFUSION \n\n\n A black background with the words "Apache Datafusion" on it. \n\n\n """
                    },
                    "field": {"fieldType": "LINK", "field": "link"},
                },
                {
                    "body": {"text": "https://datafusion.apache.org/"},
                    "field": {"fieldType": "GENERIC", "field": "title"},
                },
            ],
            "fieldMetadata": [
                {
                    "metadata": {
                        "metadata": {
                            "links": [
                                "https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html",
                                "https://datafusion.apache.org/user-guide/introduction.html#use-cases",
                                "https://github.com/apache/datafusion/tree/main/datafusion-examples",
                                "https://github.com/apache/datafusion/blob/main/CODE_OF_CONDUCT.md",
                                "https://datafusion.apache.org/contributor-guide/architecture.html",
                                "https://datafusion.apache.org/user-guide/sql/index.html",
                                "https://www.apache.org/foundation/sponsorship.html",
                                "https://docs.rs/datafusion/latest/datafusion/",
                                "https://www.apache.org/foundation/thanks.html",
                                "https://github.com/apache/datafusion-python",
                                "https://github.com/apache/datafusion/fork",
                                "https://datafusion.apache.org/ballista/",
                                "https://datafusion.apache.org/python/",
                                "https://datafusion.apache.org/comet/",
                                "https://github.com/apache/datafusion",
                                "https://datafusion.apache.org/blog/",
                                "https://crates.io/crates/datafusion",
                                "https://benchmark.clickhouse.com/",
                                "https://www.apache.org/security/",
                                "https://www.apache.org/licenses/",
                                "https://arrow.apache.org",
                                "http://rustlang.org",
                                "https://apache.org",
                                "crates.io",
                            ],
                            "paragraphs": [
                                {
                                    "end": 101,
                                    "sentences": [{"end": 56}, {"start": 56, "end": 101}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {},
                                },
                                {
                                    "start": 101,
                                    "end": 3025,
                                    "sentences": [
                                        {"start": 101, "end": 486},
                                        {"start": 486, "end": 744},
                                        {"start": 744, "end": 845},
                                        {"start": 845, "end": 930},
                                        {"start": 930, "end": 1050},
                                        {"start": 1050, "end": 1167},
                                        {"start": 1167, "end": 1285},
                                        {"start": 1285, "end": 1643},
                                        {"start": 1643, "end": 1726},
                                        {"start": 1726, "end": 1911},
                                        {"start": 1911, "end": 2051},
                                        {"start": 2051, "end": 2191},
                                        {"start": 2191, "end": 2308},
                                        {"start": 2308, "end": 2517},
                                        {"start": 2517, "end": 2805},
                                        {"start": 2805, "end": 3089},
                                    ],
                                    "page": {"pageWithVisual": True},
                                    "representation": {},
                                },
                                {
                                    "start": 3025,
                                    "end": 5646,
                                    "sentences": [{"start": 3025, "end": 5710}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {},
                                },
                                {
                                    "start": 5646,
                                    "end": 6629,
                                    "sentences": [{"start": 5646, "end": 6693}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {},
                                },
                                {
                                    "start": 6629,
                                    "end": 6647,
                                    "kind": "OCR",
                                    "sentences": [{"start": 6629, "end": 6647}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {
                                        "referenceFile": "294ebe6279dd4a1ea5d228fb0ec637ae.svg"
                                    },
                                },
                                {
                                    "start": 6652,
                                    "end": 6696,
                                    "kind": "INCEPTION",
                                    "sentences": [{"start": 6652, "end": 6696}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {
                                        "referenceFile": "294ebe6279dd4a1ea5d228fb0ec637ae.svg"
                                    },
                                },
                                {
                                    "start": 6701,
                                    "end": 6719,
                                    "kind": "OCR",
                                    "sentences": [{"start": 6701, "end": 6719}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {
                                        "referenceFile": "2d4a3557ba85414b87d4eed6aeeb417c.svg"
                                    },
                                },
                                {
                                    "start": 6724,
                                    "end": 6784,
                                    "kind": "INCEPTION",
                                    "sentences": [{"start": 6724, "end": 6784}],
                                    "page": {"pageWithVisual": True},
                                    "representation": {
                                        "referenceFile": "2d4a3557ba85414b87d4eed6aeeb417c.svg"
                                    },
                                },
                            ],
                            "lastUnderstanding": "2025-12-05T09:33:55.846595Z",
                            "lastExtract": "2025-12-05T09:33:52.195048Z",
                            "language": "en",
                            "relations": [
                                {
                                    "relations": [
                                        {
                                            "relation": "OTHER",
                                            "source": {"value": "Python", "subtype": "LANGUAGE"},
                                            "to": {"value": "DataFrame", "subtype": "PRODUCT"},
                                            "relationLabel": "operating system",
                                            "metadata": {
                                                "paragraphId": "caa82faa1ed64459944280c192918d97/u/link/101-3025",
                                                "sourceStart": 936,
                                                "sourceEnd": 942,
                                                "toStart": 965,
                                                "toEnd": 974,
                                            },
                                        },
                                        {
                                            "relation": "OTHER",
                                            "source": {"value": "Apache Spark", "subtype": "PRODUCT"},
                                            "to": {"value": "ballista", "subtype": "PRODUCT"},
                                            "relationLabel": "operating system",
                                            "metadata": {
                                                "paragraphId": "caa82faa1ed64459944280c192918d97/u/link/101-3025",
                                                "sourceStart": 1067,
                                                "sourceEnd": 1079,
                                                "toStart": 1157,
                                                "toEnd": 1165,
                                            },
                                        },
                                        {
                                            "relation": "OTHER",
                                            "source": {"value": "DataFusion", "subtype": "ORG"},
                                            "to": {"value": "DataFusion", "subtype": "ORG"},
                                            "relationLabel": "developer",
                                            "metadata": {
                                                "paragraphId": "caa82faa1ed64459944280c192918d97/u/link/101-3025",
                                                "sourceStart": 1208,
                                                "sourceEnd": 1218,
                                                "toStart": 1239,
                                                "toEnd": 1249,
                                            },
                                        },
                                    ]
                                }
                            ],
                            "mimeType": "text/html",
                            "entities": {
                                "processor": {
                                    "entities": [
                                        {
                                            "text": "Apache DataFusion",
                                            "label": "ORG",
                                            "positions": [{"end": "17"}, {"start": "20", "end": "37"}],
                                        },
                                        {
                                            "text": "DataFusion",
                                            "label": "PRODUCT",
                                            "positions": [
                                                {"start": "265", "end": "275"},
                                                {"start": "470", "end": "480"},
                                                {"start": "869", "end": "879"},
                                                {"start": "1091", "end": "1101"},
                                                {"start": "1208", "end": "1218"},
                                                {"start": "1239", "end": "1249"},
                                                {"start": "1847", "end": "1857"},
                                                {"start": "2005", "end": "2015"},
                                                {"start": "2533", "end": "2543"},
                                            ],
                                        },
                                        {
                                            "text": "Rust",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "318", "end": "322"}],
                                        },
                                        {
                                            "text": "Apache Arrow",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "356", "end": "368"}],
                                        },
                                        {
                                            "text": "Python",
                                            "label": "LANGUAGE",
                                            "positions": [{"start": "936", "end": "942"}],
                                        },
                                        {
                                            "text": "DataFrame",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "965", "end": "974"}],
                                        },
                                        {
                                            "text": "Apache Spark",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1067", "end": "1079"}],
                                        },
                                        {
                                            "text": "DataFusion Ballista",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1106", "end": "1125"}],
                                        },
                                        {
                                            "text": "ballista",
                                            "label": "PRODUCT",
                                            "positions": [
                                                {"start": "1157", "end": "1165"},
                                                {"start": "1703", "end": "1711"},
                                            ],
                                        },
                                        {
                                            "text": "SQL",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1258", "end": "1261"}],
                                        },
                                        {
                                            "text": "CSV",
                                            "label": "ORG",
                                            "positions": [{"start": "1501", "end": "1504"}],
                                        },
                                        {
                                            "text": "Parquet",
                                            "label": "ORG",
                                            "positions": [{"start": "1506", "end": "1513"}],
                                        },
                                        {
                                            "text": "JSON",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1515", "end": "1519"}],
                                        },
                                        {
                                            "text": "Avro",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1525", "end": "1529"}],
                                        },
                                        {
                                            "text": "Ballista",
                                            "label": "PRODUCT",
                                            "positions": [{"start": "1663", "end": "1671"}],
                                        },
                                        {
                                            "text": "taFusion",
                                            "label": "ORG",
                                            "positions": [{"start": "1726", "end": "1734"}],
                                        },
                                        {
                                            "text": "Apache Software Foundation",
                                            "label": "ORG",
                                            "positions": [{"start": "2755", "end": "2781"}],
                                        },
                                        {
                                            "text": "Apache Datafusion",
                                            "label": "ORG",
                                            "positions": [{"start": "6652", "end": "6669"}],
                                        },
                                    ]
                                }
                            },
                            "lastProcessingStart": "2025-12-05T09:33:41.450854Z",
                        }
                    },
                    "field": {"fieldType": "LINK", "field": "link"},
                },
                {
                    "metadata": {
                        "metadata": {
                            "links": ["https://datafusion.apache.org/"],
                            "paragraphs": [{"end": 30, "sentences": [{"end": 30}]}],
                            "lastUnderstanding": "2025-12-05T09:33:55.912497Z",
                            "lastExtract": "2025-12-05T09:33:55.862840Z",
                            "language": "cy",
                            "mimeType": "text/plain",
                            "entities": {"processor": {}},
                            "lastProcessingStart": "2025-12-05T09:33:41.450854Z",
                        }
                    },
                    "field": {"fieldType": "GENERIC", "field": "title"},
                },
            ],
            "fieldVectors": [
                {
                    "vectors": {
                        "vectors": {
                            "vectors": [
                                {
                                    "end": 101,
                                    "endParagraph": 101,
                                    "vector": datafusion_vector_0_101,
                                },
                                {
                                    "start": 101,
                                    "end": 3025,
                                    "startParagraph": 101,
                                    "endParagraph": 3025,
                                    "vector": datafusion_vector_101_3025,
                                },
                                {
                                    "start": 3025,
                                    "end": 5646,
                                    "startParagraph": 3025,
                                    "endParagraph": 5646,
                                    "vector": datafusion_vector_3025_5646,
                                },
                                {
                                    "start": 5646,
                                    "end": 6629,
                                    "startParagraph": 5646,
                                    "endParagraph": 6629,
                                    "vector": datafusion_vector_5646_6629,
                                },
                                {
                                    "start": 6652,
                                    "end": 6696,
                                    "startParagraph": 6652,
                                    "endParagraph": 6696,
                                    "vector": datafusion_vector_6652_6696,
                                },
                                {
                                    "start": 6724,
                                    "end": 6784,
                                    "startParagraph": 6724,
                                    "endParagraph": 6784,
                                    "vector": datafusion_vector_6724_6784,
                                },
                            ]
                        }
                    },
                    "field": {"fieldType": "LINK", "field": "link"},
                    "vectorsetId": "en-2024-04-24",
                },
                {"field": {"fieldType": "GENERIC", "field": "title"}, "vectorsetId": "en-2024-04-24"},
            ],
            "doneTime": "2025-12-05T09:33:55.913625Z",
            "processingId": "3c9fe327-d213-4420-be9f-98c7cf648323",
            "source": "PROCESSOR",
            "questionAnswers": [
                {"field": {"fieldType": "LINK", "field": "link"}},
                {"field": {"fieldType": "GENERIC", "field": "title"}},
            ],
            "generatedBy": [{"processor": {}}],
        },
        processor_bm,
    )

    await adjust_kb_vectorsets(kbid, processor_bm)

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, processor_bm)
    await wait_for_sync()

    return rid
