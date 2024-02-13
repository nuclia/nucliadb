# NucliaDB SDK

The NucliaDB SDK is a Python library designed as a thin wrapper around the [NucliaDB HTTP API](https://docs.nuclia.dev/docs/api). It is tailored for developers who wish to create low-level scripts to interact with NucliaDB.

## :warning: WARNING :warning:

If it's your first time using Nuclia or you want a simple way to push your unstructured data to Nuclia with a script or a CLI, we highly recommend using the [Nuclia CLI/SDK](https://github.com/nuclia/nuclia.py) instead, as it is much more user-friendly and use-case focused.

## Installation

To install it, simply with pip:

```bash
pip install nucliadb-sdk
```

## How to use it?

You can find the auto-generated documentation of the NucliaDB sdk [here](https://docs.nuclia.dev/docs/guides/nucliadb/python_nucliadb_sdk).

Essentially, each method of the `NucliaDB` class maps to an HTTP endpoint of the NucliaDB API. The parameters it accepts correspond to the Pydantic models associated to the request body scheme of the endpoint.

The method-to-endpoint mappings for the sdk are declared in-code [in the _NucliaDBBase class](https://github.com/nuclia/nucliadb/blob/main/nucliadb_sdk/nucliadb_sdk/v2/sdk.py).

For instance, to create a resource in your Knowledge Box, the endpoint is defined [here](https://docs.nuclia.dev/docs/api#tag/Resources/operation/Create_Resource_kb__kbid__resources_post).

It has a `{kbid}` path parameter and is expecting a json payload with some optional keys like `slug` or `title`, that are of type string. With `curl`, the command would be:

```bash
curl -XPOST http://localhost:8080/api/v1/kb/my-kbid/resources -H 'x-nucliadb-roles: WRITER' --data-binary '{"slug":"my-resource","title":"My Resource"}' -H "Content-Type: application/json"
{"uuid":"fbdb10a79abc45c0b13400f5697ea2ba","seqid":1}
```

and with the NucliaDB sdk:

```python
>>> from nucliadb_sdk import NucliaDB
>>>
>>> ndb = NucliaDB(region="on-prem", url="http://localhost:8080/api")
>>> ndb.create_resource(kbid="my-kbid", slug="my-resource", title="My Resource")
ResourceCreated(uuid='fbdb10a79abc45c0b13400f5697ea2ba', elapsed=None, seqid=1)
```

Note that paths parameters are mapped as required keyword arguments of the `NucliaDB` class methods: hence the `kbid="my-kbid"`. Any other keyword arguments specified in the method will be sent along in the json request body of the HTTP request.

Alternatively, you can also define the `content` parameter and pass an instance of the Pydantic model that the endpoint expects:

```python
>>> from nucliadb_sdk import NucliaDB
>>> from nucliadb_models.writer import CreateResourcePayload
>>> 
>>> ndb = NucliaDB(region="on-prem", url="http://localhost:8080/api")
>>> content = CreateResourcePayload(slug="my-resource", title="My Resource")
>>> ndb.create_resource(kbid="my-kbid", content=content)
ResourceCreated(uuid='fbdb10a79abc45c0b13400f5697ea2ba', elapsed=None, seqid=1)
```

Query parameters can be passed too on each method with the `query_params` argument. For instance:

```python
>>> ndb.get_resource_by_id(kbid="my-kbid", rid="rid", query_params={"show": ["values"]})
```

### Example Usage

The following is a sample script that fetches the HTML of a website, extracts all links that it finds on it and pushes them to NucliaDB so that they get processed by Nuclia's processing engine.

```python
from nucliadb_models.link import LinkField
from nucliadb_models.writer import CreateResourcePayload
import nucliadb_sdk
import requests
from bs4 import BeautifulSoup


def extract_links_from_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    unique_links = set()
    for link in soup.find_all("a"):
        unique_links.add(link.get("href"))
    return unique_links


def upload_link_to_nuclia(ndb, *, kbid, link, tags):
    try:
        title = link.replace("-", " ")
        slug = "-".join(tags) + "-" + link.split("/")[-1]
        content = CreateResourcePayload(
            title=title,
            slug=slug,
            links={
                "link": LinkField(
                    uri=link,
                    language="en",
                )
            },
        )
        ndb.create_resource(kbid=kbid, content=content)
        print(f"Resource created from {link}. Title={title} Slug={slug}")
    except nucliadb_sdk.exceptions.ConflictError:
        print(f"Resource already exists: {link} {slug}")
    except Exception as ex:
        print(f"Failed to create resource: {link} {slug}: {ex}")


def main(site):
    # Define the NucliaDB instance with region and URL
    ndb = nucliadb_sdk.NucliaDB(region="on-prem", url="http://localhost:8080")

    # Loop through extracted links and upload to NucliaDB
    for link in extract_links_from_url(site):
        upload_link_to_nuclia(ndb, kbid="my-kb-id", link=link, tags=["news"])

if __name__ == "__main__":
    main(site="https://en.wikipedia.org/wiki/The_Lion_King")
```

After the data is pushed, the NucliaDB SDK could also be used to find answers on top of the extracted links.

```python
>>> import nucliadb_sdk
>>> 
>>> ndb = nucliadb_sdk.NucliaDB(region="on-prem", url="http://localhost:8080")
>>> resp = ndb.chat(kbid="my-kb-id", query="What does Hakuna Matata mean?")
>>> print(resp.answer)
'Hakuna matata is actually a phrase in the East African language of Swahili that literally means “no trouble” or “no problems”.'
```

## Conclusion

Explore more features and capabilities of the NucliaDB SDK by referring to the [official documentation](https://docs.nuclia.dev/docs/guides/nucliadb/python_nucliadb_sdk). We welcome your feedback and contributions to make this SDK even better!
