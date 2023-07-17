# Python project refactoring guidance

The purpose of this proposal is to provide guidance on how we should
build/refactor our python components in order to better support and
manage our technical debt.


## Current solution/situation

Currently, most of our python code is in the `nucliadb` folder. This folder
is where most of our complexity and technical debt on the python side of things is.

Our system is a tightly coupled system. This makes sense because we are a database
and tight coupling is expected; however, some of this tight coupling feels awkward
with how each component in the system is separately bootstraped/setup/deployed/run.

This provides a situation where we have some less than ideal module/system dependencies
between our main components.

### Main components

Currently, our main top level components are:
- `ingest`
- `reader`
- `writer`
- `search`
- `train`
- `standalone`

In an ideal world, these components would not import from each other(except standalone).
However, in our world we have some awkward interfaces between `ingest` and other components.

### Ingest component abuse

`ingest` is our component that is mostly responsible for modifying data across the application.
`ingest` also is responsible in many ways for accessing data across the platform.

Even though we have a component called `writer`, it is `ingest` that a lot of the magic around
pulling data.

Moreover, `ingest/orm` and `ingest/fields` do a lot of the data mapping between what is coming
into the system and what is persisted in the k/v storages.

Difficulties:
- ingest is imported directly in reader, writer and search
- ingest GRPC service is used in reader/writer


#### Ingest GRPC Service

The `ingest` GRPC service feels like an awkward abstraction to use internally.

Like what was mentioned above, our system is already tightly coupled so abstracting
what should be more like a logical boundary directly into an unnecessary physical boundary
seems like a mistake when the data is already managed in a separate storage anyways.

A GPRC service should be used more explicitly for service to service
physical service boundaries and service boundaries are necessary for scaling
implementation requirements.

In our case, the GRPC service is only used for a couple endpoints by other internal
systems and we should target our seperate interanl systems as the audience for the
service instead of ourselves.


### Data access complexity

Data write/read access patterns and knowledge is spread in many parts of the code base.

There are 2 ways we write data to k/v storages:
- primary k/v storage: tikv, pg, etc
- blob k/v storage: gcs, s3, etc

Direct access to the data is managed through:
- `get_driver`: configured k/v storage layer
- `get_storage`: configured blob storage layer interface

Data mapping is mostly managed in 3 ways:
- `ingest/orm`: component that manages keys and storing/retrieving data
- `ingest/fields`: component that manages keys and storing/retrieving data for specific field types
- custom data access using `get_driver` and `get_storage` throughout the code base


#### "ORM"

We have a module described as ORM but it's not an Object Relational Mapper. We don't have
a relational database and the module doesn't map data like the the interfaces suggest it might.

For example, the `nucliadb.ingest.orm.knowledgebox.KnowledgeBox` class might make you think
that you have an instance of a knowledge box that can be read, modified and persisted back
to the database.

However, the `KnowledgeBox` class actually has responsibility for reading/writing various
protobuf objects related to a KnowledgeBox, getting resources, creating shards and purging data.
It is a mix of instance and class methods.


#### Protobuf usage

Protobuf being a type system and state (de)serialization system may make you think it is
reasonable to use to communicate types internally; however, protobuf, especially for how
we use it, is primarily a database schema definition system.

When we use it to communicate state internally between components, we are exposing internal
database storage details across our application. This is a data mapping pattern that often
becomes difficult to manage.

For example, imagine you have a relational database where data is stored in rows of fields.
Then, you communicate database state as a dictionary across your application, directly
mapping field names -> dictionary keys. What happens during a migration? What happens if
you need to change the underlying implementation of the storage layer for a particular interface?
You end up needing to update logic and/or state all over the code base. However, if you
isolated those contracts to a data layer, your exposure to these sorts of problems is limited.

### Global state

A common pattern we use is to configure global components that are potentially
used throughout the code base.

Examples:

- `get_storage`
- `get_driver`
- `get_cache`
- `get_pubsub`
- `get_partitioning`
- `settings` -- all settings right now are initialized when a component is loaded
- ...

These components might be conditionally configured depending on what a top level component
needs.

This type of approach ends up encouraging continued increase in global state and injecting
that global state into various components.

Functions no longer do simple input/output since their input/output is a side effect of
the global state.

Finally, testing can be quite difficult because you always need to setup/teardown that global
state OR monkey patch it.

## Proposed Solution

The proposal focusses on moving toward 3 patterns:

1. Do not allow imports between our main components
2. Isolate our data layers into separate interfaces
3. Do not use global application state


### Do not allow imports between our main components

- `ingest` focuses on components for ingesting data and not providing interfaces to other components
- shared tooling and data layers are moved to the `nucliadb.common` module
- no main component should import from the `ingest` module
- nucliadb should stop using the `ingest` grpc service

### Isolate our data layers into separate interfaces

- create data layer management components
- limit direct exposure of k/v and blob storage drivers
- limit exposure of protobuf types(this might be very difficult and cumbersome)
- Attempt to separate reads/writes into separate methods in case they need more explicit isolation later.
  We do not need command/query type system but it is similar to that idea.


### Do not use global application state

- Move toward patterns where application execution state is passed into components
- short term means there is a hybrid approach
- unify application state between main components


#### Example FastAPI setup

Currently, we setup our FastAPI services like this:

```python
from somewhere import settings

async def initialize():
    '''
    configure global components with global settings
    '''

async def finalize():
    '''
    tear down global components
    '''

app = FastAPI(
    on_startup=[initialize],
    on_shutdown=[finalize],
)
app.include_router(api_v1)
uvicorn.run(app)
```

Characteristics of this code block that could be considered anti-patterns:
- global settings
- global `app` instance
- global `app` is configuring global initialization dependencies that endpoints implicitly use
- application/settings is instantiated at import time instead of runtime.

An alternative approach could look like:

```python
class ApplicationContext:
    data_manager: DataManager
    def __init__(self, settings):
        self.settings = settings
    
    async def initialize(self):
        self.data_mananger = DataManager(self.settings)
        await self.data_mananger.initialize()

    async def finalize(self):
        await self.data_mananger.finalize()

class HTTPApplication(fastapi.FastAPI):
    def __init__(self, settings: Settings, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.application_context = ApplicationContext(settings)
        self.include_router(router)
        self.on_event("startup")(self.initialize)
        self.on_event("shutdown")(self.finalize)

    async def initialize(self) -> None:
        await self.application_context.initialize()

    async def finalize(self) -> None:
        await self.application_context.finalize()


def main():
    app = HTTPApplication(Settings())
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

- single ApplicationContext to manage runtime state
- app/settings are instantiated at runtime instead of import time
- enabled testing endpoints without needing to mock global state since you can directly inject

#### Example endpoint

Let's take a simple example for this code block:

```python
@api.get(
    f"/{KBS_PREFIX}",
    status_code=200,
    name="List Knowledge Boxes",
    response_model=KnowledgeBoxList,
    tags=["Knowledge Boxes"],
    include_in_schema=False,
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def get_kbs(request: Request, prefix: str = "") -> KnowledgeBoxList:
    driver = get_driver()
    async with driver.transaction() as txn:
        response = KnowledgeBoxList()
        async for kbid, slug in KnowledgeBox.get_kbs(txn, prefix):
            response.kbs.append(KnowledgeBoxObjSummary(slug=slug or None, uuid=kbid))
        return response
```

Characteristics of this code block that could be considered anti-patterns:
- `get_driver` is global state
- `driver.transaction` requires internal db implementation knowledge and can not
  easily be changed later
- `KnowledgeBox.get_kbs` is class method that gets a txn passed in


Alternative:

```python
@api.get(
    f"/{KBS_PREFIX}",
    status_code=200,
    name="List Knowledge Boxes",
    response_model=KnowledgeBoxList,
    tags=["Knowledge Boxes"],
    include_in_schema=False,
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def get_kbs(prefix: str = "", app_context: AppContext) -> KnowledgeBoxList:
    kb_data_manager = app_context.kb_data_manager
    response = KnowledgeBoxList()
    async for kbid, slug in kb_data_manager.list(prefix=prefix):
        response.kbs.append(KnowledgeBoxObjSummary(slug=slug or None, uuid=kbid))
    return response
```

- `app_context` is injected from the FastAPI runtime initialization but not global state
- does not expose transaction details
- testing allows directly calling `get_kbs` with `app_context` injected and no other
  test setup required

### Risks

- Managing a code base with many different ways to do things.
- Migrating a lot of tests
- Considerable refactoring



### Implementation strategy

1. Limit and isolate imports from `ingress` in other modules
2. Start integrating datamanager implementations for adopting data access alternatives for `ingress` in the `common` module
3. Refactor out the usage of the `ingress` GRPC service

Once these steps are done, re-evaluate status.