# SEARCH

## FEATURES

### RESOURCE

Search documents by keyword with facets, order, filter and batch and boolean and exact match
Score BM25

### PARAGRAPHS

Search paragraphs by fuzzy search with facets, order, filter and batch and boolean and exact match
Score Word distance

### RELATIONS

Search resources/labels/entities by a bunch of entities/resources/labels

### VECTOR

Search sentences by distance with a vector with facets

### SEARCH UNIFIED

Based on query and filters and batch query merge of:

- vectors distance filtered
- bm25 of documents
- fuzzy search of paragraphs
- relations from query and proposed results

### RESOURCE SEARCH

Search information on resource

### "BRAIN" INFORMATION

Choose which metadata to serialize

## IMPLEMENTATION

### OWN LANGUAGE QUERY PARAMETER vs STRUCTURAL

q = "MY QUERY tags:e/Ramon ..."

### STRUCTURAL GET / POST

https://nuclia.cloud/api/kb/12341231231231/search?query="MY QUERY"&filters="e/Ramon"&filters="c/Good"&faceted="e"&faceted="t"&sort="title"&page_number=0&page_size=20&features="documents"&features="paragraphs"

    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    sort_ord: Sort = Sort.ASC.value,

### RESOURCE URL / PARAMETER

https://nuclia.cloud/api/kb/12341231231231/search?resource=uuid... vs https://nuclia.cloud/api/kb/12341231231231/resource/uuid/search?...

...

"whatwever i need is this filter:e/Ramon"

features=vectors,documents,paragraphs,relations

metadata=title,summary,icon

    resources: {
        rid1: {title: asdsad, summary: asdsadsad}
        rid2: {title: asddsad, summary: asdasdd}
    }
    sentences: [
        results: {sentece1, rid1, score: 0.4, text: "asdasd"}
        facets: ...
    ]
    paragraphs: {
        results: {paragraph1, rid1, score: 0.5, text: "asdasd", labels: [...]}
        facets: ...
    }
    documents: {
        results: {rid1, score: 10.0}
        facets: ...
    }
    relations: {
        results: {rid1}
        facets: ...
    ]
    facets: [
        ....
    ]

### FILTERS

Filters can be used to narrow down the scope of a search query, whereas facets are typically used to get category counts on search results.

NucliaDB implements its own syntax for filters and facets based on "tags". This section aims to describe the complete list of tags available by providing some examples.

The hierarchy of tags is the following:

- `/t`: user provided tags
  - Example: `/t/blue`, `/t/green`

- `/l`: user and processor provided labels: `/l/{labelset}/{label}`
  - Example: `/l/movie-genre/science-fiction`

- `/n`: miscellanious resource metadata labels
  - `/n/i`: mime type of resource
    - Example filters: `/n/i/application/pdf` or `/n/i/movie/mp4`

  - `/n/s`: processing status
    - Example facet results: `/n/s/PROCESSED`, `/n/s/PENDING` or `/n/s/ERROR`

- `/e`: document entities as `/e/{entity-type}/{entity-id}`
  - Example: `/e/CITY/Barcelona`

- `/s`: languages(`s` for speech)
  - `/s/p`: primary language of the document
    - Example: `/s/p/ca` for catalan language
  - `/s/s`: all other detected languages
    - Example: `/s/s/tr` for turkish language

- `/u`: contributors/users/sources
  - `/u/s`: Origin source
    - Example: `/u/s/WEB`
  - `/u/o`: Origin contributors
    - Example: `/u/o/username`

- `/f`: field keyword field (field/keyword)
  - Example: `/f/fieldname/value`

- `/fg`: flat field keywords
  - Example: `/fg/value`
