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

https://nuclia.cloud/api/kb/12341231231231/search?resource=uuid... vs https://nuclia.cloud/api/kb/12341231231231/rid/uuid/search?...

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
