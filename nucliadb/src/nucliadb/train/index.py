from collections.abc import AsyncIterator

from nidx_protos.nodereader_pb2 import DocumentItem, ParagraphItem, StreamRequest


async def stream_documents(request: StreamRequest) -> AsyncIterator[DocumentItem]:
    # TODO(Marklogic): Stream documents matching the request from the indexing backend.
    documents: tuple[DocumentItem, ...] = ()
    for document in documents:
        yield document


async def stream_paragraphs(request: StreamRequest) -> AsyncIterator[ParagraphItem]:
    # TODO(Marklogic): Stream paragraphs matching the request from the indexing backend.
    paragraphs: tuple[ParagraphItem, ...] = ()
    for paragraph in paragraphs:
        yield paragraph
