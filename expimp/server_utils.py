from typing import AsyncIterator

from shared import LINE_SEPARATOR, Resource, decode_binary, decode_resource


async def iter_request_resources(request) -> AsyncIterator[Resource]:
    async for line in iter_request_lines(request):
        yield decode_resource(line)


async def iter_request_binaries(request) -> AsyncIterator[tuple[str, bytes]]:
    async for line in iter_request_lines(request):
        yield decode_binary(line)


async def iter_request_lines(request) -> AsyncIterator[str]:
    line_leftover = None
    async for chunk in request.stream():
        if chunk == b"":
            continue
        chunk_parts = chunk.split(LINE_SEPARATOR)
        if line_leftover is not None:
            is_intermediate_chunk = len(chunk_parts) == 1
            if is_intermediate_chunk:
                line_leftover += chunk
                continue
            else:
                line_leftover += chunk_parts[0]
                yield line_leftover
                line_leftover = None
                chunk_parts = chunk_parts[1:]

        chunk_ends_line = chunk_parts[-1] == b""
        if chunk_ends_line:
            line_leftover = None
        else:
            line_leftover = chunk_parts[-1]
        for line in chunk_parts[:-1]:
            yield line
