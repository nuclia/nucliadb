# Role based security

NucliaDB security model is based on three roles:

- MANAGER
- READER
- WRITER

Roles are passed through the HTTP Header `X-NUCLIADB-ROLES` and writen
in upper case. Multiple roles can be sent appending them with
semicolons, e.g., `X-NUCLIADB-ROLES: MANAGER;READER`.

## Manager role

*Manager* role is needed to do management operations on a
NucliaDB. A Manager can perform CRUD operations on a Knowledge Box and
get information as its counters or shards.

Operations "inside" a Knowledge Box are performed with READER and
WRITER roles.

## Reader role

*Reader* role allows all kind of reading operations. This include
reading Knowledge Boxes, resources and fields, as well as searching
content.

## Writer role

*Writer* role is intended for all write operations. That include creation, update
and deletion of resources and fields, set and remove entities groups,
labels, upload files, reindex resources and send resources
to reprocess.
