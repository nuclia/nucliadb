# Exporting and importing

NucliaDB allows you to generate an export of all the extracted information that is not binary on a file to import on a new NucliaDB.
## EXPORTING

Via HTTP
```bash
# To start an export. You get the export id in the response
EXPORT_ID=`curl -XPOST 'http://localhost:8080/api/v1/kb/$KBID/export' -s -H "x-nucliadb-roles: MANAGER" | jq .export_id`

# To check the status of the export
curl -XGET http://localhost:8080/api/v1/kb/$KBID/export/$EXPORT_ID/status -H "x-nucliadb-roles: READER"

# To download the export once it has been finished
curl -XGET http://localhost:8080/api/v1/kb/$KBID/export/$EXPORT_ID -o /path/to/export/file
```

Via python
```python

from nucliadb_sdk import NucliaDB, Region

sdk = NucliaDB(region=Region.ON_PREM, url="http://localhost:8080/api")

# To start the export
export_id = sdk.start_export(kbid=kbid).export_id

# To check the export status
status = sdk.export_status(kbid=kbid, export_id=export_id).status
assert status.value == "finished"

# To download the export
export_gen = sdk.download_export(kbid=kbid, export_id=export_id)
with open("/path/to/export/file", "wb") as f:
    for chunk in export_gen(chunk_size=1024):
        f.write(chunk)
```

## IMPORTING

Via HTTP
```bash
# To upload the exported data and start an import. You get the import id in the response
curl http://localhost:8080/api/v1/kb/$KBID/import -H 'x-nucliadb-roles: MANAGER' -H 'Content-Type: binary/octet-stream' --data-binary @/path/to/export/file

# To check the status of the import
curl -XGET http://localhost:8080/api/v1/kb/$KBID/import/$IMPORT_ID/status -H 'x-nucliadb-roles: READER'
```

Via python
```python

from nucliadb_sdk import NucliaDB, Region

sdk = NucliaDB(region=Region.ON_PREM, url="http://localhost:8080/api")

# To upload the exported data and start the import
import_id = sdk.start_import(kbid=kbid, content=open("/path/to/export/file", "rb")).import_id

# To check the import status
status = sdk.import_status(kbid=kbid, import_id=import_id).status
assert status.value == "finished"
```