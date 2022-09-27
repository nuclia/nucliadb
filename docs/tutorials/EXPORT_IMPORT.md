# Exporting and importing

NucliaDB allows you to generate an export of all the extracted information that is not binary on a file to import on a new NucliaDB. This tools is designed to be used with the open source version of NucliaDB on your own installation and will not work on the `nuclia.cloud` managed service.

## EXPORTING

```
pip install nucliadb_client
nucliadb_export --host=localhost --grpc=8030 --http=8080 --slug=MYKBSLUG --dump=FILE_TO_EXPORT
```

this will generate a file on your local machine that you will be able to import again on a new NucliaDB

## IMPORTING

```
pip install nucliadb_client
nucliadb_import --host=localhost --grpc=8030 --http=8080 --slug=MYKBSLUG --dump=FILE_OR_URL_TO_IMPORT
```

On importing the dump can be an external URL or a local file that will generate the replication

## CONS

As it does not export binarys, it will not export any value file, any preview or any vector from the original source. Its planned to support on the roadmap.
