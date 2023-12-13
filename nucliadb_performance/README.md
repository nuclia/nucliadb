# NucliaDB Performance Tests

Home for the performance / benchmarks tests logic

## Running the tests

First, you need to install the package
```
python3 -m venv venv
source venv/bin/activate

make install
```

Then start a standalone NucliaDB locally in another terminal, for instance:
```bash
DEBUG=true venv/bin/nucliadb
```
You can change the way NucliaDB is started if you want to test other drivers (pg, tikv, etc).

And finally run the tests:
```bash
make run
```
The tests are run with Molotov.

You can change the duration of the test and the number of molotov workers started with:
```bash
make run kb_slug=tiny max_workers=4 duration_s=20
```

Also, the tests can be run against three different sizes of predefined KBs: `tiny`, `small` or `medium`. The contents of these KBs are downloaded from a Google Cloud bucket (`https://storage.googleapis.com/nucliadb_indexer/nucliadb_performance/exports/$(kb_slug).export`) and then imported to the standalone NucliaDB before the tests are run.
