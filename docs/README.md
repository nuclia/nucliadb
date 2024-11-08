# NucliaDB developer documentation

The purpose of these docs are oriented around NucliaDB development.

If you are getting started using NucliaDB, refer to [getting started](https://docs.nuclia.dev/docs/management/nucliadb/intro),
where you can find basic concepts and information about how to install
and run NucliaDB.

Read the [internal](internal) documentation for information closer to
the code.

Inside [reference](reference) there are documents about different
topics, while inside [tutorials](tutorials) you can find documentation
structured as guides while inside tutorials you can find guides that
will walk you through various NucliaDB features.


## Setup nucliadb for developing

We currently use `pdm` to manage all our dependencies, please, make sure you
have it installed before starting.

`pdm` will automatically create a new virtual environment for you (it defaults
to .venv/). If you want a different virtual environment setup, refer to pdm's
documentation.

Let's install everything with:
``` shell
pdm sync -d -G :all --clean
```

Activate the virtual environment and have fun! You can test the installation by
running some tests:
``` shell
source .venv/bin/activate
make -C nucliadb test-nucliadb
```


### Lint and formatting

We use a strict linting and formattable changes won't be accepted in our CI. Use
the `lint` and `format` targets across our Makefiles to make sure everything is
properly formatted or use:
``` shell
make python-code-lint rust-code-lint
```
for Python and Rust formatting across all our codebase
