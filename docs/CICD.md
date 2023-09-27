# Building NucliaDB

NucliaDB is run primarily through a container and built with Dockerfiles.

NucliaDB has 2 build types. 1: python based components and 2: Index node that is rust.

The Python based components also depend on various packaging requirements:
- required monorepo dependency packages
- nucliadb rust bindings


Types of Dockerfiles:
- Dockerfile: primary python component build that includes rust bindings that allow standalone serving
- Dockerfile.withbinding: build python component along with rust bindings, all in one dockerfile
- Dockerfile.node: rust build for our Index Nodes
- Dockerfile.node_local: allows building the node locally
- Dockerfile.node_prebuilt: allows building the node with prebuilt rust binaries
- Dockerfile.basenode: used as a base in other containers


# CI/CD

Our CI/CD system is not currently very optimal for building container images.


## Python based build duplication

We currently build many images for our python components when it only needs to be 1 image build.

Duplicate builds:
- search
- writer
- reader
- ingest
- train


## Rust build speed

Our rust based components take a very long to build in the github action build system--sometimes as much as 60 minutes.


### Rust build server(experimental)

There is currently initial support for a rust build server. It is only integrated in a few places to validate stability
before we ramp up everywhere.