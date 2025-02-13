# Building NucliaDB

NucliaDB is run primarily through a container and built with Dockerfiles.

NucliaDB has 2 build types. 1: python based components and 2: Index node that is rust.

The Python based components also depend on various packaging requirements:
- required monorepo dependency packages
- nucliadb rust bindings


Types of Dockerfiles:
- Dockerfile: primary python component build that doesn't include rust bindings (to deploy in cluster mode with node services)
- Dockerfile.withbinding: python component build that includes rust bindings that allow standalone serving
- Dockerfile.node: rust build for our Index Nodes
- Dockerfile.node_prebuilt: allows building the node with prebuilt rust binaries


# CI/CD

Our CI/CD system is not currently very optimal for building container images.


## Rust build speed

Our rust based components take a very long to build in the github action build system--sometimes as much as 60 minutes.
To work around that, we build the binaries outside docker using rust caching and then copy those into the docker image.
