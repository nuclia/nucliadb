---
title: Telemetry
position: 5
---

Nuclia Inc. collects anonymous data regarding general usage to help us drive our development. Privacy and transparency are at the heart of Nuclia values and we only collect the minimal useful data and don't use any third party tool for the collection.

## Disabling data collection

Data collection are opt-out. To disable them, just set the environment variable `NUCLIADB_DISABLE_TELEMETRY` to whatever value.

```bash
export NUCLIADB_DISABLE_TELEMETRY=1
```

Look at `nucliadb help` command output to check whether telemetry is enabled or not:

```bash
nucliadb help
nucliadb 0.1.0
nucliadb, Inc. <info@nuclia.com>
Indexing your large dataset on object storage & making it searchable from the command line.
Telemetry enabled
```

The line `Telemetry enabled` disappears when you disable it.

## Which data is collected?

We collect the minimum amount of information to respect privacy. Here are the data collected:

- type of events among create, index, delete and serve events
- client information:
  - session uuid: uuid generated on the fly
  - nucliadb version
  - os (linux, macos, freebsd, android...)
  - architecture of the CPU
  - md5 hash of host and username
  - a boolean to know if `KUBERNETES_SERVICE_HOST` is set.

All data are sent to `telemetry.nuclia.com`.

## No third party

We did not want to add any untrusted third party tool in the collection so we decided to implement and host our own metric collection server.
