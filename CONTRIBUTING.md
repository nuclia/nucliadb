# Contributing to nucliadb

There are many ways to contribute to nucliadb.
Code contribution are welcome of course, but also
bug reports, feature request, and evangelizing are as valuable.

## Submitting a PR

Check if your issue is already listed [github](https://github.com/nuclia/nucliadb/issues).
If it is not, create your own issue.

Please add the following phrase at the end of your commit. `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
closes the issue. If your PR only partially addresses the issue and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

## Signing the CLA

nucliadb is an opensource project licensed a AGPLv3.
It is also distributed under a commercial license by Bosutech XXI.

Contributors are required to sign a Contributor License Agreement.
The process is simple and fast. Upon your first pull request, you will be prompted to
[sign our CLA by visiting this link](https://cla-assistant.io/nuclia/nucliadb).

## Development

### Setup & run tests NucliaDB

1. Install Python == 3.13 with a virtualenv with your prefered tool (pyenv, conda, pipenv,...)
2. Install NucliaDB Dev Dependencies `make install`
3. Run `pytest nucliadb/tests`
