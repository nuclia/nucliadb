# Basic concepts

## What's a Knowledge Box?

A Knowledge Box is a logical container where you will save data of any
kind.i These data is divided into resources.

A resource is similar to a file but much more powerful. It is usually
created from a document, but it contain a lot more, as fields,
metadata or extracted information.

A field represents some kind of knowledge. It has a type and has been
designed to store such kind of information efficiently. Currently,
NucliaDB supports this field types:
- file
- link
- text
- layout
- conversation
- keywordset
- datetime

A resource can contain an undefined number of fields of each type.

From the API point of view, you will find 3 levels corresponding to:
Knowledge Box, Resource and Field.
