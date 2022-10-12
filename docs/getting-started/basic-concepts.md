# Basic concepts

## What's a Knowledge Box?

A Knowledge Box is a logical container where you will save data of any
kind. This data is divided into resources.

A resource is similar to a file but much more powerful. It is usually
created from a document, but it may contain a lot more than the
binary. For example: multiple fields of various types (layouts,
conversations, etc), metadata or embeddings.

A field represents some kind of knowledge. It has a type and has been
designed to store such kind of information efficiently. Currently,
NucliaDB supports these field types:
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
