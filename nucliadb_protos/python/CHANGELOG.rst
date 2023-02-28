CHANGELOG
=========

0.0.52
------
- Make sure py.typed is on the package

0.0.51
------
- Add ShardMetadata on shard creation

0.0.50
------
- Fix trace_id type

0.0.49
------
- Extend AuditRequest with client type and trace_id

0.0.48
------
- Extend audit with counter data

0.0.47
------
- Add `shard_count` attribute in `Member`

0.0.46
------
- Turn member type into an enumeration

0.0.45
------
- Add protos to moving and accepting shards

0.0.44
------
- Add protos for upload and download

0.0.43
------
- Add load score value in cluster member

0.0.42
------

- Add advanced_query to search protos

0.0.41
------

- Add new ComputedMetadata on resource basic

0.0.40
------

- Update train sentence

0.0.39
------

- Vectorsets

0.0.38
------

- Add cancelled_by_user to Classification proto

0.0.37
------

- Add processing queue type info

0.0.36
------

- Add reindex_vectors option to IndexResource request

0.0.35
------

- Add SetVectors servicer at ingest

0.0.34
------

- Add account_seq to BrokerMessage
- Add last_account_seq to Basic

0.0.33
------

- Add new servicer at node writer to clean and upgrade a shard

0.0.32
------

- Add extra metadata with paragraph position on IndexParagraph proto

0.0.31
------

- Add positions on NER

0.0.30
------

- Export values optional
- Add last_seqid to Basic

0.0.29
------

- Pin GRPC version


0.0.28 (2022-09-05)
-------------------

- Extend audit to provide insight of how many fields have changed and it's file fields storage movements


0.0.27 (2022-08-03)
-------------------

- Paragraph can be from a title


0.0.26 (2022-08-02)
-------------------

- Train interface


0.0.25 (2022-06-29)
-------------------

- Add optional text on paragraph resource


0.0.24 (2022-05-11)
-------------------

- Message source and processing id added to broker message


0.0.23 (2022-04-26)
-------------------

- Breaking change: Vectors are now a list of floats


0.0.22 (2022-04-13)
-------------------

- Processed audit log


0.0.21 (2022-04-07)
-------------------

- Add audit log proto


0.0.20 (2022-03-30)
-------------------

- Score may be a field int or BM25 Float


0.0.19 (2022-03-08)
-------------------

- Adding field type on the output


0.0.18 (2022-03-08)
-------------------

- Adding errors on Broker Message


0.0.17 (2022-02-23)
-------------------

- Include requirements on package


0.0.16 (2022-02-23)
-------------------

- Nothing changed yet.


0.0.15 (2022-02-23)
-------------------

- Clean atributes


0.0.14 (2022-02-07)
-------------------

- Search API


0.0.13 (2022-01-13)
-------------------

- Oritin txseqid


0.0.12 (2022-01-05)
-------------------

- Add uuid as optional parameter on KB creation


0.0.11 (2021-12-27)
-------------------

- Nested position page


0.0.10 (2021-12-19)
-------------------

- Large metadata split


0.0.9 (2021-12-19)
------------------

- Add on links the embed and type of link
- Add Type of Paragraph


0.0.8 (2021-12-17)
------------------

- Add description on links.


0.0.7 (2021-12-17)
------------------

- Link image field


0.0.6 (2021-12-16)
------------------

- Nothing changed yet.


0.0.5 (2021-12-16)
------------------

- Page and image positions
  [bloodbare]

0.0.4 (2021-12-16)
------------------

- Add cell rows on spreadsheet
  [bloodbare]

0.0.3 (2021-12-16)
------------------

- Add icon on file extracted data
  [bloodabre]


0.0.2 (2021-12-02)
------------------

- Add PYI files for typing


0.0.1 (2021-12-01)
------------------

- Initial Version
