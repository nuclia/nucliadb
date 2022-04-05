# nucliadb Key Blob Store

The prefix for storing binary data on nucliadb are:

- `/kbs/{kbid}/`
  - `/r/` Scan all resources
    - `{rid}`
      - `/files/{field}`
        - `/extracted`
        - `/thumbnail`
        - `/preview`
        - `/largemetadata`
        - `/metadata`
        - `/rows`
        - `/pages`
          - `/{page}`
      - `/links/{link}`
        - `/extracted`
        - `/thumbnail`
        - `/preview`
        - `/largemetadata`
        - `/metadata`
      - `/simple/{field}`
        - `/extracted`
        - `/thumbnail`
      - `/conversation/{field}`
        - `/extracted`
        - `/thumbnail`
      - `/formatedtext/{field}`
        - `/extracted`
        - `/thumbnail`
      - `/blocktext/{field}`
        - `/extracted`
        - `/thumbnail`
