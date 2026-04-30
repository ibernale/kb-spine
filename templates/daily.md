---
type: daily
date: <% tp.date.now("YYYY-MM-DD") %>
status: draft
tags: [type/daily, access/internal]
---

# <% tp.date.now("YYYY-MM-DD, dddd") %>

## Ingested today

_(Pipeline appends a list of items ingested in the last 24h, with wikilinks.)_

## Notes / scratch

_(Free-form. Promote anything worth keeping into `wiki/` or `concepts/`.)_

## Open threads

## Related

- Previous: [[<% tp.date.yesterday("YYYY-MM-DD") %>]]
- Next: [[<% tp.date.tomorrow("YYYY-MM-DD") %>]]
