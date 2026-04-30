<%*
const name = await tp.system.prompt("Full name");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: entity
subtype: person
name: "<% name %>"
slug: <% slug %>
aliases: []
affiliations: []
primary_writing: ""
x_handle: ""
github: ""
tier: 4
status: draft
tags: [type/entity-person, access/public]
first_seen: <% today %>
last_updated: <% today %>
---

# <% name %>

## Why they matter

_(One paragraph: who they are, why their writing is high-signal.)_

## Threads I follow

- _(Topics they cover that I care about.)_

## Related

- See pipeline-managed rollup: ![[auto/entities/people/<% slug %>]]
