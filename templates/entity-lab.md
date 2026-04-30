<%*
const name = await tp.system.prompt("Lab name");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: entity
subtype: lab
name: "<% name %>"
slug: <% slug %>
parent_org: ""
status: draft
tags: [type/entity-lab, access/public]
first_seen: <% today %>
last_updated: <% today %>
---

# <% name %>

## Focus areas

## Why it's high signal

## Related

![[auto/entities/labs/<% slug %>]]
