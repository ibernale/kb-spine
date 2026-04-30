<%*
const name = await tp.system.prompt("Org name");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: entity
subtype: org
name: "<% name %>"
slug: <% slug %>
domain: ""
country: ""
type_of: ""
status: draft
tags: [type/entity-org, access/public]
first_seen: <% today %>
last_updated: <% today %>
---

# <% name %>

## What they do

_(2-3 sentences.)_

## Why they're in this KB

## Notable threads

## Related

![[auto/entities/orgs/<% slug %>]]
