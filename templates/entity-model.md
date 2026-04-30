<%*
const name = await tp.system.prompt("Model name (e.g. Claude Opus 4.7)");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: entity
subtype: model
name: "<% name %>"
slug: <% slug %>
producer: ""
modality: [text]
released: <% today %>
context_window: 0
status: draft
tags: [type/entity-model, research/model-release, access/public]
first_seen: <% today %>
last_updated: <% today %>
---

# <% name %>

## What's new

## Capabilities

## Trade-offs

## Related

![[auto/entities/models/<% slug %>]]
