<%*
const name = await tp.system.prompt("Product name");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: entity
subtype: product
name: "<% name %>"
slug: <% slug %>
producer: ""
status: draft
tags: [type/entity-product, access/internal]
first_seen: <% today %>
last_updated: <% today %>
---

# <% name %>

## What it is

## Why it matters here

## Status

## Related

![[auto/entities/products/<% slug %>]]
