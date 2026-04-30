<%*
const name = await tp.system.prompt("Concept name (e.g. Mixture of Experts)");
const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: concept
name: "<% name %>"
slug: <% slug %>
aliases: []
status: draft
tags: [type/concept, access/public]
first_seen: <% today %>
last_updated: <% today %>
linked_entities: []
linked_papers: []
---

# <% name %>

## TL;DR

_(One paragraph: what this is, why a practitioner cares.)_

## Key claims I trust

- _(Each bullet should cite a wikilink to a paper or post in raw.)_

## Open questions

## Related concepts

## Pipeline-managed companion

![[auto/concepts/<% slug %>]]
