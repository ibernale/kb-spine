<%*
const title = await tp.system.prompt("Wiki note title");
const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const today = tp.date.now("YYYY-MM-DD");
await tp.file.rename(slug);
-%>
---
type: wiki
title: "<% title %>"
slug: <% slug %>
status: draft
tags: [type/wiki, access/public]
first_seen: <% today %>
last_updated: <% today %>
---

# <% title %>

_(Free-form synthesis. Link liberally to entities, concepts, raw items.)_
