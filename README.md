# kb-spine

Shared scaffolding for the four knowledge-base vaults:

- `ai-knowledge-base` — public AI research second brain (auto-ingested).
- `santander-kb` — confidential corporate notes (offline, encrypted).
- `saas-kb` — personal SaaS products (Agentic Banker, NeuroCoach AI, TwinCore, BankMCP Gateway).
- `tech-kb` — reusable technical infrastructure (skills, prompts, LangGraph patterns).

## What lives here

- `tag-vocabulary.md` — single source of truth for the **closed** hierarchical tag list shared across all four vaults.
- `schemas/` — JSON Schemas for frontmatter contracts (`note`, `entity`, `concept`, `daily`).
- `templates/` — Templater templates for new notes.
- `lint/` — `lint_vault.py` (schema + tags + wikilinks) and `pii_scan.py` (email / IBAN / BIC / DNI / NIE / phone).
- `docs/conventions.md` — full conventions reference.

## How vaults consume it

Each vault adds this repo as a git submodule under `_spine/`:

```bash
git submodule add <kb-spine-url> _spine
```

The GH Action of `ai-knowledge-base` (and pre-commit hooks of the other vaults) call `python _spine/lint/lint_vault.py <vault-content-root>` and `python _spine/lint/pii_scan.py <vault-content-root>` before committing.

## Authoring rule

The pipeline (or pre-commit hook) writes only to `auto/` paths inside each vault. Humans write everywhere except `auto/`. This is enforced by convention; lint flags writes outside `auto/` from automated runs.

## Updating

Any change to `tag-vocabulary.md` or schemas is a coordinated update — bump version in the file, push, then `git submodule update --remote` in each vault.
