# KB Conventions

## Folder model (Karpathy LLM Wiki, adapted)

```
<vault-root>/
├── _spine/                ← git submodule → kb-spine (this repo)
├── papers/                ← raw, ingested by pipeline (research vault only)
├── blog-posts/            ← raw, ingested
├── reports/               ← raw, ingested
├── wiki/                  ← human prose, evergreen
├── entities/
│   ├── people/            ← human-curated person notes
│   ├── orgs/
│   ├── labs/
│   ├── models/
│   └── products/
├── concepts/              ← curated concepts (status: evergreen)
│   └── _candidates/       ← LLM-drafted candidates (status: candidate). You promote or delete.
├── daily/                 ← daily notes (auto-augmented)
│   └── _weekly/           ← weekly exec digest (auto-generated)
├── auto/                  ← pipeline-only territory. Humans NEVER edit here.
│   ├── entities/
│   └── concepts/
└── _index/
    └── ingested.json      ← dedup ledger (research vault)
```

## The single invariant

**Pipeline writes only to `auto/` and to the raw folders (`papers/`, `blog-posts/`, `reports/`, `daily/`, `daily/_weekly/`, `concepts/_candidates/`).**

**Humans write everywhere except `auto/`.**

If you ever feel the urge to edit something under `auto/`, you're doing it wrong: edit the corresponding hand-written sibling instead. The hand-written note transcludes the auto file with `![[auto/.../<slug>]]`.

## Frontmatter

Every Markdown file (except `.full.md` raw companions) has YAML frontmatter. The schema is one of:

- `note` — items in `papers/`, `blog-posts/`, `reports/`. Schema in `_spine/schemas/note.schema.json`.
- `entity` — items in `entities/`. Schema in `_spine/schemas/entity.schema.json`.
- `concept` — items in `concepts/`. Schema in `_spine/schemas/concept.schema.json`.

Pre-commit and the GH Action run `python _spine/lint/lint_vault.py <vault>` which validates against the right schema based on path.

## Tags

The closed list lives in `_spine/tag-vocabulary.md`. Hierarchical (`research/agents`, `governance/eu-ai-act`). Two roots are mandatory on every note:

- exactly one `type/*` tag
- exactly one `access/*` tag

Lint rejects any tag not in the vocabulary file.

## Wikilinks

Use bare basenames: `[[andrej-karpathy]]` resolves anywhere in the vault. Use path-style only for `auto/` transclusions: `![[auto/entities/people/andrej-karpathy]]`.

## File naming

- Ingested items: `YYYY-MM-DD-<slug>.md` (and `.full.md` raw companion).
- Entities: `<slug>.md`.
- Concepts: `<slug>.md`.
- Daily: `YYYY-MM-DD.md`.
- Weekly digest: `YYYY-Www.md` (ISO week).
- Wiki: `<slug>.md`.

Slug rule: lowercase, ASCII, hyphens, no leading/trailing hyphen, regex `^[a-z0-9][a-z0-9-]*$`.

## Lint and PII gates

Every commit (manual or via GH Action) runs:

1. `python _spine/lint/lint_vault.py <vault>` — frontmatter, tags, wikilinks. Fails commit on any error.
2. `python _spine/lint/pii_scan.py <vault>` — defense-in-depth scan for emails, IBANs, BICs, DNIs, NIEs, phones, IPs.
   - Public research vault: warn-only.
   - Santander vault: `--strict --config _spine/lint/santander.yml`. Fails commit on any non-allowlisted finding.

## Promoting candidates

Concept candidates and entity stubs live in their `_candidates/` or `auto/` slots until you bless them:

1. Move from `concepts/_candidates/<slug>.md` to `concepts/<slug>.md`.
2. Edit the prose (TL;DR, key claims, open questions).
3. Set `status: evergreen` in frontmatter.
4. Commit.

After this, the pipeline never overwrites the file. Auto-rollups for that concept go into `auto/concepts/<slug>.md` and are transcluded by the evergreen note.
