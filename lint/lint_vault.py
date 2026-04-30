#!/usr/bin/env python3
"""
lint_vault.py — validate a knowledge-base vault against the kb-spine contracts.

Usage:
    python lint_vault.py <vault-content-root> [--strict-wikilinks] [--max-issues N]

Checks:
    - Frontmatter parses as YAML.
    - Frontmatter validates against the right JSON Schema, picked from the path:
        */papers/*.md, */blog-posts/*.md, */reports/*.md     -> note.schema.json
        */entities/*/*.md (NOT under auto/)                  -> entity.schema.json
        */concepts/*.md, */concepts/_candidates/*.md         -> concept.schema.json
    - Every tag is in tag-vocabulary.md (closed list).
    - Every note has exactly one type/* and one access/* tag.
    - Wikilinks point to existing files (basename-resolved). Companion .full files allowed.
    - .full.md companions are skipped (raw mirrors of source content).
    - Anything under auto/ is skipped (managed by pipeline; lint elsewhere).

Exit code:
    0 if no issues, 1 if any issue found, 2 on internal error.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. pip install -r lint/requirements.txt", file=sys.stderr)
    sys.exit(2)

try:
    from jsonschema import Draft202012Validator
except ImportError:
    print("ERROR: jsonschema not installed. pip install -r lint/requirements.txt", file=sys.stderr)
    sys.exit(2)


SPINE_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = SPINE_ROOT / "schemas"
TAG_VOCAB_FILE = SPINE_ROOT / "tag-vocabulary.md"


def load_schema(name: str) -> dict[str, Any]:
    return json.loads((SCHEMA_DIR / name).read_text())


def load_tag_vocabulary() -> set[str]:
    text = TAG_VOCAB_FILE.read_text()
    tags: set[str] = set()
    in_block = False
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("- `") and "`" in s[3:]:
            tag = s[3 : s.index("`", 3)]
            if "/" in tag:
                tags.add(tag)
    if not tags:
        raise RuntimeError(f"no tags parsed from {TAG_VOCAB_FILE}")
    return tags


FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)
WIKILINK_RE = re.compile(r"\[\[([^\]\|#]+)(?:#[^\]\|]*)?(?:\|[^\]]*)?\]\]")


def parse_frontmatter(text: str) -> tuple[dict[str, Any] | None, str]:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return None, text
    raw = m.group(1)
    try:
        fm = yaml.safe_load(raw) or {}
        if not isinstance(fm, dict):
            return None, text
        return fm, text[m.end() :]
    except yaml.YAMLError:
        return None, text


def schema_for_path(p: Path) -> str | None:
    parts = set(p.parts)
    if "auto" in parts:
        return None
    if any(seg in parts for seg in ("papers", "blog-posts", "reports")):
        return "note"
    if "entities" in parts:
        return "entity"
    if "concepts" in parts:
        return "concept"
    if "wiki" in parts:
        return "wiki"
    if "daily" in parts:
        return "daily"
    return None


def required_tag_roles(tags: list[str]) -> list[str]:
    issues = []
    type_tags = [t for t in tags if t.startswith("type/")]
    access_tags = [t for t in tags if t.startswith("access/")]
    if len(type_tags) != 1:
        issues.append(f"must have exactly one type/* tag, got {type_tags}")
    if len(access_tags) != 1:
        issues.append(f"must have exactly one access/* tag, got {access_tags}")
    return issues


def index_vault_basenames(root: Path) -> dict[str, list[Path]]:
    idx: dict[str, list[Path]] = {}
    for p in root.rglob("*.md"):
        idx.setdefault(p.stem, []).append(p)
        idx.setdefault(p.name, []).append(p)
    return idx


def check_wikilinks(body: str, idx: dict[str, list[Path]], strict: bool) -> list[str]:
    issues: list[str] = []
    for m in WIKILINK_RE.finditer(body):
        target = m.group(1).strip()
        # accept path-style wikilinks like auto/entities/people/foo
        cand = target.split("/")[-1]
        if cand in idx or f"{cand}.md" in idx:
            continue
        if not strict:
            continue
        issues.append(f"unresolved wikilink: [[{target}]]")
    return issues


def lint_file(p: Path, schemas: dict[str, dict], vocab: set[str], idx: dict[str, list[Path]], strict_wikilinks: bool) -> list[str]:
    if p.name.endswith(".full.md"):
        return []  # raw companion files are not curated
    text = p.read_text(encoding="utf-8", errors="replace")
    fm, body = parse_frontmatter(text)
    issues: list[str] = []

    schema_name = schema_for_path(p)
    if schema_name in {"note", "entity", "concept"}:
        if fm is None:
            issues.append("missing or unparsable YAML frontmatter")
            return issues
        validator = Draft202012Validator(schemas[schema_name])
        for err in sorted(validator.iter_errors(fm), key=lambda e: e.path):
            loc = "/".join(str(x) for x in err.path) or "<root>"
            issues.append(f"{schema_name} schema: {loc} — {err.message}")

    if fm and "tags" in fm and isinstance(fm["tags"], list):
        for tag in fm["tags"]:
            if tag not in vocab:
                issues.append(f"unknown tag: {tag!r}")
        if schema_name in {"note", "entity", "concept", "wiki", "daily"}:
            issues.extend(required_tag_roles(fm["tags"]))

    issues.extend(check_wikilinks(body, idx, strict_wikilinks))
    return issues


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    strict = "--strict-wikilinks" in sys.argv
    max_issues = 9999
    for i, a in enumerate(sys.argv):
        if a == "--max-issues" and i + 1 < len(sys.argv):
            max_issues = int(sys.argv[i + 1])

    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory", file=sys.stderr)
        return 2

    schemas = {
        "note": load_schema("note.schema.json"),
        "entity": load_schema("entity.schema.json"),
        "concept": load_schema("concept.schema.json"),
    }
    vocab = load_tag_vocabulary()
    idx = index_vault_basenames(root)

    total = 0
    files_with_issues = 0
    for p in sorted(root.rglob("*.md")):
        # Skip files under auto/
        if "auto" in p.relative_to(root).parts:
            continue
        issues = lint_file(p, schemas, vocab, idx, strict)
        if issues:
            files_with_issues += 1
            print(f"\n{p.relative_to(root)}")
            for i in issues[:20]:
                print(f"  - {i}")
            if len(issues) > 20:
                print(f"  ... and {len(issues) - 20} more")
            total += len(issues)
            if total >= max_issues:
                print(f"\n[stopped at {max_issues} issues]", file=sys.stderr)
                break

    if total == 0:
        print(f"OK — {len(idx)} basenames indexed, no issues")
        return 0
    print(f"\nFAIL — {total} issue(s) across {files_with_issues} file(s)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
