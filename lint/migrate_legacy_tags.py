#!/usr/bin/env python3
"""
migrate_legacy_tags.py — one-shot migration of pre-spine ingested notes.

Rewrites frontmatter for existing notes under papers/, blog-posts/, reports/:
  - Maps flat tags (agents, rag, ...) to hierarchical (research/agents, ...).
  - Drops unknown tags with a warning.
  - Injects exactly one type/* tag based on the `type:` field.
  - Injects access/public (research vault is public by default).
  - Quotes published / ingested dates so YAML loads them as strings (matching
    the schema). The lint coerces dates anyway, but quoting is the canonical
    form and avoids ambiguity.

Idempotent: re-running on already-migrated files is a no-op.

Usage:
    python migrate_legacy_tags.py <vault-content-root> [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed", file=sys.stderr)
    sys.exit(2)


FLAT_TO_HIER = {
    "policy": "research/regulation",
    **{t: f"research/{t}" for t in (
        "agents", "rag", "evals", "alignment", "safety", "interpretability",
        "moe", "long-context", "reasoning", "multimodal", "robotics",
        "distillation", "pretraining", "posttraining", "inference",
        "hardware", "benchmarks", "agentic-coding", "tool-use", "regulation",
        "economics", "industry", "model-release",
    )},
}

ALLOWED = set(FLAT_TO_HIER.values()) | {
    "governance/eu-ai-act", "governance/dora", "governance/gdpr",
    "governance/eba", "governance/nis2",
}

ITEM_TYPE_TO_TAG = {
    "paper": "type/paper",
    "blog-post": "type/blog",
    "report": "type/report",
    "full-text": None,  # raw companion files: skip
}

FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def normalise(tags: list, item_type: str) -> tuple[list[str], list[str]]:
    out, seen, dropped = [], set(), []
    for t in tags or []:
        if not isinstance(t, str):
            continue
        v = t.strip().lower()
        if "/" not in v:
            v = FLAT_TO_HIER.get(v, v)
        if v in ALLOWED and v not in seen:
            out.append(v)
            seen.add(v)
        elif v not in seen:
            dropped.append(t)
    type_tag = ITEM_TYPE_TO_TAG.get(item_type)
    if type_tag and type_tag not in seen:
        out.append(type_tag)
    if "access/public" not in seen:
        out.append("access/public")
    return out, dropped


def stringify_dates(fm: dict) -> dict:
    for k in ("published", "ingested", "released"):
        v = fm.get(k)
        if isinstance(v, (_dt.date, _dt.datetime)):
            fm[k] = v.isoformat() if isinstance(v, _dt.datetime) else v.isoformat()
    return fm


import json as _json


def _scalar(v: object) -> str:
    """Render a Python scalar as a single-line YAML scalar.

    We always use JSON-style strings (double-quoted, escapes handled) for
    strings — this happens to be valid YAML and avoids ambiguity with dates,
    leading dashes, colons, and unicode."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, (int, float)):
        return str(v)
    return _json.dumps(v, ensure_ascii=False)


def render_frontmatter(fm: dict) -> str:
    order = ["title", "url", "source", "type", "authors", "published", "ingested", "tags"]
    seen_keys: set[str] = set()
    lines = ["---"]
    for k in order:
        if k not in fm:
            continue
        seen_keys.add(k)
        v = fm[k]
        if isinstance(v, list):
            lines.append(f"{k}: [{', '.join(_scalar(x) for x in v)}]")
        else:
            lines.append(f"{k}: {_scalar(v)}")
    for k, v in fm.items():
        if k in seen_keys:
            continue
        if isinstance(v, list):
            lines.append(f"{k}: [{', '.join(_scalar(x) for x in v)}]")
        else:
            lines.append(f"{k}: {_scalar(v)}")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def migrate_file(path: Path, dry: bool) -> tuple[bool, list[str]]:
    text = path.read_text(encoding="utf-8")
    m = FRONTMATTER_RE.match(text)
    if not m:
        return False, []

    fm = yaml.safe_load(m.group(1)) or {}
    if not isinstance(fm, dict):
        return False, []

    original_tags = list(fm.get("tags") or [])
    item_type = fm.get("type", "")

    if item_type == "full-text":
        return False, []  # .full.md raw companions: leave untouched

    new_tags, dropped = normalise(original_tags, item_type)

    if new_tags == original_tags:
        return False, dropped  # nothing to change

    fm["tags"] = new_tags
    fm = stringify_dates(fm)

    new_text = render_frontmatter(fm) + text[m.end() :]
    if not dry:
        path.write_text(new_text, encoding="utf-8")
    return True, dropped


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("root", type=Path)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not args.root.is_dir():
        print(f"ERROR: {args.root} is not a directory", file=sys.stderr)
        return 2

    changed = 0
    scanned = 0
    all_dropped: dict[str, int] = {}
    for f in sorted(args.root.rglob("*.md")):
        if f.name.endswith(".full.md"):
            continue
        scanned += 1
        was_changed, dropped = migrate_file(f, args.dry_run)
        for d in dropped:
            all_dropped[d] = all_dropped.get(d, 0) + 1
        if was_changed:
            changed += 1
            print(f"  migrated: {f.relative_to(args.root)}")

    print(f"\n{'(dry-run) ' if args.dry_run else ''}{changed}/{scanned} files migrated")
    if all_dropped:
        print("\nUnknown tags dropped:")
        for tag, n in sorted(all_dropped.items(), key=lambda kv: -kv[1]):
            print(f"  {tag} ({n})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
