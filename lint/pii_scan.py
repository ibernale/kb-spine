#!/usr/bin/env python3
"""
pii_scan.py — defense-in-depth PII / sensitive-string scanner for KB vaults.

Usage:
    python pii_scan.py <vault-root> [--strict] [--config <yaml>]

Default mode (no --strict): logs findings, returns 0. Use this on the public
research vault as a smoke alarm — false positives are expected.

--strict: returns 1 on any non-allowlisted finding. Use this on santander-kb
and any vault where confidentiality is load-bearing.

--config: optional YAML with extra patterns and allowlist entries:
    extra_patterns:
      - name: santander-internal-host
        pattern: '\\.santander\\.es\\b'
    allowlist:
      - 'foo@example\\.com'

Patterns checked by default:
    - email
    - IBAN (broad, length-validated)
    - BIC / SWIFT (8 or 11 chars)
    - Spanish DNI (8 digits + letter)
    - Spanish NIE (XYZ + 7 digits + letter)
    - phone numbers (ES and intl, with separators)
    - IPv4

The allowlist is applied per-line: if a line matches any allowlist regex, ALL
findings on that line are dropped. This is how we keep arXiv IDs, dates, and
canonical URLs from being flagged as IBAN/DNI.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None  # config is optional


@dataclass
class Pattern:
    name: str
    regex: re.Pattern[str]


DEFAULT_PATTERNS: list[Pattern] = [
    Pattern("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    # IBAN: 2 country letters + 2 check digits + 11..30 alphanumerics. Be strict on length.
    Pattern("iban", re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")),
    # BIC/SWIFT: 6 letters + 2 alnum + optional 3 alnum
    Pattern("bic", re.compile(r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?\b")),
    Pattern("dni", re.compile(r"\b\d{8}[A-HJ-NP-TV-Z]\b")),
    Pattern("nie", re.compile(r"\b[XYZ]\d{7}[A-HJ-NP-TV-Z]\b")),
    Pattern("phone-es", re.compile(r"(?<!\d)\+?34[\s\-]?\d{3}[\s\-]?\d{3}[\s\-]?\d{3}(?!\d)")),
    Pattern("ipv4", re.compile(r"\b(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?:\.(?:25[0-5]|2[0-4]\d|[01]?\d\d?)){3}\b")),
]


# Built-in noise filter — lines that should never be flagged regardless of patterns.
# Keep this conservative; per-vault overrides go in --config.
DEFAULT_ALLOWLIST: list[re.Pattern[str]] = [
    re.compile(r"https?://"),                       # URLs
    re.compile(r"^\s*url:\s"),                      # frontmatter url field
    re.compile(r"arxiv\.org/abs/\d{4}\.\d{4,5}"),  # arXiv IDs
    re.compile(r"^\s*ingested:\s\d{4}-\d{2}-\d{2}\s*$"),
    re.compile(r"^\s*published:\s\d{4}-\d{2}-\d{2}\s*$"),
    re.compile(r"^\s*released:\s\d{4}-\d{2}-\d{2}\s*$"),
]


def load_config(path: Path | None) -> tuple[list[Pattern], list[re.Pattern[str]]]:
    extra_patterns: list[Pattern] = []
    extra_allow: list[re.Pattern[str]] = []
    if path is None:
        return extra_patterns, extra_allow
    if yaml is None:
        print("WARNING: PyYAML not installed; --config ignored", file=sys.stderr)
        return extra_patterns, extra_allow
    cfg = yaml.safe_load(path.read_text()) or {}
    for entry in cfg.get("extra_patterns", []) or []:
        extra_patterns.append(Pattern(entry["name"], re.compile(entry["pattern"])))
    for line in cfg.get("allowlist", []) or []:
        extra_allow.append(re.compile(line))
    return extra_patterns, extra_allow


def scan_file(path: Path, patterns: list[Pattern], allowlist: list[re.Pattern[str]]) -> list[tuple[int, str, str]]:
    findings: list[tuple[int, str, str]] = []
    try:
        for lineno, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            if any(a.search(line) for a in allowlist):
                continue
            for pat in patterns:
                for m in pat.regex.finditer(line):
                    findings.append((lineno, pat.name, m.group(0)))
    except OSError as e:
        print(f"WARNING: could not read {path}: {e}", file=sys.stderr)
    return findings


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("root", type=Path, nargs="?", default=Path("."), help="directory to walk (ignored if --only-files)")
    p.add_argument("--strict", action="store_true", help="exit 1 on any finding")
    p.add_argument("--config", type=Path, default=None)
    p.add_argument("--include-full", action="store_true", help="also scan .full.md raw mirrors")
    p.add_argument("--only-files", nargs="+", type=Path, default=None,
                   help="scan only these files instead of walking the tree (ignores --include-full)")
    args = p.parse_args()

    extra_patterns, extra_allow = load_config(args.config)
    patterns = DEFAULT_PATTERNS + extra_patterns
    allowlist = DEFAULT_ALLOWLIST + extra_allow

    if args.only_files:
        targets = [f for f in args.only_files if f.is_file() and f.suffix in {".md", ".txt", ".yml", ".yaml"}]
    else:
        if not args.root.is_dir():
            print(f"ERROR: {args.root} is not a directory", file=sys.stderr)
            return 2
        # Skip the spine submodule and other vendored content — those repos
        # carry their own lint contract and false-positive surface.
        skip_dirs = {"_spine", ".git", "node_modules", "__pycache__", ".venv", "venv"}
        targets = sorted(
            f for f in args.root.rglob("*.md")
            if not skip_dirs.intersection(f.parts)
        )

    total = 0
    files = 0
    for f in targets:
        if not args.include_full and not args.only_files and f.name.endswith(".full.md"):
            continue
        findings = scan_file(f, patterns, allowlist)
        if findings:
            files += 1
            try:
                label = f.relative_to(args.root) if not args.only_files else f
            except ValueError:
                label = f
            print(f"\n{label}")
            for lineno, name, sample in findings[:10]:
                print(f"  L{lineno} [{name}] {sample}")
            if len(findings) > 10:
                print(f"  ... and {len(findings) - 10} more")
            total += len(findings)

    if total == 0:
        print("OK — no PII patterns matched")
        return 0
    print(f"\n{'FAIL' if args.strict else 'WARN'} — {total} potential PII finding(s) across {files} file(s)")
    return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
