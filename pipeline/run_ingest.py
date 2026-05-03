"""kb-spine generic ingest pipeline (Karpathy LLM Wiki).

Single source of truth for the ingest flow used by ai-knowledge-base,
santander-kb and tech-kb. The flow is identical across vaults; the
domain-specific dimensions live in `pipeline.yml` in the vault root:

  model: claude-opus-4-5            # which Anthropic model to call
  max_tokens: 16000                 # generation cap
  max_web_searches: 50              # web-search budget for the model
  default_access_tag: access/public # injected into every emitted note
  allowed_tags: [...]               # closed list of tags the LLM may emit
  flat_to_hier: {...}               # legacy flat-to-hierarchical rewrites

The vault also brings its own `master_prompt_ingest.md` and `sources.md`,
which run_ingest.py reads from cwd. Output destinations under knowledge/
(papers/, blog-posts/, reports/, _index/ingested.json) are unchanged.

Usage (from a vault root, with pipeline.yml present):
    python _spine/pipeline/run_ingest.py
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import anthropic
import httpx
import yaml

try:
    import fitz  # pymupdf
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False


PIPELINE_CONFIG_PATH = Path("pipeline.yml")
if not PIPELINE_CONFIG_PATH.exists():
    print(
        "ERROR: pipeline.yml not found in cwd. The generic spine pipeline "
        "needs a per-vault config file alongside master_prompt_ingest.md "
        "and sources.md.",
        file=sys.stderr,
    )
    sys.exit(2)
PIPELINE_CONFIG = yaml.safe_load(PIPELINE_CONFIG_PATH.read_text(encoding="utf-8")) or {}

MODEL = PIPELINE_CONFIG.get("model", "claude-opus-4-5")
MAX_TOKENS = PIPELINE_CONFIG.get("max_tokens", 16000)
MAX_WEB_SEARCHES = PIPELINE_CONFIG.get("max_web_searches", 50)
DEFAULT_ACCESS_TAG = PIPELINE_CONFIG.get("default_access_tag", "access/public")

PROMPT_PATH = Path("master_prompt_ingest.md")
SOURCES_PATH = Path("sources.md")
KNOWLEDGE_DIR = Path("knowledge")
INDEX_DIR = KNOWLEDGE_DIR / "_index"
INDEX_FILE = INDEX_DIR / "ingested.json"

KNOWLEDGE_DIR.mkdir(exist_ok=True)
INDEX_DIR.mkdir(exist_ok=True)
for sub in ("papers", "blog-posts", "reports"):
    (KNOWLEDGE_DIR / sub).mkdir(exist_ok=True)

# Realistic browser-like headers improve fetch success on consultancy and
# corporate sites that 403 anything that smells like a script.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/pdf,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}
HTTP_TIMEOUT = 45.0
MIN_PDF_TEXT = 500          # pymupdf < this many chars => probably scanned
MIN_HTML_MD_LEN = 200       # below this, treat html extraction as failed


# --- Index of already-ingested URLs -----------------------------------------

def load_index() -> dict[str, Any]:
    if not INDEX_FILE.exists():
        return {"items": []}
    try:
        return json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        print("WARNING: index file unreadable, starting fresh", file=sys.stderr)
        return {"items": []}


def save_index(index: dict[str, Any]) -> None:
    INDEX_FILE.write_text(
        json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def normalise_url(url: str) -> str:
    url = url.strip().rstrip("/")
    url = re.sub(r"\?utm_[^&]+(&utm_[^&]+)*$", "", url)
    url = re.sub(r"&utm_[^&]+", "", url)
    m = re.match(r"https?://arxiv\.org/(abs|pdf)/(\d+\.\d+)(v\d+)?(\.pdf)?", url)
    if m:
        return f"https://arxiv.org/abs/{m.group(2)}"
    return url


# --- Claude: identify candidates --------------------------------------------

def identify_candidates(prior_urls: list[str]) -> list[dict[str, Any]]:
    system_prompt = PROMPT_PATH.read_text(encoding="utf-8")
    sources_catalog = SOURCES_PATH.read_text(encoding="utf-8") if SOURCES_PATH.exists() else ""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    already_block = "\n".join(f"- {u}" for u in prior_urls[-500:])

    user_message = (
        f"Today's date is {today}.\n\n"
        f"<already_ingested>\n{already_block}\n</already_ingested>\n\n"
        f"<sources_catalog>\n{sources_catalog}\n</sources_catalog>\n\n"
        "Identify items to ingest now. Return ONLY the JSON array."
    )

    client = anthropic.Anthropic()
    print(f"Calling {MODEL} to identify candidates...", flush=True)

    text_chunks: list[str] = []
    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
        tools=[{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": MAX_WEB_SEARCHES,
        }],
    ) as stream:
        for text in stream.text_stream:
            text_chunks.append(text)

    raw = "".join(text_chunks).strip()

    fenced = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    else:
        start = raw.find("[")
        end = raw.rfind("]")
        if start != -1 and end > start:
            raw = raw[start:end + 1]

    try:
        items = json.loads(raw)
        if not isinstance(items, list):
            raise ValueError("Top-level JSON is not a list")
        return items
    except (json.JSONDecodeError, ValueError) as e:
        print(f"ERROR parsing Claude's JSON: {e}", file=sys.stderr)
        print(f"Raw output (first 500 chars): {raw[:500]}", file=sys.stderr)
        return []


# --- Content download and conversion ----------------------------------------

@dataclass
class Fetched:
    markdown: str | None
    note: str


def http_get(url: str, *, accept: str | None = None) -> httpx.Response:
    """Single shared HTTP getter with browser-like headers."""
    headers = dict(DEFAULT_HEADERS)
    if accept:
        headers["Accept"] = accept
    with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, headers=headers) as client:
        return client.get(url)


def extract_pdf_text(pdf_bytes: bytes) -> tuple[str | None, str]:
    """Returns (text, note). text is None if extraction failed or too short."""
    if not HAS_FITZ:
        return None, "no-fitz"
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        text = re.sub(r"\n{3,}", "\n\n", "\n\n".join(pages)).strip()
        if len(text) < MIN_PDF_TEXT:
            return None, "scanned-or-empty-pdf"
        return text, "pdf-extracted"
    except Exception as e:
        return None, f"pdf-failed-{type(e).__name__}"


def extract_html_markdown(html: str) -> tuple[str | None, str]:
    """Returns (markdown, note). markdown is None if extraction failed."""
    if not HAS_TRAFILATURA:
        return None, "no-trafilatura"
    md = trafilatura.extract(
        html,
        output_format="markdown",
        include_links=True,
        include_tables=True,
        include_images=False,
    )
    if not md or len(md) < MIN_HTML_MD_LEN:
        return None, "html-too-short"
    return md, "html-extracted"


def find_pdf_link_in_html(html: str, base_url: str) -> str | None:
    """For consultancy pages: find a link to a PDF if the page is a wrapper.

    Looks for the first plausible PDF link near the top of the page or in
    common 'download' anchors.
    """
    # Direct .pdf links
    pdf_match = re.search(
        r'href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']',
        html,
        re.IGNORECASE,
    )
    if pdf_match:
        return urljoin(base_url, pdf_match.group(1))

    # "download report" / "view PDF" anchors with non-.pdf URLs (some sites obfuscate)
    dl_match = re.search(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*(?:download|view)\s+(?:full\s+)?(?:report|pdf|the\s+report)',
        html,
        re.IGNORECASE,
    )
    if dl_match:
        return urljoin(base_url, dl_match.group(1))

    return None


def fetch_arxiv(arxiv_url: str) -> Fetched:
    """ar5iv first, then PDF fallback."""
    m = re.match(r"https?://arxiv\.org/abs/(\d+\.\d+)", arxiv_url)
    if not m:
        return Fetched(None, "bad-arxiv-url")
    paper_id = m.group(1)

    # Attempt 1: ar5iv HTML
    try:
        resp = http_get(f"https://ar5iv.labs.arxiv.org/html/{paper_id}")
        if resp.status_code == 200:
            md, note = extract_html_markdown(resp.text)
            if md:
                return Fetched(md, "ar5iv")
    except Exception as e:
        print(f"  ar5iv failed: {type(e).__name__}: {e}", file=sys.stderr)

    # Attempt 2: PDF
    try:
        resp = http_get(f"https://arxiv.org/pdf/{paper_id}", accept="application/pdf")
        if resp.status_code != 200:
            return Fetched(None, f"pdf-status-{resp.status_code}")
        text, note = extract_pdf_text(resp.content)
        if text:
            return Fetched(f"# Full text\n\n{text}", note)
        return Fetched(None, note)
    except Exception as e:
        return Fetched(None, f"pdf-failed-{type(e).__name__}")


def fetch_html_or_wrapped_pdf(url: str) -> Fetched:
    """For non-arXiv URLs.

    Logic:
    1. Fetch the URL.
    2. If response is a PDF, extract text from it.
    3. If response is HTML, try markdown extraction.
    4. If markdown extraction yields too little, look inside the HTML for a
       PDF link (consultancy wrapper case) and try that PDF.
    """
    try:
        resp = http_get(url)
    except Exception as e:
        return Fetched(None, f"http-failed-{type(e).__name__}")

    if resp.status_code != 200:
        return Fetched(None, f"http-status-{resp.status_code}")

    content_type = resp.headers.get("content-type", "").lower()

    # Case 1: response IS a PDF
    if "application/pdf" in content_type or url.lower().endswith(".pdf"):
        text, note = extract_pdf_text(resp.content)
        if text:
            return Fetched(f"# Full text\n\n{text}", note)
        return Fetched(None, note)

    # Case 2: HTML — try direct markdown extraction
    md, note = extract_html_markdown(resp.text)
    if md:
        return Fetched(md, "html-extracted")

    # Case 3: HTML extraction too short — likely a wrapper page. Look for a PDF.
    pdf_link = find_pdf_link_in_html(resp.text, url)
    if pdf_link:
        print(f"  HTML wrapper detected — trying {pdf_link}")
        try:
            pdf_resp = http_get(pdf_link, accept="application/pdf")
            if pdf_resp.status_code == 200:
                text, pdf_note = extract_pdf_text(pdf_resp.content)
                if text:
                    return Fetched(f"# Full text\n\n{text}", "wrapped-pdf-extracted")
                return Fetched(None, pdf_note)
        except Exception as e:
            return Fetched(None, f"wrapped-pdf-failed-{type(e).__name__}")

    return Fetched(None, note)  # html-too-short, no wrapped PDF found


def fetch_full_content(item: dict[str, Any]) -> Fetched:
    url = item.get("url", "")
    if "arxiv.org" in url:
        return fetch_arxiv(url)
    return fetch_html_or_wrapped_pdf(url)


# --- Note writing ------------------------------------------------------------

def slugify(text: str, max_length: int = 60) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:max_length].rstrip("-") or "untitled"


def folder_for_type(item_type: str) -> Path:
    mapping = {
        "paper": KNOWLEDGE_DIR / "papers",
        "blog-post": KNOWLEDGE_DIR / "blog-posts",
        "report": KNOWLEDGE_DIR / "reports",
    }
    return mapping.get(item_type, KNOWLEDGE_DIR / "blog-posts")


# Tags the LLM is allowed to emit. Loaded from pipeline.yml so each vault
# (ai-knowledge-base research, santander-kb regulatory, tech-kb engineering)
# can scope down to its domain without forking this file.
ALLOWED_LLM_TAGS = set(PIPELINE_CONFIG.get("allowed_tags", []))
if not ALLOWED_LLM_TAGS:
    print("ERROR: pipeline.yml: allowed_tags is empty or missing.", file=sys.stderr)
    sys.exit(2)

# Legacy flat tags → hierarchical equivalents. Belt-and-suspenders for the
# transition period in case the LLM regresses to flat names.
FLAT_TO_HIER = dict(PIPELINE_CONFIG.get("flat_to_hier", {}))

ITEM_TYPE_TO_TAG = {
    "paper": "type/paper",
    "blog-post": "type/blog",
    "report": "type/report",
}


def normalise_tags(raw_tags: list[str], item_type: str) -> list[str]:
    """Coerce LLM tags into the hierarchical vocabulary, then add the
    structural tags (type/* and access/*) that the pipeline owns."""
    out: list[str] = []
    seen: set[str] = set()
    for tag in raw_tags or []:
        if not isinstance(tag, str):
            continue
        t = tag.strip().lower()
        if "/" not in t:
            t = FLAT_TO_HIER.get(t, t)
        if t in ALLOWED_LLM_TAGS and t not in seen:
            out.append(t)
            seen.add(t)
    type_tag = ITEM_TYPE_TO_TAG.get(item_type, "type/blog")
    if type_tag not in seen:
        out.append(type_tag)
    out.append(DEFAULT_ACCESS_TAG)
    return out


# --- Daily note, entity rollups, weekly digest ------------------------------

DAILY_DIR = KNOWLEDGE_DIR / "daily"
WEEKLY_DIR = DAILY_DIR / "_weekly"
AUTO_DIR = KNOWLEDGE_DIR / "auto"
AUTO_PEOPLE = AUTO_DIR / "entities" / "people"
AUTO_ORGS = AUTO_DIR / "entities" / "orgs"
AUTO_CONCEPTS = AUTO_DIR / "concepts"

# Author-slug exclusions: literal author strings that never become a person rollup.
EXCLUDED_AUTHORS = {"et al.", "et al", "anonymous", ""}

# Token markers that flag a "person" string as actually an organisation,
# institution or publication. If any of these appear as a whitespace-separated
# token in the author name, the entry is routed to the org rollup instead.
ORG_AUTHOR_TOKENS = {
    "university", "universidad", "institute", "institut", "college",
    "lab", "labs", "research", "team", "ai", "inc", "corp",
    "ltd", "llc", "ag", "gmbh", "sa", "co", "company", "group",
    "foundation", "deepmind", "openai", "anthropic", "meta", "microsoft",
    "google", "apple", "nvidia", "huggingface", "mistral",
}

# How many recent items to surface in entity rollups (None = all).
ENTITY_ROLLUP_MAX = 100

# Thresholds for generating a rollup at all.
PERSON_ROLLUP_MIN_ITEMS = 2
ORG_ROLLUP_MIN_ITEMS = 3
CONCEPT_ROLLUP_MIN_ITEMS = 3
# Tag namespaces eligible for concept rollups (full history, not the
# 7-day candidate window). type/*, access/*, workflow/* are structural and
# never become concepts.
CONCEPT_ROLLUP_NAMESPACES = ("research/", "governance/", "domain/")


def begin_marker(name: str) -> str:
    return f"<!-- BEGIN AUTO:{name} -->"


def end_marker(name: str) -> str:
    return f"<!-- END AUTO:{name} -->"


def update_managed_section(path: Path, name: str, new_block: str) -> bool:
    """Replace the content between <!-- BEGIN AUTO:name --> and the matching
    end marker. If markers are missing, do nothing and emit a warning. The
    rest of the file is preserved verbatim."""
    text = path.read_text(encoding="utf-8")
    bm, em = begin_marker(name), end_marker(name)
    bi, ei = text.find(bm), text.find(em)
    if bi == -1 or ei == -1 or ei < bi:
        print(
            f"WARNING: managed-section markers '{name}' missing in {path}; skipping",
            file=sys.stderr,
        )
        return False
    new_text = text[: bi + len(bm)] + "\n" + new_block.strip() + "\n" + text[ei:]
    if new_text == text:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def render_daily_block(items: list[dict[str, Any]]) -> str:
    if not items:
        return "## Ingested today\n\n_None._"
    by_type: dict[str, list[dict[str, Any]]] = {}
    for it in items:
        by_type.setdefault(it.get("type", "blog-post"), []).append(it)
    label_for = {"paper": "Papers", "blog-post": "Blog posts", "report": "Reports"}
    lines = [f"## Ingested today ({len(items)})", ""]
    for k in ("paper", "blog-post", "report"):
        bucket = by_type.get(k, [])
        if not bucket:
            continue
        lines.append(f"### {label_for[k]}")
        for it in bucket:
            authors = it.get("authors") or []
            tail = ""
            if authors and authors != ["et al."]:
                tail = f" — _{', '.join(authors[:3])}_"
            lines.append(f"- [[{it['slug']}]] — {it.get('title', '')}{tail}")
        lines.append("")
    return "\n".join(lines).strip()


def write_daily_note(items_today: list[dict[str, Any]]) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"{today}.md"
    if not path.exists():
        stub = (
            "---\n"
            f'type: "daily"\n'
            f'date: "{today}"\n'
            'tags: ["type/daily", "access/public"]\n'
            "---\n\n"
            f"# {today}\n\n"
            f"{begin_marker('ingested')}\n"
            f"{end_marker('ingested')}\n\n"
            "## Notes / scratch\n\n"
            "_(Free-form. Promote anything worth keeping into wiki/ or concepts/.)_\n"
        )
        path.write_text(stub, encoding="utf-8")
    update_managed_section(path, "ingested", render_daily_block(items_today))
    return path


# --- Entity rollups ---------------------------------------------------------

FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def _coerce_dates(v: Any) -> Any:
    """Stringify YAML-native date / datetime values so downstream code can
    sort or format them without mixing types. Recursive."""
    if isinstance(v, _dt.datetime):
        return v.isoformat()
    if isinstance(v, _dt.date):
        return v.isoformat()
    if isinstance(v, list):
        return [_coerce_dates(x) for x in v]
    if isinstance(v, dict):
        return {k: _coerce_dates(x) for k, x in v.items()}
    return v


def parse_frontmatter(text: str) -> dict[str, Any] | None:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return None
    try:
        fm = yaml.safe_load(m.group(1)) or {}
        if not isinstance(fm, dict):
            return None
        return _coerce_dates(fm)
    except yaml.YAMLError:
        return None


def load_short_notes() -> dict[str, dict[str, Any]]:
    """Walk papers/, blog-posts/, reports/ and return url -> frontmatter+meta."""
    notes: dict[str, dict[str, Any]] = {}
    for sub in ("papers", "blog-posts", "reports"):
        d = KNOWLEDGE_DIR / sub
        if not d.exists():
            continue
        for p in d.glob("*.md"):
            if p.name.endswith(".full.md"):
                continue
            fm = parse_frontmatter(p.read_text(encoding="utf-8"))
            if not fm:
                continue
            url = fm.get("url")
            if url:
                notes[url] = {**fm, "_path": str(p), "_slug": p.stem}
    return notes


def slugify_person(name: str) -> str | None:
    """Returns a slug for a real person, or None if the name looks like an
    org / institution / publication / placeholder."""
    if not name:
        return None
    raw = name.strip()
    if raw.lower() in EXCLUDED_AUTHORS:
        return None
    tokens_lower = re.split(r"\s+", raw.lower())
    # Single-token "names" are almost always orgs or handles (Anthropic, OpenAI).
    if len(tokens_lower) < 2:
        return None
    # Any org/institution token disqualifies the whole name.
    if any(tok in ORG_AUTHOR_TOKENS for tok in tokens_lower):
        return None
    s = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s).strip().lower()
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s or None


def render_person_rollup(slug: str, name: str, items: list[dict[str, Any]]) -> str:
    items_sorted = sorted(items, key=lambda x: x.get("ingested", ""), reverse=True)
    if ENTITY_ROLLUP_MAX:
        items_sorted = items_sorted[:ENTITY_ROLLUP_MAX]
    lines = [
        "---",
        'type: "entity"',
        'subtype: "person"',
        f'name: "{name}"',
        f'slug: "{slug}"',
        'status: "auto"',
        'tags: ["type/entity-person", "access/public"]',
        "---",
        "",
        f"# {name} — pipeline rollup",
        "",
        "> Auto-generated by `run_ingest.py`. Do not edit. Hand-written sibling at",
        f"> `entities/people/{slug}.md` should transclude this with `![[auto/entities/people/{slug}]]`.",
        "",
        f"## Recent items ({len(items_sorted)})",
        "",
    ]
    for it in items_sorted:
        date = it.get("ingested", "")
        item_type = it.get("type", "blog-post")
        title = it.get("title", "").replace('"', "'")
        lines.append(f"- {date} — [[{it['slug']}]] _({item_type})_ — {title}")
    return "\n".join(lines) + "\n"


def render_org_rollup(source: str, items: list[dict[str, Any]]) -> str:
    items_sorted = sorted(items, key=lambda x: x.get("ingested", ""), reverse=True)
    if ENTITY_ROLLUP_MAX:
        items_sorted = items_sorted[:ENTITY_ROLLUP_MAX]
    lines = [
        "---",
        'type: "entity"',
        'subtype: "org"',
        f'name: "{source}"',
        f'slug: "{source}"',
        'status: "auto"',
        'tags: ["type/entity-org", "access/public"]',
        "---",
        "",
        f"# {source} — pipeline rollup",
        "",
        "> Auto-generated by `run_ingest.py`. Do not edit. Hand-written sibling at",
        f"> `entities/orgs/{source}.md` should transclude this with `![[auto/entities/orgs/{source}]]`.",
        "",
        f"## Recent items ({len(items_sorted)})",
        "",
    ]
    for it in items_sorted:
        date = it.get("ingested", "")
        item_type = it.get("type", "blog-post")
        title = it.get("title", "").replace('"', "'")
        lines.append(f"- {date} — [[{it['slug']}]] _({item_type})_ — {title}")
    return "\n".join(lines) + "\n"


def update_entity_rollups() -> tuple[int, int]:
    """Walk all short notes, group by person and source, write rollups."""
    notes = load_short_notes()
    by_person: dict[str, dict[str, Any]] = {}
    by_source: dict[str, list[dict[str, Any]]] = {}

    for url, note in notes.items():
        meta = {
            "url": url,
            "slug": note.get("_slug", ""),
            "title": note.get("title", ""),
            "type": note.get("type", "blog-post"),
            "ingested": note.get("ingested", ""),
            "published": note.get("published", ""),
        }
        for author in (note.get("authors") or []):
            slug = slugify_person(author)
            if not slug:
                continue
            entry = by_person.setdefault(slug, {"name": author, "items": []})
            entry["items"].append(meta)
        src = note.get("source")
        if src and src != "other":
            by_source.setdefault(src, []).append(meta)

    AUTO_PEOPLE.mkdir(parents=True, exist_ok=True)
    AUTO_ORGS.mkdir(parents=True, exist_ok=True)

    # auto/ is pipeline-owned and full-rewrite. Wipe stale rollups so an
    # entity that no longer meets the threshold (or that turns out to be an
    # org masquerading as a person) doesn't linger.
    for d in (AUTO_PEOPLE, AUTO_ORGS):
        for f in d.glob("*.md"):
            f.unlink()

    people_written = 0
    for slug, payload in by_person.items():
        if len(payload["items"]) < PERSON_ROLLUP_MIN_ITEMS:
            continue
        path = AUTO_PEOPLE / f"{slug}.md"
        path.write_text(render_person_rollup(slug, payload["name"], payload["items"]), encoding="utf-8")
        people_written += 1

    orgs_written = 0
    for src, items in by_source.items():
        if len(items) < ORG_ROLLUP_MIN_ITEMS:
            continue
        path = AUTO_ORGS / f"{src}.md"
        path.write_text(render_org_rollup(src, items), encoding="utf-8")
        orgs_written += 1

    return people_written, orgs_written


# --- Concept rollups (auto/concepts/*) --------------------------------------
# These are full-history rollups per tag, intended to be transcluded by
# human-written wiki/<topic>.md notes via ![[auto/concepts/<slug>]].

def render_concept_rollup(tag: str, items: list[dict[str, Any]]) -> str:
    name = friendly_name_from_tag(tag)
    slug = slug_from_tag(tag)
    items_sorted = sorted(items, key=lambda x: str(x.get("ingested", "")), reverse=True)
    if ENTITY_ROLLUP_MAX:
        items_sorted = items_sorted[:ENTITY_ROLLUP_MAX]
    lines = [
        "---",
        'type: "concept"',
        f'name: "{name}"',
        f'slug: "{slug}"',
        f'source_tag: "{tag}"',
        'status: "auto"',
        'tags: ["type/concept", "access/public"]',
        "---",
        "",
        f"# {name} — pipeline rollup",
        "",
        "> Auto-generated by `run_ingest.py` on every run. Do not edit.",
        f"> Hand-written sibling at `concepts/{slug}.md` or a wiki note in `wiki/`",
        f"> can transclude this with `![[auto/concepts/{slug}]]` to surface the",
        "> latest items tagged with this concept.",
        "",
        f"Source tag: `{tag}` — {len(items_sorted)} item{'s' if len(items_sorted) != 1 else ''} (most recent first).",
        "",
    ]
    for it in items_sorted:
        date = str(it.get("ingested", ""))
        item_type = it.get("type", "blog-post")
        title = (it.get("title") or "").replace('"', "'")
        lines.append(f"- {date} — [[{it['_slug']}]] _({item_type})_ — {title}")
    return "\n".join(lines) + "\n"


def update_concept_rollups() -> int:
    """Cluster ALL ingested items by concept tag (full history) and write
    auto/concepts/<slug>.md rollups. Distinct from concepts/_candidates/
    which are 7-day window-only and meant for human promotion."""
    notes = load_short_notes()
    by_tag: dict[str, list[dict[str, Any]]] = {}

    for url, note in notes.items():
        meta = {
            "url": url,
            "_slug": note.get("_slug", ""),
            "title": note.get("title", ""),
            "type": note.get("type", "blog-post"),
            "ingested": note.get("ingested", ""),
        }
        for tag in note.get("tags") or []:
            if any(tag.startswith(ns) for ns in CONCEPT_ROLLUP_NAMESPACES):
                by_tag.setdefault(tag, []).append(meta)

    AUTO_CONCEPTS.mkdir(parents=True, exist_ok=True)

    # Wipe stale rollups — full rewrite, like entity rollups.
    for f in AUTO_CONCEPTS.glob("*.md"):
        f.unlink()

    written = 0
    for tag, items in by_tag.items():
        if len(items) < CONCEPT_ROLLUP_MIN_ITEMS:
            continue
        slug = slug_from_tag(tag)
        path = AUTO_CONCEPTS / f"{slug}.md"
        path.write_text(render_concept_rollup(tag, items), encoding="utf-8")
        written += 1

    return written


# --- Concept candidates -----------------------------------------------------

CONCEPTS_DIR = KNOWLEDGE_DIR / "concepts"
CANDIDATES_DIR = CONCEPTS_DIR / "_candidates"

# A tag must reach this many items in the window to become a candidate.
CANDIDATE_MIN_ITEMS = 3
CANDIDATE_WINDOW_DAYS = 7

# Tag namespaces eligible for concept clustering. type/*, access/*, workflow/*
# are structural and never become concepts.
CANDIDATE_TAG_NAMESPACES = ("research/", "governance/", "domain/")


def friendly_name_from_tag(tag: str) -> str:
    """research/agentic-coding -> 'Agentic coding'."""
    leaf = tag.split("/", 1)[-1]
    return leaf.replace("-", " ").capitalize()


def slug_from_tag(tag: str) -> str:
    """research/agentic-coding -> 'research-agentic-coding'. Stable across runs."""
    return tag.replace("/", "-").lower()


def render_concept_candidate(
    tag: str,
    items: list[dict[str, Any]],
    co_occurring: list[tuple[str, int]],
) -> str:
    name = friendly_name_from_tag(tag)
    slug = slug_from_tag(tag)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items_sorted = sorted(items, key=lambda x: str(x.get("ingested", "")), reverse=True)

    lines = [
        "---",
        'type: "concept"',
        f'name: "{name}"',
        f'slug: "{slug}"',
        f'source_tag: "{tag}"',
        'status: "candidate"',
        'tags: ["type/concept", "workflow/candidate", "access/public"]',
        f'first_seen: "{today}"',
        f'last_updated: "{today}"',
        "linked_papers: []",
        "linked_entities: []",
        "---",
        "",
        f"# {name} — concept candidate",
        "",
        f"> **Auto-clustered** from items tagged `{tag}` ingested in the last {CANDIDATE_WINDOW_DAYS} days.",
        ">",
        "> **Promote**: move this file to `concepts/<slug>.md`, edit (TL;DR, key claims, open questions),",
        '> set `status: evergreen`. The pipeline never overwrites a promoted concept.',
        ">",
        "> **Discard**: delete this file. The pipeline regenerates next Sunday if the tag stays hot.",
        "",
        f"## Items in the window ({len(items_sorted)})",
        "",
    ]
    for it in items_sorted:
        date = str(it.get("ingested", ""))
        item_type = it.get("type", "blog-post")
        title = (it.get("title") or "").replace('"', "'")
        lines.append(f"- {date} — [[{it['_slug']}]] _({item_type})_ — {title}")
    lines.append("")

    if co_occurring:
        lines.append("## Co-occurring tags")
        lines.append("")
        for co_tag, count in co_occurring:
            lines.append(f"- `{co_tag}` ({count} item{'s' if count != 1 else ''})")
        lines.append("")
    else:
        lines.append("## Co-occurring tags")
        lines.append("")
        lines.append("_No notable co-occurrences in the window._")
        lines.append("")

    return "\n".join(lines)


def update_concept_candidates() -> tuple[int, int]:
    """Cluster items in the last N days by tag and write candidates.
    Returns (written, skipped_already_promoted)."""
    notes = load_short_notes()
    week_items = items_for_last_n_days(notes, days=CANDIDATE_WINDOW_DAYS)

    by_tag: dict[str, list[dict[str, Any]]] = {}
    for it in week_items:
        for tag in it.get("tags") or []:
            if any(tag.startswith(ns) for ns in CANDIDATE_TAG_NAMESPACES):
                by_tag.setdefault(tag, []).append(it)

    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)

    # Wipe stale candidates — full rewrite, like auto/.
    for f in CANDIDATES_DIR.glob("*.md"):
        f.unlink()

    written = 0
    skipped = 0
    for tag, items in by_tag.items():
        if len(items) < CANDIDATE_MIN_ITEMS:
            continue
        slug = slug_from_tag(tag)
        # Don't shadow a promoted concept.
        if (CONCEPTS_DIR / f"{slug}.md").exists():
            skipped += 1
            continue

        # Compute co-occurring tags within this bucket (only tags from the
        # eligible namespaces, excluding the focal tag itself).
        co_counts: dict[str, int] = {}
        for it in items:
            for t in it.get("tags") or []:
                if t == tag:
                    continue
                if any(t.startswith(ns) for ns in CANDIDATE_TAG_NAMESPACES):
                    co_counts[t] = co_counts.get(t, 0) + 1
        co_occurring = sorted(co_counts.items(), key=lambda kv: -kv[1])[:5]

        path = CANDIDATES_DIR / f"{slug}.md"
        path.write_text(render_concept_candidate(tag, items, co_occurring), encoding="utf-8")
        written += 1

    return written, skipped


# --- Weekly exec digest -----------------------------------------------------

WEEKLY_DIGEST_PROMPT = """\
You are an executive curator producing a weekly AI research digest for a
senior reader who already saw the daily ingest. Goal: 8-12 bullets, each
one line + wikilink, prioritised by relevance for someone running AI at a
large bank.

Priority order (top first):
1. Frontier-lab releases or major model launches.
2. Tier-4 individual researcher posts (Karpathy, Lambert, Raschka, etc.).
3. Tier-5a digest items that captured social traction.
4. Qualifying arXiv papers (only the ones with clear practitioner value).
5. Anything else.

Return ONLY a JSON object with these fields:
{
  "headline": "≤ 14 words capturing the week's defining theme",
  "items": [
    { "slug": "<file-stem-of-the-short-note>", "one_line": "≤ 18 words explaining why this matters" }
  ],
  "trending_tags": ["research/...", ...],
  "watchlist": ["<slug or short string>", ...]
}

Constraints:
- Slugs must come from the input list verbatim (do not invent).
- one_line is your synthesis, not a copy of the why_it_matters paragraph.
- Skip items that turned out to be duplicates or low-quality on reflection.
"""


def items_for_last_n_days(notes: dict[str, dict[str, Any]], days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc).date() - _dt.timedelta(days=days)
    out: list[dict[str, Any]] = []
    for url, note in notes.items():
        ing = note.get("ingested", "")
        try:
            d = _dt.date.fromisoformat(ing)
        except ValueError:
            continue
        if d >= cutoff:
            out.append({**note, "_url": url})
    return out


def write_weekly_digest(client: anthropic.Anthropic, dry_run: bool = False) -> Path | None:
    notes = load_short_notes()
    week_items = items_for_last_n_days(notes, days=7)
    if not week_items:
        print("No items in the last 7 days; skipping weekly digest.")
        return None

    # Compact payload for Claude — frontmatter + why_it_matters paragraph only.
    compact: list[dict[str, Any]] = []
    for n in week_items:
        path = Path(n.get("_path", ""))
        why = ""
        if path.exists():
            text = path.read_text(encoding="utf-8")
            m = re.search(r"##\s+Why it matters\s*\n+(.+?)(?=\n##|\Z)", text, re.DOTALL)
            if m:
                why = m.group(1).strip()[:600]
        compact.append({
            "slug": n.get("_slug"),
            "type": n.get("type"),
            "source": n.get("source"),
            "title": n.get("title"),
            "authors": n.get("authors"),
            "tags": n.get("tags"),
            "ingested": n.get("ingested"),
            "why_it_matters": why,
        })

    today = datetime.now(timezone.utc)
    iso_year, iso_week, _ = today.isocalendar()
    week_label = f"{iso_year}-W{iso_week:02d}"

    user_msg = (
        f"Today is {today.strftime('%Y-%m-%d')}. Produce the digest for week {week_label}.\n\n"
        f"Items ingested in the last 7 days ({len(compact)}):\n\n"
        + json.dumps(compact, ensure_ascii=False)
    )

    print(f"Calling {MODEL} for weekly digest of {len(compact)} items...", flush=True)
    text_chunks: list[str] = []
    with client.messages.stream(
        model=MODEL,
        max_tokens=4000,
        system=WEEKLY_DIGEST_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    ) as stream:
        for text in stream.text_stream:
            text_chunks.append(text)
    raw = "".join(text_chunks).strip()

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    else:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            raw = raw[start:end + 1]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR parsing digest JSON: {e}", file=sys.stderr)
        return None

    return _persist_weekly_digest(data, valid_slugs={n["slug"] for n in compact}, week_label=week_label, today=today, dry_run=dry_run)


def render_weekly_digest_markdown(
    data: dict[str, Any],
    valid_slugs: set[str],
    week_label: str,
    today: datetime,
) -> str:
    """Pure rendering: takes parsed LLM JSON and returns the digest markdown.

    valid_slugs is the set of slugs that appeared in the input — items
    referencing any other slug are dropped (defence against slug invention)."""
    items_clean = [it for it in data.get("items", []) if it.get("slug") in valid_slugs]

    lines = [
        "---",
        'type: "weekly-digest"',
        f'week: "{week_label}"',
        f'generated: "{today.strftime("%Y-%m-%d")}"',
        'tags: ["type/weekly-digest", "access/internal"]',
        "---",
        "",
        f"# Week {week_label} — Exec digest",
        "",
        f"> {(data.get('headline') or '').strip()}",
        "",
        f"## Top {len(items_clean)} of the week",
        "",
    ]
    for i, it in enumerate(items_clean, 1):
        lines.append(f"{i}. [[{it['slug']}]] — {(it.get('one_line') or '').strip()}")
    lines.append("")

    trending = data.get("trending_tags") or []
    if trending:
        lines.append("## What's quietly trending")
        lines.append("")
        for t in trending:
            lines.append(f"- `{t}`")
        lines.append("")

    watchlist = data.get("watchlist") or []
    if watchlist:
        lines.append("## Watchlist")
        lines.append("")
        for w in watchlist:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines)


def _persist_weekly_digest(
    data: dict[str, Any],
    valid_slugs: set[str],
    week_label: str,
    today: datetime,
    dry_run: bool,
) -> Path:
    out = render_weekly_digest_markdown(data, valid_slugs, week_label, today)
    WEEKLY_DIR.mkdir(parents=True, exist_ok=True)
    path = WEEKLY_DIR / f"{week_label}.md"
    if dry_run:
        print(f"--- DRY RUN — would write {path} ---\n{out}")
        return path
    path.write_text(out, encoding="utf-8")
    print(f"Wrote weekly digest: {path}")
    return path


# --- Original write_note ----------------------------------------------------


def write_note(item: dict[str, Any], full_content: Fetched) -> tuple[Path, Path | None]:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    slug = slugify(item.get("title", "untitled"))
    folder = folder_for_type(item.get("type", "blog-post"))

    short_path = folder / f"{today}-{slug}.md"
    full_path: Path | None = None

    fm_lines = ["---"]
    fm_lines.append(f"title: {json.dumps(item.get('title', ''))}")
    fm_lines.append(f"url: {item.get('url', '')}")
    fm_lines.append(f"source: {item.get('source', 'other')}")
    fm_lines.append(f"type: {item.get('type', 'blog-post')}")
    authors = item.get("authors") or []
    fm_lines.append(f"authors: {json.dumps(authors, ensure_ascii=False)}")
    fm_lines.append(f'published: "{item.get("published_date", "")}"')
    fm_lines.append(f'ingested: "{today}"')
    tags = normalise_tags(item.get("tags") or [], item.get("type", "blog-post"))
    fm_lines.append(f"tags: {json.dumps(tags)}")
    fm_lines.append("---\n")
    frontmatter = "\n".join(fm_lines)

    if full_content.markdown:
        full_path = folder / f"{today}-{slug}.full.md"
        full_fm = (
            "---\n"
            f"title: {json.dumps(item.get('title', '') + ' (full text)')}\n"
            f"url: {item.get('url', '')}\n"
            f"source: {item.get('source', 'other')}\n"
            f"type: full-text\n"
            f"parent: \"[[{short_path.stem}]]\"\n"
            f"ingested: {today}\n"
            f"extraction: {full_content.note}\n"
            "---\n\n"
        )
        full_path.write_text(full_fm + full_content.markdown, encoding="utf-8")

    body = [frontmatter, f"# {item.get('title', 'Untitled')}\n"]
    body.append("## Why it matters")
    body.append(item.get("why_it_matters", "_Not provided_"))
    body.append("")

    abstract = item.get("abstract_or_lede")
    if abstract:
        label = "Abstract" if item.get("type") == "paper" else "Lede"
        body.append(f"## {label} (original)")
        body.append(abstract)
        body.append("")

    body.append("## Source")
    body.append(f"[{item.get('url', '')}]({item.get('url', '')})")
    body.append("")

    if full_path is not None:
        body.append("## Full text")
        body.append(f"[[{full_path.stem}]] (extracted: {full_content.note})")
    else:
        body.append("## Full text")
        body.append(f"_Not extracted: {full_content.note}_")

    short_path.write_text("\n".join(body), encoding="utf-8")
    return short_path, full_path


# --- Main --------------------------------------------------------------------

PHASE_INGEST = "ingest"
PHASE_DAILY = "daily"
PHASE_ENTITIES = "entities"
PHASE_CANDIDATES = "candidates"
PHASE_WEEKLY = "weekly"
ALL_PHASES = (PHASE_INGEST, PHASE_DAILY, PHASE_ENTITIES, PHASE_CANDIDATES, PHASE_WEEKLY)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--phase",
        default=",".join(ALL_PHASES),
        help=f"comma-separated phases to run (default: all). Choices: {', '.join(ALL_PHASES)}.",
    )
    p.add_argument("--force-weekly", action="store_true", help="run weekly digest even if today is not Sunday")
    p.add_argument("--dry-run-weekly", action="store_true", help="print weekly digest instead of writing")
    return p.parse_args()


def run_ingest_phase() -> tuple[int, list[dict[str, Any]]]:
    """Returns (ingested_count, items_written_today). Each item dict has
    slug, type, title, authors, source — enough to render the daily note."""
    if not PROMPT_PATH.exists():
        print(f"ERROR: prompt file not found: {PROMPT_PATH}", file=sys.stderr)
        return 0, []
    if not HAS_TRAFILATURA:
        print("ERROR: trafilatura is required (pip install trafilatura)", file=sys.stderr)
        return 0, []

    index = load_index()
    prior_urls = {normalise_url(item["url"]) for item in index.get("items", [])}
    print(f"Index loaded: {len(prior_urls)} prior URLs.")

    candidates = identify_candidates(sorted(prior_urls))
    print(f"Claude returned {len(candidates)} candidates.")

    if not candidates:
        print("Nothing to ingest today.")
        return 0, []

    items_written_today: list[dict[str, Any]] = []
    extraction_summary: dict[str, int] = {}
    for item in candidates:
        url = normalise_url(item.get("url", ""))
        if not url:
            print("SKIP: missing url")
            continue
        if url in prior_urls:
            print(f"SKIP (already in index): {url}")
            continue

        print(f"\nIngesting: {item.get('title', '?')[:80]}")
        print(f"  URL: {url}")
        try:
            fetched = fetch_full_content({**item, "url": url})
        except Exception as e:
            fetched = Fetched(None, f"unhandled-{type(e).__name__}")

        try:
            short_path, full_path = write_note({**item, "url": url}, fetched)
            print(f"  Wrote: {short_path}")
            if full_path:
                print(f"  Wrote: {full_path} (extraction: {fetched.note})")
            else:
                print(f"  No full content — extraction: {fetched.note}")
        except Exception as e:
            print(f"  ERROR writing note: {e}", file=sys.stderr)
            continue

        items_written_today.append({
            "slug": short_path.stem,
            "type": item.get("type", "blog-post"),
            "title": item.get("title", ""),
            "authors": item.get("authors") or [],
            "source": item.get("source", "other"),
        })
        index["items"].append({
            "url": url,
            "title": item.get("title", ""),
            "ingested": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "extraction": fetched.note,
        })
        prior_urls.add(url)
        extraction_summary[fetched.note] = extraction_summary.get(fetched.note, 0) + 1

    save_index(index)
    print(f"\n=== Ingested {len(items_written_today)} new items today ===")
    if extraction_summary:
        print("Extraction breakdown:")
        for note, count in sorted(extraction_summary.items()):
            print(f"  {note}: {count}")
    return len(items_written_today), items_written_today


def main() -> int:
    args = parse_args()
    phases = {p.strip() for p in args.phase.split(",") if p.strip()}
    invalid = phases - set(ALL_PHASES)
    if invalid:
        print(f"ERROR: unknown phases: {invalid}", file=sys.stderr)
        return 2

    items_today: list[dict[str, Any]] = []

    if PHASE_INGEST in phases:
        if not SOURCES_PATH.exists():
            print(f"WARNING: sources catalog not found: {SOURCES_PATH}", file=sys.stderr)
        _, items_today = run_ingest_phase()

    if PHASE_DAILY in phases:
        path = write_daily_note(items_today)
        print(f"Daily note: {path} ({len(items_today)} items added today)")

    if PHASE_ENTITIES in phases:
        people, orgs = update_entity_rollups()
        print(f"Entity rollups: {people} people, {orgs} orgs")
        concept_rollups = update_concept_rollups()
        print(f"Concept rollups (auto/concepts/): {concept_rollups} tags")

    if PHASE_CANDIDATES in phases:
        written, skipped = update_concept_candidates()
        print(f"Concept candidates: {written} written, {skipped} skipped (already promoted)")

    if PHASE_WEEKLY in phases:
        is_sunday = datetime.now(timezone.utc).weekday() == 6
        if is_sunday or args.force_weekly:
            # Weekly digest is the only phase that calls Claude; treat any
            # API failure as a soft warning so the rest of the run (items
            # already on disk, rollups, candidates) gets committed.
            try:
                client = anthropic.Anthropic()
                write_weekly_digest(client, dry_run=args.dry_run_weekly)
            except Exception as e:
                print(
                    f"WARNING: weekly digest failed ({type(e).__name__}: {e}). "
                    f"Continuing — items and rollups already written are preserved.",
                    file=sys.stderr,
                )
            # Concept candidates also belong to the weekly cadence — refresh
            # them here even if --phase explicitly listed only weekly.
            if PHASE_CANDIDATES not in phases:
                w, s = update_concept_candidates()
                print(f"Concept candidates (bundled with weekly): {w} written, {s} skipped")
        else:
            print("Skipping weekly digest (not Sunday; pass --force-weekly to override).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
