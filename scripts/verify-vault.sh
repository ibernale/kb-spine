#!/usr/bin/env bash
# verify-vault.sh — quick local sanity check after a GH-Action ingest run.
#
# Usage:
#   verify-vault.sh [VAULT_PATH] [DATE]
#
# Defaults:
#   VAULT_PATH = /Users/ibernal/Projects/ai-knowledge-base
#   DATE       = today (UTC)
#
# Checks the most recent workflow run, the day's daily note, items
# ingested by date, the entity-rollup diff, and runs lint + PII scan.

set -uo pipefail

VAULT_PATH="${1:-/Users/ibernal/Projects/ai-knowledge-base}"
DATE="${2:-$(date -u +%Y-%m-%d)}"
SPINE_PATH="${VAULT_PATH}/_spine"
PYTHON_VENV="/Users/ibernal/Projects/kb-spine/.venv/bin/python"

if [ ! -d "$VAULT_PATH" ]; then
  echo "ERROR: vault not found at $VAULT_PATH" >&2
  exit 2
fi
if [ ! -x "$PYTHON_VENV" ]; then
  echo "ERROR: lint venv not found at $PYTHON_VENV" >&2
  echo "       run: python3 -m venv /Users/ibernal/Projects/kb-spine/.venv && \\
                  /Users/ibernal/Projects/kb-spine/.venv/bin/pip install -r /Users/ibernal/Projects/kb-spine/lint/requirements.txt" >&2
  exit 2
fi

cd "$VAULT_PATH"

echo "=================================================="
echo " Vault verification — $VAULT_PATH"
echo " Date: $DATE"
echo "=================================================="
echo

# ---- 1. Pull main + submodule ---------------------------------------------
echo "[1/6] git pull main + submodule update"
git checkout -q main
git pull --ff-only --quiet origin main
git submodule update --quiet --init --recursive
echo "     HEAD = $(git rev-parse --short HEAD) ($(git log -1 --pretty=%s | head -c 80))"
echo

# ---- 2. Last workflow run -------------------------------------------------
echo "[2/6] Last 3 ingest workflow runs"
gh run list --workflow=ingest.yml --limit=3 --json conclusion,createdAt,displayTitle,url \
  --template '{{range .}}     {{.conclusion}}  {{.createdAt}}  {{.displayTitle}}{{"\n"}}{{end}}' \
  || echo "     (could not list runs — gh auth?)"
LAST_RUN_CONCLUSION=$(gh run list --workflow=ingest.yml --limit=1 --json conclusion --jq '.[0].conclusion' 2>/dev/null || echo unknown)
LAST_RUN_URL=$(gh run list --workflow=ingest.yml --limit=1 --json url --jq '.[0].url' 2>/dev/null || echo "")
echo

# ---- 3. Daily note --------------------------------------------------------
echo "[3/6] Daily note for $DATE"
DAILY="knowledge/daily/${DATE}.md"
if [ ! -f "$DAILY" ]; then
  DAILY_STATE="missing"
  echo "     MISSING — $DAILY does not exist"
elif grep -q '_None\._' "$DAILY"; then
  DAILY_STATE="empty"
  echo "     EMPTY — managed section says _None._"
else
  DAILY_STATE="ok"
  ITEM_COUNT_IN_DAILY=$(awk '/<!-- BEGIN AUTO:ingested -->/,/<!-- END AUTO:ingested -->/' "$DAILY" | grep -c '^- \[\[')
  echo "     OK — managed section has $ITEM_COUNT_IN_DAILY wikilinks"
fi
echo

# ---- 4. Items ingested today ---------------------------------------------
echo "[4/6] Items with ingested: \"$DATE\""
count_dated() {
  local dir="$1"
  [ -d "$dir" ] || { echo 0; return; }
  find "$dir" -maxdepth 1 -type f -name '*.md' ! -name '*.full.md' \
    -exec grep -l "^ingested: \"$DATE\"" {} \; 2>/dev/null | wc -l | tr -d ' '
}
PAPERS=$(count_dated knowledge/papers)
BLOGS=$(count_dated knowledge/blog-posts)
REPORTS=$(count_dated knowledge/reports)
TOTAL=$((PAPERS + BLOGS + REPORTS))
echo "     papers: $PAPERS, blog-posts: $BLOGS, reports: $REPORTS  (total: $TOTAL)"
echo

# ---- 5. Entity rollup diff (last commit) ---------------------------------
echo "[5/6] Entity rollup churn in HEAD"
ROLLUP_DIFF=$(git diff --name-status HEAD~1 HEAD -- knowledge/auto/entities/ 2>/dev/null || true)
if [ -z "$ROLLUP_DIFF" ]; then
  echo "     no entity rollup changes in HEAD"
else
  echo "$ROLLUP_DIFF" | sed 's/^/     /'
fi
echo

# ---- 6. Lint + PII --------------------------------------------------------
echo "[6/6] Lint full vault"
if "$PYTHON_VENV" "$SPINE_PATH/lint/lint_vault.py" knowledge >/tmp/_lint.out 2>&1; then
  LINT_STATE="clean"
  tail -1 /tmp/_lint.out | sed 's/^/     /'
else
  LINT_STATE="fail"
  echo "     LINT ISSUES:"
  cat /tmp/_lint.out | sed 's/^/     /'
fi
echo
echo "     PII scan (warn-only):"
"$PYTHON_VENV" "$SPINE_PATH/lint/pii_scan.py" knowledge 2>&1 | tail -1 | sed 's/^/     /'
PII_STATE=$([ "$("$PYTHON_VENV" "$SPINE_PATH/lint/pii_scan.py" knowledge 2>&1 | tail -1)" = "OK — no PII patterns matched" ] && echo clean || echo warn)
echo

# ---- Summary --------------------------------------------------------------
echo "=================================================="
echo " RESUMEN"
echo "=================================================="

OVERALL="OK"
[ "$LAST_RUN_CONCLUSION" != "success" ] && OVERALL="FAIL"
[ "$DAILY_STATE" = "missing" ] && OVERALL="FAIL"
[ "$LINT_STATE" = "fail" ] && OVERALL="FAIL"
[ "$DAILY_STATE" = "empty" ] && [ "$OVERALL" = "OK" ] && OVERALL="WARN"
[ "$TOTAL" -eq 0 ] && [ "$OVERALL" = "OK" ] && OVERALL="WARN"

cat <<SUMMARY
- Estado: $OVERALL
- Workflow: $LAST_RUN_CONCLUSION ($LAST_RUN_URL)
- Items ingresados: $TOTAL ($PAPERS papers, $BLOGS blog-posts, $REPORTS reports)
- Daily note $DATE: $DAILY_STATE
- Lint: $LINT_STATE
- PII: $PII_STATE
SUMMARY

[ "$OVERALL" = "FAIL" ] && exit 1
[ "$OVERALL" = "WARN" ] && exit 0
exit 0
