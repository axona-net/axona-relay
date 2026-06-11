#!/bin/bash
# Off-machine backup of the PoW benchmark history. The collected file is
# append-only + cumulative (every snapshot is the FULL history), so we only need
# the latest: gzip it and force-push a single-commit branch — keeps the backup
# repo tiny and always current. Run by cron every few hours + on demand.
set -e
SRC=/Users/croqueteer/Documents/claude/axona-relay/pow-results.jsonl
DIR=/Users/croqueteer/Documents/claude/pow-data-backup
[ -f "$SRC" ] || { echo "no source file at $SRC"; exit 1; }
[ -d "$DIR/.git" ] || { echo "backup repo not set up at $DIR (run the one-time setup first)"; exit 1; }
cd "$DIR"
gzip -c "$SRC" > pow-results.jsonl.gz
lines=$(wc -l < "$SRC" | tr -d ' ')
stamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
git add pow-results.jsonl.gz
# single-commit history (amend + force-push) so the repo stays small; latest snapshot is a superset of all prior
git commit --amend -m "pow snapshot: $lines results @ $stamp" >/dev/null 2>&1 || git commit -m "pow snapshot: $lines results @ $stamp" >/dev/null 2>&1
git push -f origin HEAD:main >/dev/null 2>&1
echo "✓ backed up $lines results ($(du -h pow-results.jsonl.gz | cut -f1) gz) @ $stamp → $(git remote get-url origin)"
