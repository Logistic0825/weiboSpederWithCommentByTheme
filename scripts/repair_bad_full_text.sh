#!/usr/bin/env bash
set -euo pipefail

COOKIE=${1:-}
FILES=(
  "output_new_medical_generate_AI/all_weibo_with_comments.with_full_text.jsonl"
  "output_new_medical_AI/all_weibo_with_comments.with_full_text.jsonl"
)

if [ -n "$COOKIE" ]; then
  python3 -m weibo_spider.tools.repair_bad_full_text_jsonl --inputs "${FILES[@]}" --cookie "$COOKIE"
else
  python3 -m weibo_spider.tools.repair_bad_full_text_jsonl --inputs "${FILES[@]}"
fi
