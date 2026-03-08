#!/usr/bin/env bash
set -euo pipefail

INPUT=${1:-output_new_medical_AI_seen/all_weibo_with_comments.jsonl}
OUTPUT=${2:-output_new_medical_AI_seen/all_weibo_with_comments.with_full_text.jsonl}
COOKIE=${3:-}

if [ -n "$COOKIE" ]; then
  python3 -m weibo_spider.tools.enrich_full_text_jsonl --input "$INPUT" --output "$OUTPUT" --cookie "$COOKIE"
else
  python3 -m weibo_spider.tools.enrich_full_text_jsonl --input "$INPUT" --output "$OUTPUT"
fi
