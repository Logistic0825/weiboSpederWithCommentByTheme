#!/usr/bin/env bash
set -euo pipefail

python3 -m weibo_spider.tools.split_bad_publish_time_jsonl --inputs \
  output_new_medical_generate_AI/all_weibo_with_comments.with_full_text.jsonl \
  output_new_medical_AI/all_weibo_with_comments.with_full_text.jsonl
