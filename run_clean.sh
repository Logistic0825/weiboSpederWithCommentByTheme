#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "${SCRIPT_DIR}/clean_weibo.py" \
  --dataset "Logistic12/weiboDataWithCommentByTheme" \
  --output "output/cleaned_weibo.jsonl" \
  --metrics "output/clean_metrics.json" \
  --streaming "1" \
  --batch-size 200 \
  --print-every 100 \
  --min-effective-len 3 \
  --max-len-truncate 500 \
  --max-len-drop 1000 \
  --near-dup-sim 0.9 \
  --drop-sensitive "true" \
  --ai-enable "true" \
  --ai-on-sensitive-only "true" \
  --ai-sample-rate 1.0
