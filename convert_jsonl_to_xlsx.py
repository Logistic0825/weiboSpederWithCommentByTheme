import json
import os
import sys
from typing import Any, Dict, List

try:
    from openpyxl import Workbook  # type: ignore
except Exception:
    Workbook = None  # type: ignore


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def safe_get(d: Dict[str, Any], path: List[str], default: Any = "") -> Any:
    cur: Any = d
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return default
    return cur if cur is not None else default


def export_xlsx(input_jsonl: str, output_xlsx: str) -> None:
    if Workbook is None:
        raise RuntimeError("缺少 openpyxl，请先安装：pip install openpyxl")
    os.makedirs(os.path.dirname(output_xlsx), exist_ok=True)
    wb = Workbook()
    ws_articles = wb.active
    ws_articles.title = "articles"
    ws_comments = wb.create_sheet("comments")

    ws_articles.append(
        [
            "keyword",
            "weibo_id",
            "text",
            "author",
            "up",
            "re",
            "cm",
            "comments_count",
            "cleaned_comments_count",
        ]
    )
    ws_comments.append(
        [
            # article fields
            "keyword",
            "weibo_id",
            "article_text",
            "author",
            "up",
            "re",
            "cm",
            # comment fields
            "original_post_id",
            "comment_id",
            "user",
            "content",
            "likes",
            # flags from AI/clean
            "ai_ad_spam",
            "ai_meaningless",
            "ai_sensitive",
            "machine_water_army",
            "truncated",
            "sensitive_hits",
        ]
    )

    rows = read_jsonl(input_jsonl)
    for r in rows:
        details = r.get("weibo_details") or {}
        stats = details.get("stats") or {}
        top_comments = r.get("top_comments") or []
        cleaned_comments = r.get("cleaned_top_comments") or top_comments

        ws_articles.append(
            [
                r.get("keyword", ""),
                details.get("id", ""),
                details.get("text", ""),
                details.get("author", ""),
                safe_get(stats, ["up"], 0),
                safe_get(stats, ["re"], 0),
                safe_get(stats, ["cm"], 0),
                len(top_comments),
                len(cleaned_comments),
            ]
        )

        for c in cleaned_comments:
            flags = c.get("_flags") or {}
            ai = flags.get("ai") or {}
            hits = flags.get("sensitive_hits") or []
            ws_comments.append(
                [
                    # article fields
                    r.get("keyword", ""),
                    details.get("id", ""),
                    details.get("text", ""),
                    details.get("author", ""),
                    safe_get(stats, ["up"], 0),
                    safe_get(stats, ["re"], 0),
                    safe_get(stats, ["cm"], 0),
                    # comment fields
                    c.get("original_post_id", ""),
                    c.get("comment_id", ""),
                    c.get("user", ""),
                    c.get("content", ""),
                    c.get("likes", 0),
                    bool(ai.get("ad_spam", False)),
                    bool(ai.get("meaningless", False)),
                    bool(ai.get("sensitive", False)),
                    bool(ai.get("machine_water_army", False)),
                    bool(c.get("_truncated", False)),
                    ",".join(hits) if hits else "",
                ]
            )

    wb.save(output_xlsx)


def main() -> None:
    input_jsonl = sys.argv[1] if len(sys.argv) > 1 else "output/cleaned_weibo.jsonl"
    output_xlsx = sys.argv[2] if len(sys.argv) > 2 else "output/cleaned_weibo.xlsx"
    export_xlsx(input_jsonl, output_xlsx)
    print(f"已生成 {output_xlsx}")


if __name__ == "__main__":
    main()
