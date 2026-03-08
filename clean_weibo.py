import os
import re
import json
import math
import html
from typing import Any, Dict, List, Optional, Tuple
import random
import argparse
import datetime

try:
    from datasets import load_dataset  # type: ignore
except Exception:
    load_dataset = None  # type: ignore

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # type: ignore

try:
    from openpyxl import Workbook  # type: ignore
except Exception:
    Workbook = None  # type: ignore
CHINESE_PHONE_RE = re.compile(r"(?:\+?86[- ]?)?1[3-9]\d{9}")
QQ_NUMBER_RE = re.compile(r"(?:QQ|qq|Qq|qQ)[^\d]{0,3}(\d{5,12})")
WECHAT_ID_RE = re.compile(r"(?:微|VX|vx|V信|v信|微信)[^a-zA-Z0-9]{0,3}([a-zA-Z][a-zA-Z0-9_-]{4,})")
URL_RE = re.compile(r"https?://[^\s]+|(?:www\.)?[a-zA-Z0-9-]+\.(?:com|cn|net|org|top|vip|io)(?:/[^\s]*)?")
TAOBAO_CODE_RE = re.compile(r"(?:￥|淘口令|复[制]这段文案|打开淘宝)")
INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u2060\uFEFF]")
HTML_TAG_RE = re.compile(r"<[^>]+>")
EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF\U00002700-\U000027BF]"
)

SENSITIVE_WORDS = {
    "政治": ["颠覆", "煽动", "分裂", "境外势力"],
    "色情": ["色欲", "嫖娼", "约炮", "黄片", "AV"],
    "暴力": ["杀人", "爆炸", "恐袭", "血腥", "仇恨"],
    "违法": ["赌博", "网赌", "代孕", "走私", "洗钱"],
    "侮辱": ["傻逼", "脑残", "废物", "去你妈", "骂了隔壁"],
}


def to_half_width(s: str) -> str:
    res = []
    for ch in s:
        code = ord(ch)
        if code == 12288:
            res.append(" ")
        elif 65281 <= code <= 65374:
            res.append(chr(code - 65248))
        else:
            res.append(ch)
    return "".join(res)


def normalize_text(s: str) -> str:
    s = html.unescape(s or "")
    s = INVISIBLE_CHARS_RE.sub("", s)
    s = HTML_TAG_RE.sub(" ", s)
    s = to_half_width(s)
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def effective_length(s: str) -> int:
    no_emoji = EMOJI_RE.sub("", s)
    no_punct = re.sub(r"[^\w\u4e00-\u9fff]+", "", no_emoji)
    return len(no_punct)


def is_pure_punct_or_emoji(s: str) -> bool:
    s2 = re.sub(r"\s+", "", s)
    if not s2:
        return True
    no_word = re.sub(r"[\w\u4e00-\u9fff]", "", s2)
    return len(no_word) >= max(1, int(len(s2) * 0.9))


def is_meaningless_text(s: str) -> bool:
    s_norm = normalize_text(s)
    if not s_norm:
        return True
    if re.fullmatch(r"\d{3,}", s_norm):
        return True
    if re.fullmatch(r"(哈{3,}|呵{3,}|啊{3,}|嘿{3,}|哇{3,})", s_norm):
        return True
    unique_ratio = len(set(s_norm)) / max(1, len(s_norm))
    if unique_ratio < 0.15 and len(s_norm) >= 10:
        return True
    return False


def contains_ad_spam(s: str) -> bool:
    if CHINESE_PHONE_RE.search(s):
        return True
    if QQ_NUMBER_RE.search(s):
        return True
    if WECHAT_ID_RE.search(s):
        return True
    if URL_RE.search(s):
        return True
    if TAOBAO_CODE_RE.search(s):
        return True
    return False


def contains_sensitive(s: str) -> Tuple[bool, List[str]]:
    hits = []
    for cat, words in SENSITIVE_WORDS.items():
        for w in words:
            if w in s:
                hits.append(f"{cat}:{w}")
    if len(hits) > 0:
        print('触发敏感词',s , hits)
    return (len(hits) > 0, hits)


def similarity(a: str, b: str) -> float:
    import difflib

    return difflib.SequenceMatcher(None, a, b).ratio()

def article_text_norm(example: Dict[str, Any]) -> Optional[str]:
    details = example.get("weibo_details") or {}
    text = details.get("full_text") or details.get("text") or ""
    text_norm = normalize_text(text)
    return text_norm or None

def parse_publish_time(details: Dict[str, Any]) -> Optional[datetime]:
    s = details.get("publish_time")
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return None

def in_time_range(details: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    if not settings.get("time_filter_enable", False):
        return True
    dt = parse_publish_time(details)
    if dt is None:
        return False
    start = settings.get("start_date_iso")
    end = settings.get("end_date_iso")
    try:
        start_dt = datetime.datetime.fromisoformat(start + "T00:00:00+08:00") if start else None
        end_dt = datetime.datetime.fromisoformat(end + "T23:59:59+08:00") if end else None
    except Exception:
        return True
    if start_dt and dt < start_dt:
        return False
    if end_dt and dt > end_dt:
        return False
    return True

def dedupe_comments_by_id(comments: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen_ids = set()
    kept: List[Dict[str, Any]] = []
    removed = 0
    for c in comments:
        cid = c.get("comment_id")
        if cid and cid in seen_ids:
            removed += 1
            continue
        if cid:
            seen_ids.add(cid)
        kept.append(c)
    return kept, removed

def make_openai_client() -> Optional[Any]:
    if OpenAI is None:
        return None
    gpts_key = os.getenv("GPTS_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")
    api_key = gpts_key or openai_key
    if gpts_key:
        base_url = os.getenv("GPTS_BASE_URL") or "https://api.gptsapi.net/v1"
    else:
        base_url = os.getenv("GPTS_BASE_URL") or "https://api.openai.com/v1"
    if not api_key:
        return None
    try:
        client = OpenAI(base_url=base_url, api_key=api_key)
        return client
    except Exception:
        return None


def read_jsonl_as_dataset(path: str) -> Dict[str, List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        rows = []
    return {"train": rows}

def load_dataset_with_fallback(dataset_name: str, input_jsonl: Optional[str]) -> Dict[str, List[Dict[str, Any]]]:
    if input_jsonl and os.path.isfile(input_jsonl):
        return read_jsonl_as_dataset(input_jsonl)
    if load_dataset is None:
        raise RuntimeError("datasets 未安装，且未提供本地 JSONL")
    try:
        return load_dataset(dataset_name)
    except Exception:
        if input_jsonl and os.path.isfile(input_jsonl):
            return read_jsonl_as_dataset(input_jsonl)
        raise

def read_jsonl_rows(path: str) -> List[Dict[str, Any]]:
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

def export_jsonl_to_xlsx(
    jsonl_path: str,
    xlsx_path: str,
    include_ai_flags: bool,
    clean_map: Optional[Dict[str, Any]] = None,
    annotate_clean: bool = False,
) -> None:
    if Workbook is None:
        raise RuntimeError("缺少 openpyxl，请先安装：pip install openpyxl")
    os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
    wb = Workbook()
    ws_articles = wb.active
    ws_articles.title = "articles"
    ws_comments = wb.create_sheet("comments")
    # articles header（中文）
    ws_articles.append(
        ["关键词", "微博ID", "微博正文", "完整正文", "作者", "点赞数", "转发数", "评论数", "原评论条数", "清洗后评论条数", "发布时间"]
    )
    # comments header（中文）
    base_comment_header = [
        "关键词", "微博ID", "微博正文", "完整正文", "作者", "点赞数", "转发数", "评论数", "发布时间",
        "原微博ID", "评论ID", "评论用户", "评论内容", "评论点赞"
    ]
    comment_header = list(base_comment_header)
    if include_ai_flags:
        comment_header += ["AI广告", "AI无意义", "AI敏感", "机器水军", "是否截断", "敏感命中"]
    if annotate_clean:
        comment_header += ["是否被清洗", "被清洗原因"]
    ws_comments.append(comment_header)
    rows = read_jsonl_rows(jsonl_path)
    for r in rows:
        details = r.get("weibo_details") or {}
        stats = details.get("stats") or {}
        orig_comments = r.get("top_comments") or []
        cleaned_comments = r.get("cleaned_top_comments") or []
        ws_articles.append(
            [
                r.get("keyword", ""),
                details.get("id", ""),
                details.get("text", ""),
                details.get("full_text", ""),
                details.get("author", ""),
                stats.get("up", 0),
                stats.get("re", 0),
                stats.get("cm", 0),
                len(orig_comments),
                len(cleaned_comments),
                details.get("publish_time", ""),
            ]
        )
        comments_iter = cleaned_comments if (include_ai_flags and not annotate_clean) else orig_comments
        for c in comments_iter:
            row_base = [
                r.get("keyword", ""),
                details.get("id", ""),
                details.get("text", ""),
                details.get("full_text", ""),
                details.get("author", ""),
                stats.get("up", 0),
                stats.get("re", 0),
                stats.get("cm", 0),
                details.get("publish_time", ""),
                c.get("original_post_id", ""),
                c.get("comment_id", ""),
                c.get("user", ""),
                c.get("content", ""),
                c.get("likes", 0),
            ]
            row = list(row_base)
            if include_ai_flags:
                if annotate_clean and clean_map is not None:
                    cm = clean_map.get(str(c.get("comment_id", ""))) or {}
                    ai = cm.get("ai", {})
                    reasons = cm.get("reasons", [])
                    hits = cm.get("sensitive_hits", [])
                    row += [
                        bool(ai.get("ad_spam", "ai_ad_spam" in reasons)),
                        bool(ai.get("meaningless", "ai_meaningless" in reasons)),
                        bool(ai.get("sensitive", "ai_sensitive" in reasons)),
                        bool(ai.get("machine_water_army", False)),
                        bool(cm.get("truncated", False)),
                        ",".join(hits) if hits else "",
                    ]
                else:
                    flags = c.get("_flags") or {}
                    ai = flags.get("ai") or {}
                    hits = flags.get("sensitive_hits") or []
                    row += [
                        bool(ai.get("ad_spam", False)),
                        bool(ai.get("meaningless", False)),
                        bool(ai.get("sensitive", False)),
                        bool(ai.get("machine_water_army", False)),
                        bool(c.get("_truncated", False)),
                        ",".join(hits) if hits else "",
                    ]
            if annotate_clean and clean_map is not None:
                cm = clean_map.get(str(c.get("comment_id", ""))) or {}
                dropped = bool(cm.get("dropped", False))
                reasons = cm.get("reasons", [])
                row += [dropped, ",".join(sorted(set(reasons))) if dropped and reasons else ""]
            ws_comments.append(row)
    wb.save(xlsx_path)

def ai_classify(client: Any, text: str) -> Dict[str, bool]:
    if client is None or not text:
        return {}
    try:
        MODEL_NAME = os.getenv("GPTS_MODEL", "gpt-4o-mini")
        msg = (
            "你是评论清洗助手。判断文本是否属于以下类型，严格给出JSON："
            '{"results":[{"ad_spam":bool,"machine_water_army":bool,"sensitive":bool,"meaningless":bool}]}。'
            "仅根据文本内容判断。"
        )
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": msg},
                {"role": "user", "content": text},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            timeout=60,
        )
        content = resp.choices[0].message.content
        parsed = json.loads(content)
        results = parsed.get("results", [{}])[0]
        out = {}
        for k in ["ad_spam", "machine_water_army", "sensitive", "meaningless"]:
            v = results.get(k)
            if isinstance(v, bool):
                out[k] = v
        return out
    except Exception:
        return {}


def clean_comment(
    comment: Dict[str, Any],
    client: Optional[Any],
    settings: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], List[str], Dict[str, Any]]:
    orig = comment.get("content", "") or ""
    content = normalize_text(orig)
    all_reasons: List[str] = []
    reason_details: Dict[str, Any] = {}
    info: Dict[str, Any] = {
        "ai_called": False,
        "truncated": False,
        "content_norm": content,
        "content_orig": orig,
        "text_length": len(content),
        "effective_length": effective_length(content),
        "reason_details": reason_details,
    }
    if not content:
        all_reasons.append("empty")
        reason_details["empty"] = {"desc": "norm_empty"}
    eff_len_orig = effective_length(orig)
    eff_len_norm = effective_length(content)
    if eff_len_orig < settings["min_effective_len"] or eff_len_norm < settings["min_effective_len"]:
        all_reasons.append("too_short")
        reason_details["too_short"] = {"effective_length_orig": eff_len_orig, "effective_length_norm": eff_len_norm, "threshold": settings["min_effective_len"]}
    if len(content) > settings["max_len_drop"]:
        all_reasons.append("too_long_drop")
        reason_details["too_long_drop"] = {"length": len(content), "threshold": settings["max_len_drop"]}
    elif len(content) > settings["max_len_truncate"]:
        content = content[: settings["max_len_truncate"]].rstrip() + "..."
        info["truncated"] = True
    if is_pure_punct_or_emoji(content):
        all_reasons.append("pure_punct_emoji")
        reason_details["pure_punct_emoji"] = {}
    if is_meaningless_text(content):
        all_reasons.append("meaningless_rule")
        reason_details["meaningless_rule"] = {}
    if contains_ad_spam(content):
        all_reasons.append("ad_spam_rule")
        reason_details["ad_spam_rule"] = {}
    sensitive_flag, sensitive_hits = contains_sensitive(content)
    if sensitive_flag:
        info["sensitive_hits"] = sensitive_hits
        reason_details["sensitive_hits"] = {"hits": sensitive_hits}
    ai_flags = {}
    should_call_ai = bool(client is not None and settings.get("ai_enable", True))
    if settings.get("ai_on_sensitive_only", False) and not sensitive_flag:
        should_call_ai = False
    sample_rate = float(settings.get("ai_sample_rate", 1.0))
    if sample_rate < 1.0 and random.random() > sample_rate:
        should_call_ai = False
    if should_call_ai:
        ai_flags = ai_classify(client, content)
        info["ai_called"] = True
    if ai_flags.get("ad_spam"):
        all_reasons.append("ai_ad_spam")
        reason_details["ai_ad_spam"] = {}
    if ai_flags.get("meaningless"):
        all_reasons.append("ai_meaningless")
        reason_details["ai_meaningless"] = {}
    if ai_flags.get("sensitive") and settings["drop_sensitive"]:
        all_reasons.append("ai_sensitive")
        reason_details["ai_sensitive"] = {}
    delete_reasons = {
        "empty",
        "too_short",
        "too_long_drop",
        "pure_punct_emoji",
        "meaningless_rule",
        "ad_spam_rule",
        "ai_ad_spam",
        "ai_meaningless",
    }
    if settings.get("drop_sensitive"):
        delete_reasons.add("ai_sensitive")
    will_delete = any(r in delete_reasons for r in all_reasons)
    if will_delete:
        info["ai_flags"] = ai_flags
        return None, all_reasons, info
    cleaned = dict(comment)
    cleaned["content"] = content
    cleaned["_flags"] = {
        "ai": ai_flags,
        "sensitive_hits": sensitive_hits,
    }
    if info.get("truncated"):
        cleaned["_truncated"] = True
    return cleaned, all_reasons, info


def dedupe_comments(comments: List[Dict[str, Any]], sim_threshold: float = 0.9) -> Tuple[List[Dict[str, Any]], int, List[Dict[str, Any]]]:
    seen_user_content = set()
    kept: List[Dict[str, Any]] = []
    contents_cache: List[str] = []
    removed = 0
    removed_examples: List[Dict[str, Any]] = []
    for c in comments:
        user = c.get("user") or ""
        content = c.get("content") or ""
        key = (user, content)
        if key in seen_user_content:
            removed += 1
            ex = dict(c)
            ex["reason"] = "near_dup"
            removed_examples.append(ex)
            continue
        is_dup = False
        matched_prev = None
        for prev in contents_cache[-50:]:
            if similarity(prev, content) >= sim_threshold:
                is_dup = True
                matched_prev = prev
                break
        if is_dup:
            removed += 1
            ex = dict(c)
            ex["reason"] = "near_dup"
            if matched_prev is not None:
                ex["_near_dup_with"] = matched_prev
            removed_examples.append(ex)
            continue
        seen_user_content.add(key)
        contents_cache.append(content)
        kept.append(c)
    return kept, removed, removed_examples

def dedupe_comments_exact(comments: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, List[Dict[str, Any]]]:
    seen_norms = set()
    kept: List[Dict[str, Any]] = []
    removed = 0
    removed_examples: List[Dict[str, Any]] = []
    for c in comments:
        content = normalize_text(c.get("content", "") or "")
        if content in seen_norms:
            removed += 1
            ex = dict(c)
            ex["reason"] = "exact_dup"
            removed_examples.append(ex)
            continue
        seen_norms.add(content)
        kept.append(c)
    return kept, removed, removed_examples

def process_example(
    example: Dict[str, Any],
    client: Optional[Any],
    settings: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    comments = example.get("top_comments") or []
    cleaned: List[Dict[str, Any]] = []
    m = {
        "before": len(comments),
        "comments_exact_dup_removed": 0,
        "after_before_dedupe": 0,
        "after": 0,
        "near_dup_removed": 0,
        "truncated_count": 0,
        "dropped_reasons": {},
        "ai_invocations": 0,
        "dropped_examples": {},
    }
    comments, exact_removed, exact_removed_examples = dedupe_comments_exact(comments)
    m["comments_exact_dup_removed"] += exact_removed
    if exact_removed_examples:
        m["dropped_reasons"]["exact_dup"] = m["dropped_reasons"].get("exact_dup", 0) + len(exact_removed_examples)
        m["dropped_examples"].setdefault("exact_dup", []).extend(
            [
                {
                    "original_post_id": ex.get("original_post_id"),
                    "comment_id": ex.get("comment_id"),
                    "user": ex.get("user"),
                    "likes": ex.get("likes"),
                    "content_orig": ex.get("content"),
                    "content_norm": normalize_text(ex.get("content", "") or ""),
                    "reason": "exact_dup",
                }
                for ex in exact_removed_examples
            ]
        )
    for c in comments:
        cc, reasons, info = clean_comment(c, client, settings)
        if info.get("ai_called"):
            m["ai_invocations"] += 1
        if cc is not None:
            if cc.get("_truncated"):
                m["truncated_count"] += 1
            cleaned.append(cc)
        else:
            for r in reasons or ["unspecified"]:
                m["dropped_reasons"][r] = m["dropped_reasons"].get(r, 0) + 1
                case = {
                    "original_post_id": c.get("original_post_id"),
                    "comment_id": c.get("comment_id"),
                    "user": c.get("user"),
                    "likes": c.get("likes"),
                    "content_orig": info.get("content_orig", c.get("content")),
                    "content_norm": info.get("content_norm"),
                    "reason": r,
                }
                if info.get("sensitive_hits"):
                    case["sensitive_hits"] = info["sensitive_hits"]
                m["dropped_examples"].setdefault(r, []).append(case)
    m["after_before_dedupe"] = len(cleaned)
    cleaned, removed, removed_examples = dedupe_comments(cleaned, settings["near_dup_sim"])
    m["near_dup_removed"] = removed
    if removed_examples:
        m["dropped_reasons"]["near_dup"] = m["dropped_reasons"].get("near_dup", 0) + len(removed_examples)
        m["dropped_examples"].setdefault("near_dup", []).extend(
            [
                {
                    "original_post_id": ex.get("original_post_id"),
                    "comment_id": ex.get("comment_id"),
                    "user": ex.get("user"),
                    "likes": ex.get("likes"),
                    "content_orig": ex.get("content"),
                    "content_norm": ex.get("content"),
                    "reason": "near_dup",
                    "_near_dup_with": ex.get("_near_dup_with"),
                }
                for ex in removed_examples
            ]
        )
    m["after"] = len(cleaned)
    out = dict(example)
    out["cleaned_top_comments"] = cleaned
    if settings.get("replace_comments", True):
        out["top_comments"] = cleaned
    return out, m


DEFAULT_SETTINGS = {
    "min_effective_len": 5,
    "max_len_truncate": 500,
    "max_len_drop": 1000,
    "near_dup_sim": 0.9,
    "drop_sensitive": True,
    "ai_enable": True,
    "ai_on_sensitive_only": False,
    "ai_sample_rate": 1.0,
    "replace_comments": True,
    "drop_articles_no_comments": False,
    "dedupe_articles_exact": True,
    "dedupe_comments_exact": True,
    "time_filter_enable": True,
    "start_date_iso": "2025-01-01",
    "end_date_iso": "2027-06-01",
}


def clean_dataset(
    dataset_name: str = "Logistic12/weiboDataWithCommentByTheme",
    settings: Optional[Dict[str, Any]] = None,
    input_jsonl: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if load_dataset is None:
        if not input_jsonl:
            raise RuntimeError("datasets 未安装，且未提供本地 JSONL")
    settings = settings or DEFAULT_SETTINGS
    client = make_openai_client()
    ds = load_dataset_with_fallback(dataset_name, input_jsonl)
    cleaned_all: List[Dict[str, Any]] = []
    metrics_all: Dict[str, Any] = {
        "total_examples_raw": 0,
        "total_examples_after_article_dedupe": 0,
        "total_examples": 0,
        "total_comments_raw": 0,
        "total_comments_after_article_id_dedupe": 0,
        "total_comments_after_comment_exact_dedupe": 0,
        "total_comments_after_comment_id_dedupe": 0,
        "total_comments_before": 0,
        "total_comments_after_before_dedupe": 0,
        "total_comments_after": 0,
        "near_dup_removed": 0,
        "truncated_count": 0,
        "ai_invocations": 0,
        "dropped_reasons": {},
        "splits": {},
        "dropped_examples": {},
        "articles_dropped_no_comments": 0,
        "articles_dropped_dup": 0,
        "articles_dropped_time": 0,
        "comments_exact_dup_removed": 0,
        "comments_id_dup_removed": 0,
    }
    seen_articles: set = set()
    for split_name in ds.keys():
        split = ds[split_name]
        s_m = {
            "examples_raw": 0,
            "examples_after_article_dedupe": 0,
            "examples": 0,
            "comments_raw": 0,
            "comments_after_comment_exact_dedupe": 0,
            "comments_before": 0,
            "comments_after_before_dedupe": 0,
            "comments_after": 0,
            "near_dup_removed": 0,
            "truncated_count": 0,
            "ai_invocations": 0,
            "dropped_reasons": {},
            "dropped_examples": {},
            "articles_dropped_no_comments": 0,
            "articles_dropped_dup": 0,
            "comments_exact_dup_removed": 0,
        }
        for ex in split:
            ex_dict = dict(ex)
            metrics_all["total_examples_raw"] += 1
            s_m["examples_raw"] += 1
            raw_comments = len(ex_dict.get("top_comments") or [])
            metrics_all["total_comments_raw"] += raw_comments
            s_m["comments_raw"] += raw_comments
            details = ex_dict.get("weibo_details") or {}
            if not in_time_range(details, settings):
                metrics_all["articles_dropped_time"] += 1
                s_m["articles_dropped_time"] += 1
                continue
            art_key = details.get("id") if settings.get("dedupe_articles_exact", True) else None
            if art_key:
                if art_key in seen_articles:
                    metrics_all["articles_dropped_dup"] += 1
                    s_m["articles_dropped_dup"] += 1
                    continue
                seen_articles.add(art_key)
            metrics_all["total_examples_after_article_dedupe"] += 1
            s_m["examples_after_article_dedupe"] += 1
            metrics_all["total_comments_after_article_id_dedupe"] += len(ex_dict.get("top_comments") or [])
            s_m["comments_after_article_id_dedupe"] += len(ex_dict.get("top_comments") or [])
            comments = list(ex_dict.get("top_comments") or [])
            comments_after_id, removed_id = dedupe_comments_by_id(comments)
            ex_dict["top_comments"] = comments_after_id
            metrics_all["comments_id_dup_removed"] += removed_id
            s_m["comments_id_dup_removed"] += removed_id
            metrics_all["total_comments_after_comment_id_dedupe"] += len(comments_after_id)
            s_m["comments_after_comment_id_dedupe"] += len(comments_after_id)
            out, m = process_example(ex_dict, client, settings)
            if settings.get("drop_articles_no_comments", False) and len(out.get("top_comments") or []) == 0:
                metrics_all["articles_dropped_no_comments"] += 1
                s_m["articles_dropped_no_comments"] += 1
            else:
                cleaned_all.append(out)
            metrics_all["total_examples"] += 1
            s_m["examples"] += 1
            metrics_all["comments_exact_dup_removed"] += m.get("comments_exact_dup_removed", 0)
            s_m["comments_exact_dup_removed"] += m.get("comments_exact_dup_removed", 0)
            metrics_all["total_comments_after_comment_exact_dedupe"] += m.get("after_before_dedupe", 0)
            s_m["comments_after_comment_exact_dedupe"] += m.get("after_before_dedupe", 0)
            metrics_all["total_comments_before"] += m["before"]
            s_m["comments_before"] += m["before"]
            metrics_all["total_comments_after_before_dedupe"] += m["after_before_dedupe"]
            s_m["comments_after_before_dedupe"] += m["after_before_dedupe"]
            metrics_all["near_dup_removed"] += m["near_dup_removed"]
            s_m["near_dup_removed"] += m["near_dup_removed"]
            metrics_all["total_comments_after"] += m["after"]
            s_m["comments_after"] += m["after"]
            metrics_all["truncated_count"] += m["truncated_count"]
            s_m["truncated_count"] += m["truncated_count"]
            metrics_all["ai_invocations"] += m["ai_invocations"]
            s_m["ai_invocations"] += m["ai_invocations"]
            metrics_all["comments_exact_dup_removed"] += m["comments_exact_dup_removed"]
            s_m["comments_exact_dup_removed"] += m["comments_exact_dup_removed"]
            for k, v in m["dropped_reasons"].items():
                metrics_all["dropped_reasons"][k] = metrics_all["dropped_reasons"].get(k, 0) + v
                s_m["dropped_reasons"][k] = s_m["dropped_reasons"].get(k, 0) + v
            for reason, cases in m.get("dropped_examples", {}).items():
                metrics_all["dropped_examples"].setdefault(reason, []).extend(cases)
                s_m["dropped_examples"].setdefault(reason, []).extend(cases)
        metrics_all["splits"][split_name] = s_m
    return cleaned_all, metrics_all


def save_jsonl(rows: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def save_json(obj: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def build_summary_table(metrics: Dict[str, Any]) -> str:
    headers = [
        "指标", "数值",
    ]
    rows = [
        ("原始样本数", str(metrics.get("total_examples_raw", 0))),
        ("文章去重后样本数", str(metrics.get("total_examples_after_article_dedupe", 0))),
        ("最终样本数", str(metrics.get("total_examples", 0))),
        ("原始评论数", str(metrics.get("total_comments_raw", 0))),
        ("文章ID去重后评论数", str(metrics.get("total_comments_after_article_id_dedupe", 0))),
        ("评论ID去重后评论数", str(metrics.get("total_comments_after_comment_id_dedupe", 0))),
        ("评论精确去重后", str(metrics.get("total_comments_after_comment_exact_dedupe", 0))),
        ("清洗后评论数(去重前)", str(metrics.get("total_comments_after_before_dedupe", 0))),
        ("清洗后评论数(最终)", str(metrics.get("total_comments_after", 0))),
        ("文章重复删除数", str(metrics.get("articles_dropped_dup", 0))),
        ("文章时间不在范围删除数", str(metrics.get("articles_dropped_time", 0))),
        ("文章空评论删除数", str(metrics.get("articles_dropped_no_comments", 0))),
        ("评论精确重复删除数", str(metrics.get("comments_exact_dup_removed", 0))),
        ("评论ID重复删除数", str(metrics.get("comments_id_dup_removed", 0))),
        ("评论近似重复删除数", str(metrics.get("near_dup_removed", 0))),
        ("截断评论数", str(metrics.get("truncated_count", 0))),
        ("AI判别调用次数", str(metrics.get("ai_invocations", 0))),
    ]
    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for r in rows:
        table += f"| {r[0]} | {r[1]} |\n"
    return table

def build_splits_table(metrics: Dict[str, Any]) -> str:
    splits = metrics.get("splits", {}) or {}
    headers = [
        "split", "样本原始", "样本去重后", "样本最终", "评论原始", "评论精确后", "评论后(去重前)", "评论后(最终)",
        "文重删", "空文删", "评精确删", "近似删", "截断", "AI调用",
    ]
    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for name, s in splits.items():
        row = [
            str(name),
            str(s.get("examples_raw", s.get("examples", 0))),
            str(s.get("examples_after_article_dedupe", s.get("examples", 0))),
            str(s.get("examples", 0)),
            str(s.get("comments_raw", s.get("comments_before", 0))),
            str(s.get("comments_after_comment_exact_dedupe", 0)),
            str(s.get("comments_after_before_dedupe", 0)),
            str(s.get("comments_after", 0)),
            str(s.get("articles_dropped_dup", 0)),
            str(s.get("articles_dropped_no_comments", 0)),
            str(s.get("comments_exact_dup_removed", 0)),
            str(s.get("near_dup_removed", 0)),
            str(s.get("truncated_count", 0)),
            str(s.get("ai_invocations", 0)),
        ]
        table += "| " + " | ".join(row) + " |\n"
    return table

def build_reason_table(metrics: Dict[str, Any]) -> str:
    dr = metrics.get("dropped_reasons", {}) or {}
    headers = ["原因", "数量"]
    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for k in sorted(dr.keys()):
        table += f"| {k} | {dr.get(k, 0)} |\n"
    return table

def build_schema() -> Dict[str, str]:
    return {
        "total_examples_raw": "原始样本总数",
        "total_examples_after_article_dedupe": "文章严格去重后样本数",
        "total_examples": "最终输出样本数",
        "total_comments_raw": "原始评论总数",
        "total_comments_after_article_id_dedupe": "文章ID去重后评论数",
        "total_comments_after_comment_exact_dedupe": "评论严格去重后评论数",
        "total_comments_after_comment_id_dedupe": "评论ID去重后评论数",
        "total_comments_before": "清洗前评论计数（按样本累积）",
        "total_comments_after_before_dedupe": "清洗后、近似去重前评论计数",
        "total_comments_after": "清洗后最终评论计数",
        "articles_dropped_dup": "文章严格重复删除数量",
        "articles_dropped_time": "文章时间不在范围删除数量",
        "articles_dropped_no_comments": "清洗后无有效评论的文章删除数量",
        "articles_dropped_dup_ids": "重复删除的文章ID列表",
        "comments_exact_dup_removed": "评论严格重复删除数量",
        "comments_id_dup_removed": "评论ID重复删除数量",
        "near_dup_removed": "评论近似重复删除数量",
        "truncated_count": "被截断的评论数量",
        "ai_invocations": "AI判别调用次数",
        "dropped_reasons": "按原因的删除计数分布",
        "dropped_examples": "按原因的删除样本明细列表",
        "summary_table_markdown": "总体汇总表（Markdown）",
        "splits_table_markdown": "分片汇总表（Markdown）",
        "reason_table_markdown": "删除原因分布表（Markdown）",
        "ai_reason_counts": "AI判别相关原因的计数聚合",
    }

def build_field_catalog() -> Dict[str, Any]:
    return {
        "sample_fields": {
            "keyword": "关键词",
            "weibo_details": {
                "id": "微博ID",
                "text": "微博正文（摘要）",
                "full_text": "微博完整正文",
                "author": "作者昵称",
                "stats": {
                    "up": "点赞数",
                    "re": "转发数",
                    "cm": "评论数",
                },
                "publish_time": "发布时间（字符串）",
            },
            "top_comments": "原始评论列表（每项含 original_post_id、comment_id、user、content、likes）",
            "cleaned_top_comments": "清洗后评论列表，结构同 top_comments",
            "clean_comment_flags": {
                "_flags": {
                    "ai": {
                        "ad_spam": "AI判定为广告",
                        "meaningless": "AI判定为无意义",
                        "sensitive": "AI判定为敏感",
                        "machine_water_army": "AI判定为机器水军",
                    },
                    "sensitive_hits": "命中敏感词列表",
                },
                "_truncated": "是否因长度被截断",
            },
        },
        "metrics_fields": build_schema(),
        "splits_fields": "分片级指标对象，键与 metrics_fields 对应，作用域为各分片",
        "reason_keys": {
            "empty": "规范化后为空",
            "too_short": "有效长度低于阈值",
            "too_long_drop": "超过删除长度阈值",
            "pure_punct_emoji": "纯标点或表情",
            "meaningless_rule": "无意义文本",
            "ad_spam_rule": "规则命中广告",
            "exact_dup": "文本完全重复",
            "near_dup": "文本近似重复",
            "ai_ad_spam": "AI判定广告",
            "ai_meaningless": "AI判定无意义",
            "ai_sensitive": "AI判定敏感（在允许删除敏感时）",
        },
        "tables": {
            "summary_table_markdown": "总体汇总表（Markdown）",
            "splits_table_markdown": "分片汇总表（Markdown）",
            "reason_table_markdown": "删除原因分布表（Markdown）",
        },
        "extra": {
            "articles_dropped_dup_ids": "重复删除文章ID列表",
        },
    }

def run_streaming_clean(
    dataset_name: str,
    settings: Dict[str, Any],
    out_path: str,
    metrics_path: str,
    raw_out_path: str,
    input_jsonl: Optional[str],
    export_clean_xlsx: Optional[str] = "output/cleaned_weibo.xlsx",
    export_raw_xlsx: Optional[str] = "output/raw_weibo.xlsx",
    batch_size: int = 200,
    print_every: int = 100,
) -> None:
    client = make_openai_client()
    ds = load_dataset_with_fallback(dataset_name, input_jsonl)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    metrics_all: Dict[str, Any] = {
        "total_examples_raw": 0,
        "total_examples_after_article_dedupe": 0,
        "total_examples": 0,
        "total_comments_raw": 0,
        "total_comments_after_article_id_dedupe": 0,
        "total_comments_after_comment_exact_dedupe": 0,
        "total_comments_after_comment_id_dedupe": 0,
        "total_comments_before": 0,
        "total_comments_after_before_dedupe": 0,
        "total_comments_after": 0,
        "near_dup_removed": 0,
        "truncated_count": 0,
        "ai_invocations": 0,
        "dropped_reasons": {
            "empty": 0,
            "too_short": 0,
            "too_long_drop": 0,
            "pure_punct_emoji": 0,
            "meaningless_rule": 0,
            "ad_spam_rule": 0,
            "exact_dup": 0,
            "near_dup": 0,
            "ai_ad_spam": 0,
            "ai_meaningless": 0,
            "ai_sensitive": 0,
        },
        "splits": {},
        "dropped_examples": {},
        "articles_dropped_no_comments": 0,
        "articles_dropped_dup": 0,
        "articles_dropped_time": 0,
        "articles_dropped_dup_ids": [],
        "comments_exact_dup_removed": 0,
        "comments_id_dup_removed": 0,
    }
    processed_since_last_print = 0
    processed_since_last_save = 0
    total_est = sum(len(ds[s]) for s in ds.keys())
    pbar = None
    if tqdm is not None:
        pbar = tqdm(total=total_est, desc="清洗进度", unit="样本", ascii=True)
    seen_articles: set = set()
    clean_map: Dict[str, Any] = {}
    with open(out_path, "w", encoding="utf-8") as out_f, open(raw_out_path, "w", encoding="utf-8") as raw_out_f:
        for split_name in ds.keys():
            split = ds[split_name]
            s_m = {
                "examples_raw": 0,
                "examples_after_article_dedupe": 0,
                "examples": 0,
                "comments_raw": 0,
                "comments_after_article_id_dedupe": 0,
                "comments_after_comment_exact_dedupe": 0,
                "comments_after_comment_id_dedupe": 0,
                "comments_before": 0,
                "comments_after_before_dedupe": 0,
                "comments_after": 0,
                "near_dup_removed": 0,
                "truncated_count": 0,
                "ai_invocations": 0,
                "dropped_reasons": {
                    "empty": 0,
                    "too_short": 0,
                    "too_long_drop": 0,
                    "pure_punct_emoji": 0,
                    "meaningless_rule": 0,
                    "ad_spam_rule": 0,
                    "exact_dup": 0,
                    "near_dup": 0,
                    "ai_ad_spam": 0,
                    "ai_meaningless": 0,
                    "ai_sensitive": 0,
                },
                "dropped_examples": {},
                "articles_dropped_no_comments": 0,
                "articles_dropped_dup": 0,
                "articles_dropped_time": 0,
                "articles_dropped_dup_ids": [],
                "comments_exact_dup_removed": 0,
                "comments_id_dup_removed": 0,
            }
            for ex in split:
                ex_dict = dict(ex)
                raw_out_f.write(json.dumps(ex_dict, ensure_ascii=False) + "\n")
                metrics_all["total_examples_raw"] += 1
                s_m["examples_raw"] += 1
                raw_comments = len(ex_dict.get("top_comments") or [])
                metrics_all["total_comments_raw"] += raw_comments
                s_m["comments_raw"] += raw_comments
                details = ex_dict.get("weibo_details") or {}
                if not in_time_range(details, settings):
                    metrics_all["articles_dropped_time"] += 1
                    s_m["articles_dropped_time"] += 1
                    for c in ex_dict.get("top_comments") or []:
                        cid = str(c.get("comment_id", ""))
                        if not cid:
                            continue
                        cm = clean_map.setdefault(cid, {})
                        cm["dropped"] = True
                        cm.setdefault("reasons", [])
                        cm["reasons"].append("article_time")
                    if pbar is not None:
                        pbar.update(1)
                    processed_since_last_print += 1
                    processed_since_last_save += 1
                    continue
                art_key = details.get("id") if settings.get("dedupe_articles_exact", True) else None
                if art_key:
                    if art_key in seen_articles:
                        metrics_all["articles_dropped_dup"] += 1
                        s_m["articles_dropped_dup"] += 1
                        if details.get("id"):
                            metrics_all["articles_dropped_dup_ids"].append(details["id"])
                            s_m["articles_dropped_dup_ids"].append(details["id"])
                        for c in ex_dict.get("top_comments") or []:
                            cid = str(c.get("comment_id", ""))
                            if not cid:
                                continue
                            cm = clean_map.setdefault(cid, {})
                            cm["dropped"] = True
                            cm.setdefault("reasons", [])
                            cm["reasons"].append("article_dup")
                        if pbar is not None:
                            pbar.update(1)
                        processed_since_last_print += 1
                        processed_since_last_save += 1
                        continue
                    seen_articles.add(art_key)
                metrics_all["total_examples_after_article_dedupe"] += 1
                s_m["examples_after_article_dedupe"] += 1
                metrics_all["total_comments_after_article_id_dedupe"] += len(ex_dict.get("top_comments") or [])
                s_m["comments_after_article_id_dedupe"] += len(ex_dict.get("top_comments") or [])
                comments = list(ex_dict.get("top_comments") or [])
                comments_after_id, removed_id = dedupe_comments_by_id(comments)
                ex_dict["top_comments"] = comments_after_id
                metrics_all["comments_id_dup_removed"] += removed_id
                s_m["comments_id_dup_removed"] += removed_id
                metrics_all["total_comments_after_comment_id_dedupe"] += len(comments_after_id)
                s_m["comments_after_comment_id_dedupe"] += len(comments_after_id)
                out, m = process_example(ex_dict, client, settings)
                if settings.get("drop_articles_no_comments", False) and len(out.get("top_comments") or []) == 0:
                    metrics_all["articles_dropped_no_comments"] += 1
                    s_m["articles_dropped_no_comments"] += 1
                else:
                    out_f.write(json.dumps(out, ensure_ascii=False) + "\n")
                # 聚合清洗映射：保留与删除评论
                for kept in out.get("cleaned_top_comments") or []:
                    cid = str(kept.get("comment_id", ""))
                    if not cid:
                        continue
                    flags = kept.get("_flags") or {}
                    ai = flags.get("ai") or {}
                    cm = clean_map.setdefault(cid, {})
                    cm["dropped"] = False
                    cm["ai"] = ai
                    cm["truncated"] = bool(kept.get("_truncated", False))
                    cm["sensitive_hits"] = (flags.get("sensitive_hits") or [])
                    cm.setdefault("reasons", [])
                metrics_all["total_examples"] += 1
                s_m["examples"] += 1
                metrics_all["comments_exact_dup_removed"] += m.get("comments_exact_dup_removed", 0)
                s_m["comments_exact_dup_removed"] += m.get("comments_exact_dup_removed", 0)
                metrics_all["total_comments_after_comment_exact_dedupe"] += m.get("comments_after_exact_dedupe", m["before"])
                s_m["comments_after_comment_exact_dedupe"] += m.get("comments_after_exact_dedupe", m["before"])
                metrics_all["total_comments_before"] += m["before"]
                s_m["comments_before"] += m["before"]
                metrics_all["total_comments_after_before_dedupe"] += m["after_before_dedupe"]
                s_m["comments_after_before_dedupe"] += m["after_before_dedupe"]
                metrics_all["near_dup_removed"] += m["near_dup_removed"]
                s_m["near_dup_removed"] += m["near_dup_removed"]
                metrics_all["total_comments_after"] += m["after"]
                s_m["comments_after"] += m["after"]
                metrics_all["truncated_count"] += m["truncated_count"]
                s_m["truncated_count"] += m["truncated_count"]
                metrics_all["ai_invocations"] += m["ai_invocations"]
                s_m["ai_invocations"] += m["ai_invocations"]
                for k, v in m["dropped_reasons"].items():
                    metrics_all["dropped_reasons"][k] = metrics_all["dropped_reasons"].get(k, 0) + v
                    s_m["dropped_reasons"][k] = s_m["dropped_reasons"].get(k, 0) + v
                for reason, cases in m.get("dropped_examples", {}).items():
                    metrics_all["dropped_examples"].setdefault(reason, []).extend(cases)
                    s_m["dropped_examples"].setdefault(reason, []).extend(cases)
                    for ex_case in cases:
                        cid = str(ex_case.get("comment_id", ""))
                        if not cid:
                            continue
                        cm = clean_map.setdefault(cid, {})
                        cm["dropped"] = True
                        cm.setdefault("reasons", [])
                        cm["reasons"].append(reason)
                        if ex_case.get("sensitive_hits"):
                            cm["sensitive_hits"] = ex_case["sensitive_hits"]
                processed_since_last_print += 1
                processed_since_last_save += 1
                if pbar is not None:
                    pbar.update(1)
                    if processed_since_last_print >= print_every:
                        pbar.set_postfix(
                            {
                                "前": metrics_all["total_comments_before"],
                                "后": metrics_all["total_comments_after"],
                                "近似重复删": metrics_all["near_dup_removed"],
                                "截断": metrics_all["truncated_count"],
                                "AI": metrics_all["ai_invocations"],
                                "文重删": metrics_all["articles_dropped_dup"],
                                "评精确删": metrics_all["comments_exact_dup_removed"],
                            }
                        )
                if processed_since_last_print >= print_every:
                    print(
                        f"进度: 样本(原始) {metrics_all['total_examples_raw']} | 样本(文章去重后) {metrics_all['total_examples_after_article_dedupe']} | 样本(最终) {metrics_all['total_examples']} | "
                        f"评论前 {metrics_all['total_comments_before']} | 去重前后 {metrics_all['total_comments_after_before_dedupe']} -> {metrics_all['total_comments_after']} | "
                        f"文重删 {metrics_all['articles_dropped_dup']} | 评论精确重复删 {metrics_all['comments_exact_dup_removed']} | "
                        f"近似重复移除 {metrics_all['near_dup_removed']} | 截断 {metrics_all['truncated_count']} | AI调用 {metrics_all['ai_invocations']} | 空评论文章删 {metrics_all['articles_dropped_no_comments']}",
                        flush=True,
                    )
                    processed_since_last_print = 0
                if processed_since_last_save >= batch_size:
                    save_json(metrics_all, metrics_path)
                    processed_since_last_save = 0
            metrics_all["splits"][split_name] = s_m
    save_json(metrics_all, metrics_path)
    if pbar is not None:
        pbar.close()
    metrics_all["summary_table_markdown"] = build_summary_table(metrics_all)
    metrics_all["splits_table_markdown"] = build_splits_table(metrics_all)
    metrics_all["reason_table_markdown"] = build_reason_table(metrics_all)
    metrics_all["ai_reason_counts"] = {k: v for k, v in (metrics_all.get("dropped_reasons") or {}).items() if str(k).startswith("ai_")}
    metrics_all["schema"] = build_schema()
    print("\n指标汇总表：")
    print(metrics_all["summary_table_markdown"])
    print("\n分片指标表：")
    print(metrics_all["splits_table_markdown"])
    print("\n原因分布表：")
    print(metrics_all["reason_table_markdown"])
    try:
        save_json(build_field_catalog(), os.path.join(os.path.dirname(metrics_path), "field_catalog.json"))
    except Exception:
        pass
    try:
        if export_clean_xlsx:
            export_jsonl_to_xlsx(out_path, export_clean_xlsx, include_ai_flags=True, clean_map=None, annotate_clean=False)
        if export_raw_xlsx:
            export_jsonl_to_xlsx(raw_out_path, export_raw_xlsx, include_ai_flags=True, clean_map=clean_map, annotate_clean=True)
        print(f"已导出 XLSX：clean={export_clean_xlsx} raw={export_raw_xlsx}")
    except Exception as e:
        print(f"导出 XLSX 失败：{e}")


def main() -> None:
    def str2bool(v: str) -> bool:
        return str(v).lower() in {"1", "true", "yes", "y", "t"}

    parser = argparse.ArgumentParser(description="微博评论清洗")
    parser.add_argument("--dataset", type=str, default="Logistic12/weiboDataWithCommentByTheme")
    parser.add_argument("--output", type=str, default=os.getenv("OUTPUT_PATH") or "output/cleaned_weibo.jsonl")
    parser.add_argument("--metrics", type=str, default=os.getenv("METRICS_PATH") or "output/clean_metrics.json")
    parser.add_argument("--raw-output", type=str, default=os.getenv("RAW_OUTPUT_PATH") or "output/raw_weibo.jsonl")
    parser.add_argument("--input-jsonl", type=str, default=os.getenv("INPUT_JSONL") or "")
    parser.add_argument("--export-clean-xlsx", type=str, default=os.getenv("EXPORT_CLEAN_XLSX") or "output/cleaned_weibo.xlsx")
    parser.add_argument("--export-raw-xlsx", type=str, default=os.getenv("EXPORT_RAW_XLSX") or "output/raw_weibo.xlsx")
    parser.add_argument("--streaming", type=str, default=os.getenv("STREAMING", "1"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "200")))
    parser.add_argument("--print-every", type=int, default=int(os.getenv("PRINT_EVERY", "100")))
    parser.add_argument("--min-effective-len", type=int, default=DEFAULT_SETTINGS["min_effective_len"])
    parser.add_argument("--max-len-truncate", type=int, default=DEFAULT_SETTINGS["max_len_truncate"])
    parser.add_argument("--max-len-drop", type=int, default=DEFAULT_SETTINGS["max_len_drop"])
    parser.add_argument("--near-dup-sim", type=float, default=DEFAULT_SETTINGS["near_dup_sim"])
    parser.add_argument("--drop-sensitive", type=str, default=str(DEFAULT_SETTINGS["drop_sensitive"]))
    parser.add_argument("--ai-enable", type=str, default=str(DEFAULT_SETTINGS["ai_enable"]))
    parser.add_argument("--ai-on-sensitive-only", type=str, default=str(DEFAULT_SETTINGS["ai_on_sensitive_only"]))
    parser.add_argument("--ai-sample-rate", type=float, default=float(DEFAULT_SETTINGS["ai_sample_rate"]))
    parser.add_argument("--time-filter-enable", type=str, default=str(DEFAULT_SETTINGS["time_filter_enable"]))
    parser.add_argument("--start-date", type=str, default=DEFAULT_SETTINGS["start_date_iso"])
    parser.add_argument("--end-date", type=str, default=DEFAULT_SETTINGS["end_date_iso"])
    args = parser.parse_args()

    settings = dict(DEFAULT_SETTINGS)
    settings["min_effective_len"] = args.min_effective_len
    settings["max_len_truncate"] = args.max_len_truncate
    settings["max_len_drop"] = args.max_len_drop
    settings["near_dup_sim"] = args.near_dup_sim
    settings["drop_sensitive"] = str2bool(args.drop_sensitive)
    settings["ai_enable"] = str2bool(args.ai_enable)
    settings["ai_on_sensitive_only"] = str2bool(args.ai_on_sensitive_only)
    settings["ai_sample_rate"] = args.ai_sample_rate
    settings["time_filter_enable"] = str2bool(args.time_filter_enable)
    settings["start_date_iso"] = args.start_date
    settings["end_date_iso"] = args.end_date

    streaming = str2bool(args.streaming)
    out_path = args.output
    metrics_path = args.metrics
    batch_size = args.batch_size
    print_every = args.print_every

    if streaming:
        run_streaming_clean(
            dataset_name=args.dataset,
            settings=settings,
            out_path=out_path,
            metrics_path=metrics_path,
            raw_out_path=args.raw_output,
            input_jsonl=(args.input_jsonl or None),
            export_clean_xlsx=args.export_clean_xlsx,
            export_raw_xlsx=args.export_raw_xlsx,
            batch_size=batch_size,
            print_every=print_every,
        )
        print(f"已实时写入数据到 {out_path}，指标滚动更新到 {metrics_path}")
    else:
        ds2 = load_dataset_with_fallback(args.dataset, (args.input_jsonl or None))
        with open(args.raw_output, "w", encoding="utf-8") as rf:
            for split_name in ds2.keys():
                for ex in ds2[split_name]:
                    rf.write(json.dumps(dict(ex), ensure_ascii=False) + "\n")
        rows, metrics = clean_dataset(dataset_name=args.dataset, settings=settings, input_jsonl=(args.input_jsonl or None))
        save_jsonl(rows, out_path)
        metrics["summary_table_markdown"] = build_summary_table(metrics)
        metrics["splits_table_markdown"] = build_splits_table(metrics)
        metrics["reason_table_markdown"] = build_reason_table(metrics)
        metrics["ai_reason_counts"] = {k: v for k, v in (metrics.get("dropped_reasons") or {}).items() if str(k).startswith("ai_")}
        metrics["schema"] = build_schema()
        save_json(metrics, metrics_path)
        print(f"清洗完成，样本数 {metrics['total_examples']}，评论前 {metrics['total_comments_before']}，去重前后 {metrics['total_comments_after_before_dedupe']} -> {metrics['total_comments_after']}，近似重复移除 {metrics['near_dup_removed']}，截断 {metrics['truncated_count']}。")
        print("清洗原因统计：")
        print(json.dumps(metrics["dropped_reasons"], ensure_ascii=False, indent=2))
        print(f"AI判别调用次数：{metrics['ai_invocations']}")
        print(f"已输出数据到 {out_path}，指标到 {metrics_path}")
        print("\n指标汇总表：")
        print(metrics["summary_table_markdown"])
        print("\n分片指标表：")
        print(metrics["splits_table_markdown"])
        print("\n原因分布表：")
        print(metrics["reason_table_markdown"])
        try:
            save_json(build_field_catalog(), os.path.join(os.path.dirname(metrics_path), "field_catalog.json"))
        except Exception:
            pass
        try:
            if args.export_clean_xlsx:
                export_jsonl_to_xlsx(out_path, args.export_clean_xlsx, include_ai_flags=True)
            if args.export_raw_xlsx:
                export_jsonl_to_xlsx(args.raw_output, args.export_raw_xlsx, include_ai_flags=False)
            print(f"已导出 XLSX：clean={args.export_clean_xlsx} raw={args.export_raw_xlsx}")
        except Exception as e:
            print(f"导出 XLSX 失败：{e}")


if __name__ == "__main__":
    main()
