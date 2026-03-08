import os
import json
import argparse
import asyncio
import aiohttp
import random
import re
import html
from tqdm import tqdm


def clean_html_to_text(html_text: str) -> str:
    try:
        s = html_text or ''
        s = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', '', s)
        s = re.sub(r'(?i)<br\s*/?>', '\n', s)
        s = re.sub(r'(?i)</p>', '\n', s)
        s = re.sub(r'(?is)<span[^>]*class=["\']url-icon["\'][^>]*>.*?</span>', '', s)
        s = re.sub(r'(?is)</?[^>]+>', '', s)
        s = html.unescape(s)
        s = s.replace('网页链接', '')
        s = re.sub(r'[ \t\r\f]+', ' ', s)
        s = re.sub(r'\n{2,}', '\n', s)
        return s.strip()
    except Exception:
        return (html_text or '').strip()


async def fetch_full_text(session: aiohttp.ClientSession, cookie: str, weibo_id: str) -> tuple:
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
    headers = {'User-Agent': ua, 'Cookie': cookie, 'Referer': f'https://m.weibo.cn/detail/{weibo_id}'}
    try:
        # show接口
        show_url = f'https://m.weibo.cn/statuses/show?id={weibo_id}'
        data = None
        for _ in range(3):
            await asyncio.sleep(random.uniform(1.5, 3.5))
            async with session.get(show_url, headers=headers) as resp:
                if resp.status != 200:
                    await asyncio.sleep(1)
                    continue
                try:
                    data = await resp.json()
                except Exception:
                    txt = await resp.text()
                    try:
                        data = json.loads(txt)
                    except Exception:
                        data = None
            if data:
                break
        if not data:
            # 尝试直接 extend
            ext_url = f'https://m.weibo.cn/statuses/extend?id={weibo_id}'
            headers_ext = {
                'User-Agent': ua,
                'Cookie': cookie,
                'Referer': f'https://m.weibo.cn/detail/{weibo_id}',
            }
            await asyncio.sleep(random.uniform(1.5, 3.5))
            async with session.get(ext_url, headers=headers_ext) as resp2:
                if resp2.status == 200:
                    try:
                        ext = await resp2.json()
                    except Exception:
                        t = await resp2.text()
                        try:
                            ext = json.loads(t)
                        except Exception:
                            ext = None
                    if ext:
                        long_html = (ext.get('data') or {}).get('longTextContent') or ''
                        if long_html:
                            return clean_html_to_text(long_html), ''
            return '', ''
        d = data.get('data') or {}
        is_long = bool(d.get('isLongText'))
        text_html = d.get('text') or ''
        full_text = clean_html_to_text(text_html)
        publish_time = d.get('created_at') or ''

        force_extend = is_long or ('全文' in (text_html or '')) or ('...全文' in (text_html or ''))
        if not force_extend:
            return full_text, publish_time

        # 418规避：模拟阅读延迟
        await asyncio.sleep(random.uniform(1.5, 3.5))
        # extend接口
        ext_url = f'https://m.weibo.cn/statuses/extend?id={weibo_id}'
        headers_ext = {
            'User-Agent': ua,
            'Cookie': cookie,
            'Referer': f'https://m.weibo.cn/detail/{weibo_id}',
        }
        async with session.get(ext_url, headers=headers_ext) as resp2:
            if resp2.status != 200:
                return full_text, publish_time
            ext = None
            try:
                ext = await resp2.json()
            except Exception:
                t = await resp2.text()
                try:
                    ext = json.loads(t)
                except Exception:
                    ext = None
        if ext:
            long_html = (ext.get('data') or {}).get('longTextContent') or ''
            if long_html:
                return clean_html_to_text(long_html), publish_time
        return full_text, publish_time
    except Exception:
        return '', ''


def _collect_processed_ids(path: str) -> set:
    ids = set()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                wid = str(((obj.get('weibo_details') or {}).get('id')) or '')
                if wid:
                    ids.add(wid)
    except Exception:
        ids = set()
    return ids


async def process_file(input_path: str, output_path: str, cookie: str):
    async with aiohttp.ClientSession() as session:
        resume_mode = os.path.exists(output_path)
        processed_ids = _collect_processed_ids(output_path) if resume_mode else set()
        pending_total = 0
        with open(input_path, 'r', encoding='utf-8') as counter:
            for line in counter:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                wid = str(((obj.get('weibo_details') or {}).get('id')) or '')
                if wid and wid in processed_ids:
                    continue
                pending_total += 1
        with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'a' if resume_mode else 'w', encoding='utf-8') as fout:
            pbar = tqdm(total=pending_total, desc='Enriching full_text', unit='line')
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                wid = str(((obj.get('weibo_details') or {}).get('id')) or '')
                if wid and wid in processed_ids:
                    continue
                base_text = (((obj.get('weibo_details') or {}).get('text')) or '')
                full_text = ''
                publish_time = ''
                if wid:
                    full_text, publish_time = await fetch_full_text(session, cookie, wid)
                if not full_text:
                    full_text = clean_html_to_text(base_text)
                obj.setdefault('weibo_details', {})['full_text'] = full_text
                if publish_time:
                    obj.setdefault('weibo_details', {})['publish_time'] = publish_time
                fout.write(json.dumps(obj, ensure_ascii=False) + '\n')
                fout.flush()
                pbar.update(1)


def main():
    parser = argparse.ArgumentParser(description='Enrich JSONL with full_text by calling Weibo APIs')
    parser.add_argument('--input', required=True, help='Input JSONL path')
    parser.add_argument('--output', help='Output JSONL path (default: input.with_full_text.jsonl)')
    parser.add_argument('--cookie', help='Weibo cookie (if not provided, read from config.json in CWD)')
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    output_path = args.output or (input_path.replace('.jsonl', '') + '.with_full_text.jsonl')
    output_path = os.path.abspath(output_path)

    cookie = args.cookie
    if not cookie:
        # try read config.json in CWD
        config_path = os.path.join(os.getcwd(), 'config.json')
        if os.path.isfile(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as cf:
                    cfg = json.load(cf)
                    cookie = cfg.get('cookie') or ''
            except Exception:
                cookie = ''
    if not cookie:
        raise SystemExit('Cookie is required. Pass --cookie or ensure config.json has "cookie" field.')

    asyncio.run(process_file(input_path, output_path, cookie))


if __name__ == '__main__':
    main()
