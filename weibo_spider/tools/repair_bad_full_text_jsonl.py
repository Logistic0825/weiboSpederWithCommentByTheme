import os
import json
import argparse
import asyncio
try:
    import aiohttp
except Exception:
    aiohttp = None
import random
import re
import html
try:
    from tqdm import tqdm
except Exception:
    class _DummyTqdm:
        def __init__(self, total=0, desc='', unit=''):
            pass
        def update(self, n):
            pass
        def close(self):
            pass
    tqdm = _DummyTqdm
import time
import urllib.request
import urllib.error


def clean_html_to_text(s: str) -> str:
    try:
        s = s or ''
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
        return (s or '').strip()


async def fetch_full(session, cookie: str, wid: str) -> tuple:
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
    headers = {'User-Agent': ua, 'Cookie': cookie, 'Referer': f'https://m.weibo.cn/detail/{wid}'}
    show_url = f'https://m.weibo.cn/statuses/show?id={wid}'
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
                t = await resp.text()
                try:
                    data = json.loads(t)
                except Exception:
                    data = None
        if data:
            break
    if not data:
        ext_url = f'https://m.weibo.cn/statuses/extend?id={wid}'
        async with session.get(ext_url, headers=headers) as resp2:
            if resp2.status == 200:
                try:
                    ext = await resp2.json()
                except Exception:
                    tt = await resp2.text()
                    try:
                        ext = json.loads(tt)
                    except Exception:
                        ext = None
                if ext:
                    long_html = (ext.get('data') or {}).get('longTextContent') or ''
                    if long_html:
                        return clean_html_to_text(long_html), ''
        return '', ''
    d = data.get('data') or {}
    base_html = d.get('text') or ''
    base_text = clean_html_to_text(base_html)
    publish_time = d.get('created_at') or ''
    need_ext = ('全文' in (base_html or '')) or ('...全文' in (base_html or '')) or ('网页链接' in (base_html or '')) or bool(d.get('isLongText'))
    if not need_ext:
        return base_text, publish_time
    await asyncio.sleep(random.uniform(1.5, 3.5))
    ext_url = f'https://m.weibo.cn/statuses/extend?id={wid}'
    async with session.get(ext_url, headers=headers) as resp2:
        if resp2.status != 200:
            return base_text, publish_time
        try:
            ext = await resp2.json()
        except Exception:
            tt = await resp2.text()
            try:
                ext = json.loads(tt)
            except Exception:
                ext = None
        if ext:
            long_html = (ext.get('data') or {}).get('longTextContent') or ''
            if long_html:
                return clean_html_to_text(long_html), publish_time
    return base_text, publish_time

def fetch_full_sync(cookie: str, wid: str) -> tuple:
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
    headers = {'User-Agent': ua, 'Cookie': cookie, 'Referer': f'https://m.weibo.cn/detail/{wid}'}
    def _get(url):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.read().decode('utf-8', errors='ignore')
        except Exception:
            return ''
    show_url = f'https://m.weibo.cn/statuses/show?id={wid}'
    data = None
    for _ in range(3):
        time.sleep(random.uniform(1.5, 3.5))
        txt = _get(show_url)
        if not txt:
            time.sleep(1)
            continue
        try:
            data = json.loads(txt)
        except Exception:
            data = None
        if data:
            break
    if not data:
        ext_url = f'https://m.weibo.cn/statuses/extend?id={wid}'
        time.sleep(random.uniform(1.5, 3.5))
        tt = _get(ext_url)
        if tt:
            try:
                ext = json.loads(tt)
            except Exception:
                ext = None
            if ext:
                long_html = (ext.get('data') or {}).get('longTextContent') or ''
                if long_html:
                    return clean_html_to_text(long_html), ''
        return '', ''
    d = data.get('data') or {}
    base_html = d.get('text') or ''
    base_text = clean_html_to_text(base_html)
    publish_time = d.get('created_at') or ''
    need_ext = ('全文' in (base_html or '')) or ('...全文' in (base_html or '')) or ('网页链接' in (base_html or '')) or bool(d.get('isLongText'))
    if not need_ext:
        return base_text, publish_time
    ext_url = f'https://m.weibo.cn/statuses/extend?id={wid}'
    time.sleep(random.uniform(1.5, 3.5))
    tt = _get(ext_url)
    if tt:
        try:
            ext = json.loads(tt)
        except Exception:
            ext = None
        if ext:
            long_html = (ext.get('data') or {}).get('longTextContent') or ''
            if long_html:
                return clean_html_to_text(long_html), publish_time
    return base_text, publish_time


def is_badcase(obj: dict) -> bool:
    d = obj.get('weibo_details') or {}
    text = d.get('text') or ''
    ft = d.get('full_text') or ''
    t_clean = clean_html_to_text(text)
    if ('全文' in text) or ('...全文' in text) or ('网页链接' in text):
        if not ft or ft.strip() == '' or ft.strip() == t_clean.strip():
            return True
    return False


async def repair_file(path: str, cookie: str):
    tmp_path = path + '.tmp'
    async with aiohttp.ClientSession() as session, open(path, 'r', encoding='utf-8') as fin, open(tmp_path, 'w', encoding='utf-8') as fout:
        lines = fin.readlines()
        pbar = tqdm(total=len(lines), desc=f'Repair {os.path.basename(path)}', unit='line')
        for line in lines:
            line = line.strip()
            if not line:
                pbar.update(1)
                continue
            try:
                obj = json.loads(line)
            except Exception:
                fout.write(line + '\n')
                pbar.update(1)
                continue
            if is_badcase(obj):
                wid = str(((obj.get('weibo_details') or {}).get('id')) or '')
                full_text, publish_time = await fetch_full(session, cookie, wid) if wid else ('', '')
                if full_text:
                    obj.setdefault('weibo_details', {})['full_text'] = full_text
                if publish_time:
                    obj.setdefault('weibo_details', {})['publish_time'] = publish_time
            fout.write(json.dumps(obj, ensure_ascii=False) + '\n')
            pbar.update(1)
        pbar.close()
    os.replace(tmp_path, path)

def repair_file_sync(path: str, cookie: str):
    tmp_path = path + '.tmp'
    with open(path, 'r', encoding='utf-8') as fin, open(tmp_path, 'w', encoding='utf-8') as fout:
        lines = fin.readlines()
        pbar = tqdm(total=len(lines), desc=f'Repair {os.path.basename(path)}', unit='line')
        for line in lines:
            line = line.strip()
            if not line:
                pbar.update(1)
                continue
            try:
                obj = json.loads(line)
            except Exception:
                fout.write(line + '\n')
                pbar.update(1)
                continue
            if is_badcase(obj):
                wid = str(((obj.get('weibo_details') or {}).get('id')) or '')
                full_text, publish_time = fetch_full_sync(cookie, wid) if wid else ('', '')
                if full_text:
                    obj.setdefault('weibo_details', {})['full_text'] = full_text
                if publish_time:
                    obj.setdefault('weibo_details', {})['publish_time'] = publish_time
            fout.write(json.dumps(obj, ensure_ascii=False) + '\n')
            pbar.update(1)
        pbar.close()
    os.replace(tmp_path, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inputs', nargs='+', required=True)
    ap.add_argument('--cookie')
    args = ap.parse_args()
    cookie = args.cookie
    if not cookie:
        cfg_path = os.path.join(os.getcwd(), 'config.json')
        if os.path.isfile(cfg_path):
            try:
                with open(cfg_path, 'r', encoding='utf-8') as cf:
                    cfg = json.load(cf)
                    cookie = cfg.get('cookie') or ''
            except Exception:
                cookie = ''
    if not cookie:
        raise SystemExit('Cookie is required')
    if aiohttp is None:
        for p in args.inputs:
            repair_file_sync(os.path.abspath(p), cookie)
    else:
        async def _run():
            await asyncio.gather(*(repair_file(os.path.abspath(p), cookie) for p in args.inputs))
        asyncio.run(_run())


if __name__ == '__main__':
    main()
