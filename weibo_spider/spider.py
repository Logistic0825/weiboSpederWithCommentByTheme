#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import logging
import logging.config
import os
import random
import shutil
import sys
import asyncio
import aiohttp
import re
import html
from datetime import date, datetime, timedelta
from time import sleep

from absl import app, flags
from tqdm import tqdm

from . import config_util, datetime_util
from .downloader import AvatarPictureDownloader
from .parser import AlbumParser, IndexParser, PageParser, PhotoParser
from .weibo import Weibo
from .parser.util import handle_html_async
from .user import User
from .writer.csv_writer import CsvWriter
from .writer.jsonl_writer import JsonlWriter

FLAGS = flags.FLAGS

flags.DEFINE_string('config_path', None, 'The path to config.json.')
flags.DEFINE_string('keywords', None, 'Comma-separated keywords to search.')
flags.DEFINE_string('keyword_list', None, 'The path to keywords txt file.')
flags.DEFINE_string('output_dir', None, 'The dir path to store results.')
flags.DEFINE_integer('keyword_concurrency', 1, 'Number of concurrent keyword tasks.')
flags.DEFINE_integer('target_total_jsonl', 0, 'Stop after writing this many JSONL records; 0 means unlimited.')
flags.DEFINE_boolean('require_comments', True, 'Only save posts that have at least one top comment.')

logging_path = os.path.split(
    os.path.realpath(__file__))[0] + os.sep + 'logging.conf'
logging.config.fileConfig(logging_path)
logger = logging.getLogger('spider')


class Spider:
    def __init__(self, config):
        """Weibo类初始化"""
        self.filter = config[
            'filter']  # 取值范围为0、1,程序默认值为0,代表要爬取用户的全部微博,1代表只爬取用户的原创微博
        since_date = config['since_date']
        if isinstance(since_date, int):
            since_date = date.today() - timedelta(since_date)
        self.since_date = str(
            since_date)  # 起始时间，即爬取发布日期从该值到结束时间的微博，形式为yyyy-mm-dd
        self.end_date = config[
            'end_date']  # 结束时间，即爬取发布日期从起始时间到该值的微博，形式为yyyy-mm-dd，特殊值"now"代表现在
        random_wait_pages = config['random_wait_pages']
        self.random_wait_pages = [
            min(random_wait_pages),
            max(random_wait_pages)
        ]  # 随机等待频率，即每爬多少页暂停一次
        random_wait_seconds = config['random_wait_seconds']
        self.random_wait_seconds = [
            min(random_wait_seconds),
            max(random_wait_seconds)
        ]  # 随机等待时间，即每次暂停要sleep多少秒
        self.global_wait = config['global_wait']  # 配置全局等待时间，如每爬1000页等待3600秒等
        self.page_count = 0  # 统计每次全局等待后，爬取了多少页，若页数满足全局等待要求就进入下一次全局等待
        self.write_mode = config[
            'write_mode']  # 结果信息保存类型，为list形式，可包含txt、csv、json、mongo和mysql五种类型
        self.pic_download = config[
            'pic_download']  # 取值范围为0、1,程序默认值为0,代表不下载微博原始图片,1代表下载
        self.video_download = config[
            'video_download']  # 取值范围为0、1,程序默认为0,代表不下载微博视频,1代表下载
        self.file_download_timeout = config.get(
            'file_download_timeout',
            [5, 5, 10
             ])  # 控制文件下载“超时”时的操作，值是list形式，包含三个数字，依次分别是最大超时重试次数、最大连接时间和最大读取时间
        self.result_dir_name = config.get(
            'result_dir_name', 0)  # 结果目录名，取值为0或1，决定结果文件存储在用户昵称文件夹里还是用户id文件夹里
        self.cookie = config['cookie']
        self.mysql_config = config.get('mysql_config')  # MySQL数据库连接配置，可以不填

        self.sqlite_config = config.get('sqlite_config')
        self.kafka_config = config.get('kafka_config')
        self.mongo_config = config.get('mongo_config')
        self.post_config = config.get('post_config')
        self.keyword_file_path = ''
        keyword_list = config.get('keyword_list', [])
        if FLAGS.keyword_list:
            keyword_list = FLAGS.keyword_list
        if not isinstance(keyword_list, list):
            if not os.path.isabs(keyword_list):
                keyword_list = os.getcwd() + os.sep + keyword_list
            if not os.path.isfile(keyword_list):
                logger.warning('不存在%s文件', keyword_list)
                sys.exit()
            self.keyword_file_path = keyword_list
            keyword_list = config_util.get_keyword_list(keyword_list)
        if FLAGS.keywords:
            keyword_list = FLAGS.keywords.split(',')
        self.keyword_list = list(dict.fromkeys([k.strip() for k in keyword_list if k]))  # 去重并清理
        self.got_num = 0
        self.session = None
        self.csv_writer = None
        self.jsonl_writer = None
        self.keyword_concurrency = FLAGS.keyword_concurrency
        self.write_lock = None
        self.total_saved_count = 0
        self.total_jsonl_count = 0
        self.target_total_jsonl = FLAGS.target_total_jsonl or 0
        self.require_comments = FLAGS.require_comments
        jitter = config.get('per_request_jitter_seconds', [0, 0])
        self.per_request_jitter_seconds = [min(jitter), max(jitter)]
        self.proxies = config.get('proxies') or []
        self.rotate_on_block = bool(config.get('rotate_on_block', True))
        self.proxy_index = -1
        self.current_proxy = None
        if self.proxies:
            self.proxy_index = 0
            self.current_proxy = self.proxies[0]

    async def write_weibo(self, weibos):
        """将爬取到的信息写入文件或数据库"""
        for downloader in self.downloaders:
            await downloader.download_files(weibos, self.session)
        for writer in self.writers:
            writer.write_weibo(weibos)

    def write_user(self, user):
        """将用户信息写入数据库"""
        for writer in self.writers:
            writer.write_user(user)

    async def get_user_info(self, user_uri):
        """获取用户信息"""
        url = 'https://weibo.cn/%s/profile' % (user_uri)
        selector = await handle_html_async(self.cookie, url, self.session)
        self.user = await IndexParser(self.cookie, user_uri, selector=selector).get_user_async(self.session)
        self.page_count += 1

    async def download_user_avatar(self, user_uri):
        """下载用户头像"""
        # Note: This remains synchronous for now as it's a minor part of the flow
        avatar_album_url = PhotoParser(self.cookie,
                                       user_uri).extract_avatar_album_url()
        pic_urls = AlbumParser(self.cookie,
                               avatar_album_url).extract_pic_urls()
        await AvatarPictureDownloader(
            self._get_filepath('img'),
            self.file_download_timeout).handle_download(pic_urls, self.session)

    async def get_search_info(self, keyword):
        """获取关键词搜索结果"""
        try:
            page = 1
            empty_streak = 0
            page1 = 0
            random_pages = random.randint(*self.random_wait_pages)
            while True:
                if sum(self.per_request_jitter_seconds) > 0:
                    await asyncio.sleep(random.uniform(*self.per_request_jitter_seconds))
                url = f'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D1%26q%3D{keyword}&page_type=searchall&page={page}'
                user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
                headers = {'User-Agent': user_agent, 'Cookie': self.cookie}
                data = None
                for _ in range(3):
                    async with self.session.get(url, headers=headers, proxy=self.current_proxy) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(1)
                            continue
                        try:
                            data = await resp.json()
                        except Exception:
                            content = await resp.text()
                            try:
                                import json as _json
                                data = _json.loads(content)
                            except Exception:
                                data = None
                        if data:
                            break
                if not data or int(data.get('ok', 0)) != 1:
                    empty_streak += 1
                    if empty_streak >= 2:
                        delay = 5
                        logger.warning(u'关键词"%s"连续返回空结果/错误(ok!=1)，可能触发风控。将延时%d秒后继续', keyword, delay)
                        for i in tqdm(range(delay)):
                            await asyncio.sleep(1)
                        if self.rotate_on_block and self.proxies:
                            self._advance_proxy()
                        empty_streak = 0
                        page += 1
                        continue
                    page += 1
                    continue
                cards = data.get('data', {}).get('cards', [])
                weibos = []
                def _extract_mblogs(card):
                    mblogs = []
                    if card.get('mblog'):
                        mblogs.append(card['mblog'])
                    for group in card.get('card_group', []) or []:
                        if group.get('mblog'):
                            mblogs.append(group['mblog'])
                    return mblogs
                mblogs = []
                for c in cards:
                    mblogs.extend(_extract_mblogs(c))
                if not mblogs:
                    empty_streak += 1
                    if empty_streak >= 2:
                        delay = 5
                        logger.warning(u'关键词"%s"连续空页(无搜索结果)，可能触发风控。将延时%d秒后继续', keyword, delay)
                        for i in tqdm(range(delay)):
                            await asyncio.sleep(1)
                        if self.rotate_on_block and self.proxies:
                            self._advance_proxy()
                        empty_streak = 0
                        page += 1
                        continue
                    page += 1
                    continue
                empty_streak = 0
                for mb in mblogs:
                    try:
                        w = Weibo()
                        w.id = str(mb.get('id') or mb.get('mid') or '')
                        u = mb.get('user') or {}
                        w.user_id = str(u.get('id') or '')
                        w.author = u.get('screen_name') or ''
                        w.keyword = keyword
                        # 文本字段去标签
                        text_html = mb.get('text') or ''
                        text_plain = self._clean_html_to_text(text_html)
                        w.content = text_plain
                        page_info = mb.get('page_info') or {}
                        w.article_url = page_info.get('page_url') or page_info.get('url') or ''
                        w.original = False if mb.get('retweeted_status') else True
                        w.publish_time = mb.get('created_at') or ''
                        w.publish_tool = mb.get('source') or ''
                        w.up_num = int(mb.get('attitudes_count') or 0)
                        w.retweet_num = int(mb.get('reposts_count') or 0)
                        w.comment_num = int(mb.get('comments_count') or 0)
                        pics = mb.get('pics') or []
                        pic_urls = []
                        for p in pics:
                            url = ''
                            if isinstance(p, str):
                                url = p
                            elif isinstance(p, dict):
                                url = p.get('url') or (p.get('large') or {}).get('url') or ''
                            if url:
                                pic_urls.append(url)
                        w.original_pictures = ','.join(pic_urls) if pic_urls else '无'
                        w.retweet_pictures = '无'
                        media_info = page_info.get('media_info') or {}
                        w.video_url = media_info.get('stream_url') or media_info.get('mp4_sd_url') or ''
                        weibos.append(w)
                    except Exception as e:
                        logger.exception(e)
                        continue
                logger.info(u'%s已获取关键词"%s"的第%d页搜索结果%s',
                            '-' * 30, keyword, page, '-' * 30)
                yield page, weibos
                self.page_count += 1
                if (page - page1) % random_pages == 0:
                    await asyncio.sleep(random.randint(*self.random_wait_seconds))
                    page1 = page
                    random_pages = random.randint(*self.random_wait_pages)
                if self.page_count >= self.global_wait[0][0]:
                    logger.info(u'即将进入全局等待时间，%d秒后程序继续执行' %
                                self.global_wait[0][1])
                    for i in tqdm(range(self.global_wait[0][1])):
                        await asyncio.sleep(1)
                    self.page_count = 0
                    self.global_wait.append(self.global_wait.pop(0))
                page += 1
        except Exception as e:
            logger.exception(e)

    async def fetch_top_comments(self, weibo_id):
        """获取微博第一页一级评论"""
        try:
            url = f'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}'
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
            headers = {'User-Agent': user_agent, 'Cookie': self.cookie}
            if sum(self.per_request_jitter_seconds) > 0:
                await asyncio.sleep(random.uniform(*self.per_request_jitter_seconds))
            async with self.session.get(url, headers=headers, proxy=self.current_proxy) as resp:
                if resp.status != 200:
                    return []
                data = None
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    try:
                        import json as _json
                        data = _json.loads(text)
                    except Exception:
                        return []
            comments = []
            nodes = ((data or {}).get('data') or {}).get('data') or []
            for c in nodes:
                try:
                    cid = str(c.get('id') or '')
                    user = (c.get('user') or {}).get('screen_name') or ''
                    text_html = c.get('text') or ''
                    content = self._clean_html_to_text(text_html)
                    likes = int(c.get('like_count') or 0)
                    comments.append({
                        'original_post_id': weibo_id,
                        'comment_id': cid,
                        'user': user,
                        'content': content,
                        'likes': likes,
                    })
                except Exception as e:
                    logger.exception(e)
                    continue
            return comments
        except Exception as e:
            logger.exception(e)
            return []

    async def fetch_full_weibo_text(self, weibo_id, force_extend=False):
        try:
            if sum(self.per_request_jitter_seconds) > 0:
                await asyncio.sleep(random.uniform(*self.per_request_jitter_seconds))
            ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
            headers = {'User-Agent': ua, 'Cookie': self.cookie}
            url_show = f'https://m.weibo.cn/statuses/show?id={weibo_id}'
            async with self.session.get(url_show, headers=headers, proxy=self.current_proxy) as resp:
                if resp.status != 200:
                    return ''
                data = None
                try:
                    data = await resp.json()
                except Exception:
                    t = await resp.text()
                    try:
                        import json as _json
                        data = _json.loads(t)
                    except Exception:
                        data = None
            if not data:
                return ''
            d = data.get('data') or {}
            is_long = bool(d.get('isLongText'))
            text_html = d.get('text') or ''
            content_plain = self._clean_html_to_text(text_html)
            if is_long or force_extend:
                await asyncio.sleep(random.uniform(1.5, 3.5))
                if sum(self.per_request_jitter_seconds) > 0:
                    await asyncio.sleep(random.uniform(*self.per_request_jitter_seconds))
                url_ext = f'https://m.weibo.cn/statuses/extend?id={weibo_id}'
                headers2 = {'User-Agent': ua, 'Cookie': self.cookie, 'Referer': f'https://m.weibo.cn/detail/{weibo_id}'}
                async with self.session.get(url_ext, headers=headers2, proxy=self.current_proxy) as resp2:
                    if resp2.status == 200:
                        ext = None
                        try:
                            ext = await resp2.json()
                        except Exception:
                            tt = await resp2.text()
                            try:
                                import json as _json
                                ext = _json.loads(tt)
                            except Exception:
                                ext = None
                        if ext:
                            long_html = (ext.get('data') or {}).get('longTextContent') or ''
                            if long_html:
                                content_plain = self._clean_html_to_text(long_html)
            return content_plain
        except Exception as e:
            logger.exception(e)
            return ''

    def _advance_proxy(self):
        if not self.proxies:
            return
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        self.current_proxy = self.proxies[self.proxy_index]
        logger.warning(u'代理已切换: %s', self.current_proxy)
    def _clean_html_to_text(self, html_text):
        try:
            s = html_text or ''
            s = re.sub(r'(?is)<(script|style)[^>]*>.*?</\\1>', '', s)
            s = re.sub(r'(?i)<br\\s*/?>', '\n', s)
            s = re.sub(r'(?i)</p>', '\n', s)
            s = re.sub(r'(?is)<span[^>]*class=["\\\']url-icon["\\\'][^>]*>.*?</span>', '', s)
            s = re.sub(r'(?is)</?[^>]+>', '', s)
            s = html.unescape(s)
            s = s.replace('网页链接', '')
            s = re.sub(r'[ \\t\\r\\f]+', ' ', s)
            s = re.sub(r'\\n{2,}', '\\n', s)
            return s.strip()
        except Exception:
            return (html_text or '').strip()

    def _get_filepath(self, type):
        """获取结果文件路径"""
        try:
            dir_name = self.user.nickname
            if self.result_dir_name:
                dir_name = self.user.id
            if FLAGS.output_dir is not None:
                file_dir = FLAGS.output_dir + os.sep + dir_name
            else:
                file_dir = (os.getcwd() + os.sep + 'weibo' + os.sep + dir_name)
            if type == 'img' or type == 'video':
                file_dir = file_dir + os.sep + type
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            if type == 'img' or type == 'video':
                return file_dir
            file_path = file_dir + os.sep + self.user.id + '.' + type
            return file_path
        except Exception as e:
            logger.exception(e)

    def _get_search_filepath(self, type, keyword, page):
        """获取搜索结果文件路径"""
        try:
            # Deprecated: 分页面路径，聚合化后不再使用
            dir_name = keyword
            if FLAGS.output_dir is not None:
                file_dir = FLAGS.output_dir + os.sep + dir_name
            else:
                file_dir = (os.getcwd() + os.sep + 'weibo' + os.sep + dir_name)
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            file_path = file_dir + os.sep + f'search_results_page{page}.{type}'
            return file_path
        except Exception as e:
            logger.exception(e)

    def initialize_info(self, user_config):
        """初始化爬虫信息"""
        self.got_num = 0
        self.user_config = user_config
        self.weibo_id_list = []
        if self.end_date == 'now':
            self.new_since_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        else:
            self.new_since_date = self.end_date
        self.writers = []
        if 'csv' in self.write_mode:
            from .writer import CsvWriter

            self.writers.append(
                CsvWriter(self._get_filepath('csv'), self.filter))
        if 'txt' in self.write_mode:
            from .writer import TxtWriter

            self.writers.append(
                TxtWriter(self._get_filepath('txt'), self.filter))
        if 'json' in self.write_mode:
            from .writer import JsonWriter

            self.writers.append(JsonWriter(self._get_filepath('json')))
        if 'mysql' in self.write_mode:
            from .writer import MySqlWriter

            self.writers.append(MySqlWriter(self.mysql_config))
        if 'mongo' in self.write_mode:
            from .writer import MongoWriter

            self.writers.append(MongoWriter(self.mongo_config))
        if 'sqlite' in self.write_mode:
            from .writer import SqliteWriter

            self.writers.append(SqliteWriter(self.sqlite_config))

        if 'kafka' in self.write_mode:
            from .writer import KafkaWriter

            self.writers.append(KafkaWriter(self.kafka_config))

        if 'post' in self.write_mode:
            from .writer import PostWriter

            self.writers.append(PostWriter(self.post_config))

        self.downloaders = []
        if self.pic_download == 1:
            from .downloader import (
                OriginPictureDownloader,
                RetweetPictureDownloader)

            self.downloaders.append(
                OriginPictureDownloader(self._get_filepath('img'),
                                        self.file_download_timeout))
        if self.pic_download and not self.filter:
            self.downloaders.append(
                RetweetPictureDownloader(self._get_filepath('img'),
                                         self.file_download_timeout))
        if self.video_download == 1:
            from .downloader import VideoDownloader

            self.downloaders.append(
                VideoDownloader(self._get_filepath('video'),
                                self.file_download_timeout))

    async def get_one_keyword(self, keyword):
        """获取一个关键词的搜索结果"""
        try:
            async for page, weibos in self.get_search_info(keyword):
                pairs = []
                for wb in weibos:
                    top_comments = await self.fetch_top_comments(wb.id)
                    pairs.append((wb, top_comments))
                weibos_filtered = [wb for wb, cs in pairs if (cs if self.require_comments else True)]
                if self.csv_writer and weibos_filtered:
                    async with self.write_lock:
                        self.csv_writer.write_weibo(weibos_filtered)
                        self.total_saved_count += len(weibos_filtered)
                if self.jsonl_writer:
                    records = []
                    for wb, cs in pairs:
                        if self.require_comments and not cs:
                            continue
                        records.append({
                            'keyword': keyword,
                            'weibo_details': {
                                'id': wb.id,
                                'text': wb.content,
                                'author': wb.author,
                                'stats': {'up': wb.up_num, 're': wb.retweet_num, 'cm': wb.comment_num},
                            },
                            'top_comments': cs if cs else []
                        })
                    if records:
                        async with self.write_lock:
                            self.jsonl_writer.write_weibo(records)
                            self.total_jsonl_count += len(records)
                if self.target_total_jsonl and self.total_jsonl_count >= self.target_total_jsonl:
                    logger.info(u'已达到目标JSONL条数（%d），停止当前关键词抓取', self.target_total_jsonl)
                    break
                self.got_num += len(weibos)
            logger.info(u'关键词"%s"共爬取%d条微博', keyword, self.got_num)
            logger.info(u'关键词"%s"信息抓取完毕', keyword)
        except Exception as e:
            logger.exception(e)
    async def start(self):
        """运行爬虫"""
        try:
            if not self.keyword_list:
                logger.info(u'没有配置有效的关键词，请通过config.json或keywords.txt配置keyword_list或通过命令行传入 --keywords')
                return
            async with aiohttp.ClientSession() as session:
                self.session = session
                # 初始化聚合写入器
                base_out = os.getcwd() + os.sep + 'output'
                if not os.path.isdir(base_out):
                    os.makedirs(base_out)
                self.write_lock = asyncio.Lock()
                if 'csv' in self.write_mode:
                    self.csv_writer = CsvWriter(base_out + os.sep + 'all_weibo_results.csv', self.filter)
                if 'jsonl' in self.write_mode or 'json' in self.write_mode:
                    self.jsonl_writer = JsonlWriter(base_out + os.sep + 'all_weibo_with_comments.jsonl')
                if self.keyword_concurrency and self.keyword_concurrency > 1:
                    sem = asyncio.Semaphore(self.keyword_concurrency)
                    async def _run(k):
                        async with sem:
                            await self.get_one_keyword(k)
                    tasks = [asyncio.create_task(_run(k)) for k in self.keyword_list]
                    await asyncio.gather(*tasks)
                else:
                    kw_count = 0
                    kw_count1 = random.randint(*self.random_wait_pages)
                    random_kws = random.randint(*self.random_wait_pages)
                    for keyword in self.keyword_list:
                        if self.target_total_jsonl and self.total_jsonl_count >= self.target_total_jsonl:
                            break
                        if (kw_count - kw_count1) % random_kws == 0:
                            await asyncio.sleep(random.randint(*self.random_wait_seconds))
                            kw_count1 = kw_count
                            random_kws = random.randint(*self.random_wait_pages)
                        kw_count += 1
                        await self.get_one_keyword(keyword)
                if self.total_saved_count == 0:
                    delay = 120
                    logger.warning(u'未抓取到任何内容，可能触发风控或需要人机验证。将延时%d秒，请在浏览器完成登录/验证以刷新 Cookie：%s',
                                   delay,
                                   'https://passport.weibo.com/sso/signin?entry=wapsso&source=wapssowb&url=https://m.weibo.cn/')
                    for i in tqdm(range(delay)):
                        await asyncio.sleep(1)
        except Exception as e:
            logger.exception(e)


def _get_config():
    """获取config.json数据"""
    src = os.path.split(
        os.path.realpath(__file__))[0] + os.sep + 'config_sample.json'
    config_path = os.getcwd() + os.sep + 'config.json'
    if FLAGS.config_path:
        config_path = FLAGS.config_path
    elif not os.path.isfile(config_path):
        shutil.copy(src, config_path)
        logger.info(u'请先配置当前目录(%s)下的config.json文件，'
                    u'如果想了解config.json参数的具体意义及配置方法，请访问\n'
                    u'https://github.com/dataabc/weiboSpider#2程序设置' % 
                    os.getcwd())
        sys.exit()
    try:
        with open(config_path) as f:
            try:
                config_util.check_cookie(config_path)
            except Exception:
                logger.info("Using the cookie field in config.json as the request cookie.")
            config = json.loads(f.read())
            return config
    except ValueError:
        logger.error(u'config.json 格式不正确，请访问 '
                     u'https://github.com/dataabc/weiboSpider#2程序设置')
        sys.exit()

async def async_main(_):
    try:
        config = _get_config()
        config_util.validate_config(config)
        wb = Spider(config)
        await wb.start()
    except Exception as e:
        logger.exception(e)

def main(_):
    asyncio.run(async_main(_))

if __name__ == '__main__':
    app.run(main)
