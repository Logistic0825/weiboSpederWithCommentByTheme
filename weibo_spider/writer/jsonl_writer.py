import json
import logging
import os

from .writer import Writer

logger = logging.getLogger('spider.jsonl_writer')


class JsonlWriter(Writer):
    def __init__(self, file_path):
        self.file_path = file_path
        # Ensure directory exists
        d = os.path.dirname(self.file_path)
        if d and not os.path.isdir(d):
            os.makedirs(d)

    def write_user(self, user):
        self.user = user

    def write_weibo(self, records):
        """records: list of dict; append each as JSON line"""
        try:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + '\n')
            logger.info(u'%d条微博+评论写入JSONL文件完毕，保存路径：%s', len(records), self.file_path)
        except Exception as e:
            logger.exception(e)
