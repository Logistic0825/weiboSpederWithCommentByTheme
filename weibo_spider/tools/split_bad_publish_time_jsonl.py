import os
import json
import argparse


def split_file(path: str):
    path = os.path.abspath(path)
    base_dir = os.path.dirname(path)
    bad_dir = base_dir + '_bad_case'
    os.makedirs(bad_dir, exist_ok=True)
    bad_path = os.path.join(bad_dir, os.path.basename(path))
    tmp_path = path + '.tmp'
    with open(path, 'r', encoding='utf-8') as fin, \
         open(tmp_path, 'w', encoding='utf-8') as fout_good, \
         open(bad_path, 'w', encoding='utf-8') as fout_bad:
        for line in fin:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                fout_good.write(s + '\n')
                continue
            d = obj.get('weibo_details') or {}
            publish_time = d.get('publish_time')
            if publish_time:
                fout_good.write(json.dumps(obj, ensure_ascii=False) + '\n')
            else:
                fout_bad.write(json.dumps(obj, ensure_ascii=False) + '\n')
    os.replace(tmp_path, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inputs', nargs='+', required=True)
    args = ap.parse_args()
    for p in args.inputs:
        split_file(p)


if __name__ == '__main__':
    main()
