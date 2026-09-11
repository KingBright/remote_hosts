#!/usr/bin/env python3
"""Validate the canonical issue register and render its Markdown view."""
import argparse
import collections
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATUSES = {'open', 'partial', 'implemented_unverified', 'candidate_verified', 'blocked_external', 'closed'}
LABELS = {'open':'未修复', 'partial':'部分修复', 'implemented_unverified':'已实现待验', 'candidate_verified':'候选已验证', 'blocked_external':'外部阻塞', 'closed':'已验收关闭'}

def validate(data):
    if data.get('schema_version') != 1:
        raise ValueError('unsupported backlog schema')
    items = data['items']
    ids = [item['id'] for item in items]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate issue ID')
    by_id = {item['id']: item for item in items}
    for item in items:
        if not re.fullmatch(r'RH-\d{3}', item['id']):
            raise ValueError('invalid issue ID')
        if item['priority'] not in ('P0', 'P1', 'P2') or item['status'] not in STATUSES:
            raise ValueError(item['id'] + ': invalid priority/status')
        for key in ('title', 'problem', 'discovery', 'current_resolution', 'owner', 'target_version', 'acceptance', 'evidence'):
            if not item.get(key):
                raise ValueError(item['id'] + ': missing ' + key)
        if item['status'] == 'closed' and not item.get('closure_evidence'):
            raise ValueError(item['id'] + ': closing requires acceptance evidence')
        if any(dep not in by_id or dep == item['id'] for dep in item['depends_on']):
            raise ValueError(item['id'] + ': invalid dependency')
    visited, active = set(), set()
    def walk(key):
        if key in active:
            raise ValueError('dependency cycle at ' + key)
        if key in visited:
            return
        active.add(key)
        for dep in by_id[key]['depends_on']:
            walk(dep)
        active.remove(key)
        visited.add(key)
    for key in by_id:
        walk(key)
    return collections.Counter(item['status'] for item in items)

def render(data):
    counts = validate(data)
    lines = ['# Remote Hosts 产品问题清单', '',
             '> 唯一事实源：`docs/product/backlog.json`。本页由 `scripts/product-backlog.py --render` 生成。', '',
             '更新日期：' + data['updated_at'] + '。共 ' + str(len(data['items'])) + ' 项。', '',
             '已验证候选不等于线上修复；部分修复不能关闭整项。关闭必须附本项验收证据。', '',
             '状态汇总：' + '；'.join(LABELS[key] + ' ' + str(counts[key]) for key in LABELS if counts[key]) + '。', '',
             '## 版本规划', '']
    for milestone in data['milestones']:
        lines += ['**' + milestone['version'] + '**：' + milestone['goal'], '']
    lines += ['## 问题索引', '', '| ID | 优先级 | 状态 | 目标版本 | 问题 |', '|---|---|---|---|---|']
    for item in data['items']:
        lines.append('| {id} | {priority} | {status} | {target_version} | {title} |'.format(**dict(item, status=LABELS[item['status']])))
    lines += ['', '## 逐项验收', '']
    for item in data['items']:
        lines += ['### ' + item['id'] + ' · ' + item['title'], '',
                  '**' + item['priority'] + ' / ' + LABELS[item['status']] + ' / ' + item['target_version'] + '**', '',
                  '现象与范围：' + item['problem'], '', '当前处理：' + item['current_resolution'], '',
                  '验收：' + '；'.join(item['acceptance']), '',
                  '证据或实现位置：' + '、'.join('`' + e + '`' for e in item['evidence']), '',
                  '依赖：' + ('、'.join(item['depends_on']) or '无'), '']
        if item.get('closure_evidence'):
            lines += ['关闭证据：' + '、'.join('`' + e + '`' for e in item['closure_evidence']), '']
    return '\n'.join(lines)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=pathlib.Path, default=ROOT)
    parser.add_argument('--render', action='store_true')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    source = args.root / 'docs/product/backlog.json'
    data = json.loads(source.read_text())
    counts = validate(data)
    expected = render(data)
    view = source.with_name('BACKLOG.md')
    if args.render:
        view.write_text(expected)
    if args.check and (not view.exists() or view.read_text() != expected):
        raise SystemExit('BACKLOG.md is stale; run --render')
    print(json.dumps({'issues':len(data['items']), 'statuses':dict(counts), 'valid':True}, ensure_ascii=False))

if __name__ == '__main__':
    main()
