#!/usr/bin/env python3
"""
terraform_log_cli.py

Универсальный CLI для чтения Terraform-style логов (JSONL или plain text),
парсинга записей и сортировки/группировки по выбранной стратегии.

Поддерживаемые сортировки (--sort):
  - none          : сохранить порядок файла (по умолчанию)
  - timestamp     : сортировать по timestamp (None в конец)
  - level         : сортировать по уровню (TRACE..CRITICAL)
  - level_timestamp : сначала уровень, потом timestamp
  - resource      : группировать/сортировать по распознанному ресурсу (heuristic)
  - message       : сортировать по тексту сообщения

Опции вывода (--output):
  - pretty  : человекочитаемый вывод (по умолчанию)
  - jsonl   : вывод json lines (каждая запись как JSON)

Примеры:
  python terraform_log_cli.py /path/to/log.json --sort timestamp --limit 200
  python terraform_log_cli.py /path/to/log.txt --sort resource --output jsonl
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Optional, Union
from typing import Iterable, Iterator, Dict, Any, Optional, List, Tuple
import sys
import os
from hah import *


MAX_DT = datetime(8999, 12, 31, 23, 59, 59,     999999)


# ---- regexes and keys (from earlier versions) ----
ISO_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+\-]\d{2}:?\d{2})?")
ALT_TS_RE = re.compile(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:[.,]\d+)?")
LEVEL_RE = re.compile(r"\b(trace|debug|info|warn|warning|error|critical)\b", re.I)
VERTEX_RE = re.compile(r'vertex\s+"([^"]+)"')
RESOURCE_ID_RE = re.compile(r"\b([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\b")  # heuristic

TIMESTAMP_KEYS = ["@timestamp", "timestamp", "time", "ts", "date"]
LEVEL_KEYS = ["@level", "level", "severity", "lvl"]
MESSAGE_KEYS = ["@message", "message", "msg", "log"]

LEVEL_ORDER = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "WARNING": 3, "ERROR": 4, "CRITICAL": 5}


# ---- low-level helpers ----
def read_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    """Читает JSONL файл построчно, возвращает dict для каждой строки.
    Печатает ошибки парсинга в stderr, но продолжает обработку."""
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[json decode error] line {i}: {e}", file=sys.stderr)
                continue

def _normalize_level(level: Optional[str]) -> Optional[str]:
    if not level:
        return None
    if not isinstance(level, str):
        return str(level)
    m = LEVEL_RE.search(level) 
    return m.group(1).upper() if m else level.strip().upper()


def _normalize_timestamp(ts: Optional[Union[str, datetime]]) -> Optional[str]:
    """
    Нормализует ts и возвращает ISO-строку (datetime.isoformat()).
    - Сохраняет tzinfo, если он был в исходной строке.
    - Если ts отсутствует или пустой — возвращает предыдущую удачную метку (last_ts), если она есть.
    - Если нет previous и парсинг невозможен — возвращает MAX_TS.isoformat() (маркер).
    - Внутренняя переменная-функциональный атрибут _last хранит предыдущую нормализованную ISO-строку.
    """
    MAX_TS = datetime(8999, 12, 31, 23, 59, 59, 999999)

    # Инициализация хранения последней метки времени
    if not hasattr(_normalize_timestamp, "_last"):
        _normalize_timestamp._last = None  # type: ignore[attr-defined]

    # Helper: установить и вернуть значение как ISO-строку
    def _set_and_return(dt_obj: datetime) -> str:
        iso = dt_obj.isoformat()
        _normalize_timestamp._last = iso  # type: ignore[attr-defined]
        return iso

    # 1) Если ts уже datetime -> просто сохранить/вернуть iso
    if isinstance(ts, datetime):
        return _set_and_return(ts)

    # 2) Если ts пустой/None -> вернуть предыдущую метку, иначе MAX
    if ts is None or (isinstance(ts, str) and not ts.strip()):
        if getattr(_normalize_timestamp, "_last", None):
            return _normalize_timestamp._last  # type: ignore[attr-defined]
        # нет предыдущей — вернуть маркер
        marker = MAX_TS.isoformat()
        _normalize_timestamp._last = marker  # type: ignore[attr-defined]
        return marker

    # 3) ts — строка: попытаться распарсить
    s = str(ts).replace(",", ".")
    # replace first space with 'T' if there's no 'T' (to help fromisoformat)
    s = s.replace(" ", "T", 1) if " " in s and "T" not in s else s

    try:
        dt = datetime.fromisoformat(s)
        return _set_and_return(dt)
    except Exception:
        # fallback: alt format YYYY/MM/DD HH:MM:SS
        m = ALT_TS_RE.search(str(ts))
        if m:
            raw = m.group(0).split(",")[0]
            try:
                dt = datetime.strptime(raw, "%Y/%m/%d %H:%M:%S")
                return _set_and_return(dt)
            except Exception:
                # парсинг альт формата не удался — продолжим к следующему шагу
                pass

    # 4) если сюда попали — парсинг не удался:
    # попробовать вернуть previous (если есть), иначе MAX marker
    if getattr(_normalize_timestamp, "_last", None):
        return _normalize_timestamp._last  # type: ignore[attr-defined]

    marker = MAX_TS.isoformat()
    _normalize_timestamp._last = marker  # type: ignore[attr-defined]
    return marker


def _extract_from_dict(d: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Попытаться получить timestamp, level, message из распарсенного JSON-объекта."""
    ts = None
    for k in TIMESTAMP_KEYS:
        if k in d and isinstance(d[k], str) and d[k].strip():
            ts = d[k]
            break
    level = None
    for k in LEVEL_KEYS:
        if k in d and isinstance(d[k], str) and d[k].strip():
            level = d[k]
            break
    msg = None
    for k in MESSAGE_KEYS:
        if k in d and isinstance(d[k], str) and d[k].strip():
            msg = d[k]
            break
    # best-effort message fallback
    if msg is None:
        for v in d.values():
            if isinstance(v, str) and 0 < len(v) < 1000:
                msg = v
                break
    return ts, level, msg


def _find_ts_in_text(s: str) -> Optional[str]:
    m = ISO_TS_RE.search(s)
    if m:
        return m.group(0)
    m = ALT_TS_RE.search(s)
    if m:
        return m.group(0)
    return None


def _find_level_in_text(s: str) -> Optional[str]:
    m = LEVEL_RE.search(s)
    if m:
        return m.group(1).upper()
    return None


def _extract_resource_from_message(msg: Optional[str]) -> Optional[str]:
    if not msg:
        return None
    m = VERTEX_RE.search(msg)
    if m:
        # vertex "t1_vpc_router.default (expand)" -> extract first token before space or parentheses
        candidate = m.group(1)
        # sometimes contains ' (expand)' suffix -> strip
        candidate = candidate.split()[0]
        return candidate
    # fallback: try generic resource id pattern
    m2 = RESOURCE_ID_RE.search(msg)
    if m2:
        return m2.group(1)
    return None


# ---- parsing entry (unified) ----
def parse_entry_from_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    ts_raw, level_raw, msg = _extract_from_dict(d)
    if not ts_raw and isinstance(msg, str):
        ts_raw = _find_ts_in_text(msg)
    if not level_raw and isinstance(msg, str):
        level_raw = _find_level_in_text(msg)
    ts = _normalize_timestamp(ts_raw)
    level = _normalize_level(level_raw)
    resource = _extract_resource_from_message(msg)
    return {"timestamp": ts, "level": level, "message": msg, "json": d, "resource": resource, "raw": json.dumps(d, ensure_ascii=False)}

# ---- top-level file parser (auto-detect) ----
def parse_log_file(path: str) -> Iterator[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".jsonl", ".ndjson", ".json"):
        # try jsonl first
        for obj in read_jsonl(path):
            yield parse_entry_from_dict(obj)
        return
    with open(path, "r", encoding="utf-8") as fh:
        # peek some content
        peek = fh.read(4096)
        fh.seek(0)
        lines = peek.splitlines()
        for obj in read_jsonl(path):
            yield parse_entry_from_dict(obj)
        return


# ---- sorting helpers ----
def _level_key(level: Optional[str]) -> int:
    if not level:
        return 999
    return LEVEL_ORDER.get(level.upper(), 50)


def sort_entries(entries: Iterable[Dict[str, Any]], mode: str = "none") -> List[Dict[str, Any]]:
    arr = list(entries)
    if mode == "none":
        return arr
    if mode == "timestamp":
        def k(e):
            v = e.get("timestamp")
            return (v is None, v)  # None -> goes last
        return sorted(arr, key=k)
    if mode == "level":
        return sorted(arr, key=lambda e: (_level_key(e.get("level")), e.get("timestamp") or ""))
    if mode == "level_timestamp":
        return sorted(arr, key=lambda e: (_level_key(e.get("level")), e.get("timestamp") or ""))
    if mode == "resource":
        # group by resource; entries without resource go last; keep timestamp order inside group
        def k(e):
            res = e.get("resource")
            ts = e.get("timestamp") or ""
            return (res is None, res or "", ts)
        return sorted(arr, key=k)
    if mode == "message":
        return sorted(arr, key=lambda e: (e.get("message") or ""))
    # default fallback
    return arr


# ---- CLI ----



def main_logs_mode(args):
    """Существующая логика из первой main()"""
    try:
        entries_iter = parse_log_file(args.path)
    except FileNotFoundError:
        print(f"File not found: {args.path}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Error opening/reading file: {exc}", file=sys.stderr)
        return 3

    if args.filter_level:
        fl = args.filter_level.strip().upper()
        entries_iter = (e for e in entries_iter if (e.get("level") or "").upper() == fl)

    entries = sort_entries(entries_iter, args.sort)
    
    out_count = 0
    for e in entries:
        if args.limit and out_count >= args.limit:
            break
        if args.output == "pretty":
            ts = e.get("timestamp") or "-"
            lvl = e.get("level") or "-"
            res = e.get("resource") or "-"
            msg = e.get("message") or "-"
            print(f"[{out_count}] {ts} {lvl:7} {res:25} {msg}")
        else:
            print(json.dumps(e, ensure_ascii=False))
        out_count += 1
    return 0




def main_sections_mode(args):

    """Логика из main_sections_cli()"""
    entries, sections = detect_sections_and_augment(args.path)


    if args.mode == "sections":
        print("Sections:")
        for s in sections:
            print(f" id={s['id']} type={s['type']} tf_req_id={s.get('tf_req_id')} start={s['start_index']} end={s.get('end_index')} entries={s.get('entries')}")
    else: 
        count = 0
        for i, e in enumerate(entries):
            sid = e.get("section_id") or "-"
            stype = e.get("section_type") or "-"
            tfid = e.get("tf_req_id") or "-"
            print(f"[{i}] {e.get('timestamp') or '-'} {e.get('level') or '-':7} {sid}/{stype:<8} tf_req_id={tfid} {e.get('message') or e.get('raw') or ''}")
            count += 1
        return 0




def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="terraform_log_cli")
    p.add_argument("path")
    p.add_argument("--sort", choices=["none", "level", "level_timestamp", "resource", "message"], default="none", help="Sorting/grouping strategy")
    p.add_argument("--output", choices=["pretty", "jsonl"], default="pretty", help="Output format")
    p.add_argument("--limit", type=int, default=0, help="Limit output lines (0 = no limit)")
    p.add_argument("--filter-level", type=str, default=None, help="Only show entries with this log level (case-insensitive)")
    p.add_argument("--mode", choices=["logs", "sections", "sections-full"], default="logs", help="Operation mode")
    # p.add_argument("--section-id", type=int, default=None, help="Show only entries for this section id")


    args = p.parse_args(argv)
    if args.mode == "sections" or args.mode == "sections-full":
        return main_sections_mode(args)
    else:  # logs mode
        return main_logs_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())