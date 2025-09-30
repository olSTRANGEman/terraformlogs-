from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Optional, Union
from typing import Iterable, Iterator, Dict, Any, Optional, List, Tuple
import sys
import os

MAX_DT = datetime(8999, 12, 31, 23, 59, 59, 999999)


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


# ---- Section detection heuristics ----
# start/end regexes for plan/apply sections (heuristic)
_START_PATTERNS = [
    re.compile(r'CLI (command )?args.*\bplan\b', re.I),
    re.compile(r'CLI (command )?args.*\bapply\b', re.I),
    re.compile(r'starting Plan operation', re.I),
    re.compile(r'starting Apply operation', re.I),
    re.compile(r'\bterraform\b.*\bplan\b', re.I),
    re.compile(r'\bterraform\b.*\bapply\b', re.I),
    re.compile(r'backend/local: starting Plan operation', re.I),
]
_END_PATTERNS = [
    re.compile(r'\bPlan:\s*\d+\s*to add', re.I),
    re.compile(r'Apply complete', re.I),
    re.compile(r'No changes\. Infrastructure is up-to-date', re.I),
    re.compile(r'Plan:\s*\d+\s*to add,\s*\d+\s*to change,\s*\d+\s*to destroy', re.I),
    re.compile(r'Plan:\s*0\s*to add,\s*0\s*to change,\s*0\s*to destroy', re.I),
]


def _detect_section_start_type(text: str) -> Optional[str]:
    """Если найден старт секции, вернуть 'plan' или 'apply' (или None)."""
    if not text:
        return None
    t = text.lower()
    for pat in _START_PATTERNS:
        if pat.search(text):
            # determine type by word presence
            if 'apply' in t:
                return 'apply'
            if 'plan' in t:
                return 'plan'
            # fallback: try pattern contents
            p = pat.pattern.lower()
            if 'apply' in p:
                return 'apply'
            if 'plan' in p:
                return 'plan'
            return 'plan'
    return None


def _detect_section_end(text: str) -> Optional[str]:
    """Если найден конец секции, попытаться определить тип (plan/apply) или return 'plan'/'apply' or None."""
    if not text:
        return None
    t = text.lower()
    for pat in _END_PATTERNS:
        if pat.search(text):
            # choose type by content
            if 'apply' in t or 'apply complete' in t:
                return 'apply'
            if 'plan' in t or 'to add' in t:
                return 'plan'
            # default to plan
            return 'plan'
    return None


# ---- top-level file parser (auto-detect) ----
def parse_log_file(path: str) -> Iterator[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    # collect all raw parsed objects first
    parsed_entries: List[Dict[str, Any]] = []
    if ext in (".jsonl", ".ndjson", ".json"):
        for obj in read_jsonl(path):
            parsed_entries.append(parse_entry_from_dict(obj))
    else:
        # fallback: try reading as jsonl as before
        for obj in read_jsonl(path):
            parsed_entries.append(parse_entry_from_dict(obj))

    sections: List[Dict[str, Any]] = []
    # open sections keyed by tf_req_id
    open_by_id: Dict[str, Dict[str, Any]] = {}

    def _close_section_by_id(tfid: str, close_at_idx: int):
        """Закрыть секцию с id=tfid на индексе close_at_idx (последняя запись, которая принадлежала секции)."""
        sec = open_by_id.pop(tfid, None)
        if not sec:
            return
        sec["end_index"] = close_at_idx
        sec["end_ts"] = parsed_entries[close_at_idx].get("timestamp") if 0 <= close_at_idx < len(parsed_entries) else sec.get("start_ts")
        # пометим последнюю запись как end
        parsed_entries[close_at_idx]["section_id"] = sec["id"]
        parsed_entries[close_at_idx]["section_type"] = sec["type"]
        parsed_entries[close_at_idx]["section_event"] = "end"

    # Iterate entries and maintain sections by tf_req_id
    for idx, entry in enumerate(parsed_entries):
        # get tf_req_id if present in json payload
        tfid = None
        j = entry.get("json")
        if isinstance(j, dict):
            # prioritize exact key 'tf_req_id' (user specified). tolerate also 'tf_reqID' variants if you want.
            tfid = j.get("tf_req_id")

        # text blob for heuristics
        text_blob = entry.get("message") or entry.get("raw") or json.dumps(j or {})

        # If this entry has a tf_req_id -> it belongs to that section (start if not seen)
        if tfid:
            # If there are other open sections with different ids -> they must be closed now
            other_ids = [k for k in open_by_id.keys() if k != tfid]
            for other in other_ids:
                # close at previous index (last occurrence of that id was at idx-1)
                close_idx = idx - 1 if idx - 1 >= 0 else 0
                _close_section_by_id(other, close_idx)

            if tfid not in open_by_id:
                # Start a new section for this tfid
                sec_id = len(sections) + 1
                # infer type from start patterns; fallback to end-detection or 'plan'
                stype = _detect_section_start_type(text_blob) or _detect_section_end(text_blob) or "plan"
                sec = {
                    "id": sec_id,
                    "type": stype,
                    "tf_req_id": tfid,
                    "start_index": idx,
                    "start_ts": entry.get("timestamp"),
                    "end_index": None,
                    "end_ts": None,
                    "entries": 0,
                }
                sections.append(sec)
                open_by_id[tfid] = sec
                # mark this entry as start
                entry["section_id"] = sec_id
                entry["section_type"] = stype
                entry["section_event"] = "start"
                sec["entries"] += 1
            else:
                # already open for this id -> mark inside
                sec = open_by_id[tfid]
                entry["section_id"] = sec["id"]
                entry["section_type"] = sec["type"]
                entry["section_event"] = "inside"
                sec["entries"] += 1

            # Also check end-pattern in the same entry: if it indicates end for this type -> close it now
            end_type = _detect_section_end(text_blob)
            if end_type and open_by_id.get(tfid) and open_by_id[tfid]["type"] == end_type:
                # close on current index (end entry included)
                _close_section_by_id(tfid, idx)
            continue  # processed this entry

        # If no tf_req_id present:
        # We must NOT continue assigning it to previously open tf_req_id sections.
        # So close all open sections now (they ended at previous index).
        if open_by_id:
            # close each open on last index they were seen (we approximate as idx-1)
            for open_id in list(open_by_id.keys()):
                close_idx = idx - 1 if idx - 1 >= 0 else 0
                _close_section_by_id(open_id, close_idx)

        # fallback: previous heuristic (start/end detection without tfid)
        # detect start
        start_type = _detect_section_start_type(text_blob)
        if start_type:
            sec_id = len(sections) + 1
            sec = {
                "id": sec_id,
                "type": start_type,
                "tf_req_id": None,
                "start_index": idx,
                "start_ts": entry.get("timestamp"),
                "end_index": None,
                "end_ts": None,
                "entries": 1,
            }
            sections.append(sec)
            # Note: we DO NOT put into open_by_id (no tfid) — but keep as synthetic open by index in a separate variable
            # To remain simple, treat non-tfid sections as "open" in a temp var
            # store temporary open section id under special key
            open_by_id[f"_anon_{sec_id}"] = sec
            entry["section_id"] = sec_id
            entry["section_type"] = start_type
            entry["section_event"] = "start"
            # continue to next (we may detect end on same entry below)
        else:
            # if currently inside an anon open section, assign it
            anon_keys = [k for k in open_by_id.keys() if k.startswith("_anon_")]
            if anon_keys:
                # pick last anon
                ak = anon_keys[-1]
                sec = open_by_id[ak]
                entry["section_id"] = sec["id"]
                entry["section_type"] = sec["type"]
                entry["section_event"] = "inside"
                sec["entries"] += 1

        # detect end patterns: if match, try to close section of same type
        end_type = _detect_section_end(text_blob)
        if end_type:
            # prefer closing a real tfid section of that type (shouldn't exist here because we closed them earlier),
            # otherwise close the latest anon section of that type
            # find latest open (including anon) matching type
            candidates = [k for k, v in open_by_id.items() if v["type"] == end_type]
            if candidates:
                last_key = candidates[-1]
                sec = open_by_id[last_key]
                # close on current index
                sec["end_index"] = idx
                sec["end_ts"] = entry.get("timestamp")
                # mark current entry as end
                entry["section_id"] = sec["id"]
                entry["section_type"] = sec["type"]
                entry["section_event"] = "end"
                # remove from open_by_id
                open_by_id.pop(last_key, None)

    # end for entries: close any still-open sections (they end on last entry where seen)
    if open_by_id:
        last_idx = len(parsed_entries) - 1
        for key, sec in list(open_by_id.items()):
            # if section already has end_index set skip (but should not)
            if sec.get("end_index") is None:
                sec["end_index"] = last_idx
                sec["end_ts"] = parsed_entries[last_idx].get("timestamp")
                # mark last entry as end (if not already set)
                parsed_entries[last_idx]["section_id"] = sec["id"]
                parsed_entries[last_idx]["section_type"] = sec["type"]
                parsed_entries[last_idx]["section_event"] = parsed_entries[last_idx].get("section_event") or "end"
            open_by_id.pop(key, None)

    # Save sections for CLI use
    parse_log_file._sections = sections  # type: ignore[attr-defined]

    # yield augmented entries
    for e in parsed_entries:
        yield e


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
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="terraform_log_cli", description="Parse and sort Terraform-style logs")
    p.add_argument("path", help="Path to log file (JSONL or plain text)")
    p.add_argument("--sort", choices=["none", "timestamp", "level", "level_timestamp", "resource", "message"],
                   default="none", help="Sorting/grouping strategy")
    p.add_argument("--output", choices=["pretty", "jsonl"], default="pretty", help="Output format")
    p.add_argument("--limit", type=int, default=0, help="Limit output lines (0 = no limit)")
    p.add_argument("--filter-level", type=str, default=None, help="Only show entries with this log level (case-insensitive)")
    p.add_argument("--sections", action="store_true", help="Show detected plan/apply sections summary and exit")
    p.add_argument("--section-id", type=int, default=None, help="If set, only show entries that belong to this section id (as detected)")
    args = p.parse_args(argv)

    try:
        # parse_log_file now fills parse_log_file._sections
        entries_list = list(parse_log_file(args.path))
        sections = getattr(parse_log_file, "_sections", [])
    except FileNotFoundError:
        print(f"File not found: {args.path}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Error opening/reading file: {exc}", file=sys.stderr)
        return 3

    # If user asked for sections summary, print and exit
    if args.sections:
        if not sections:
            print("No plan/apply sections detected.")
            return 0
        # print table-like summary
        print("Detected sections:")
        for s in sections:
            start_idx = s.get("start_index")
            end_idx = s.get("end_index")
            start_ts = s.get("start_ts") or "-"
            end_ts = s.get("end_ts") or "-"
            print(f"- id={s['id']} type={s['type']} start_idx={start_idx} end_idx={end_idx} start_ts={start_ts} end_ts={end_ts} entries={s.get('entries',0)}")
        return 0

    # build iterator from parsed entries
    entries_iter = iter(entries_list)

    # apply filter-level early
    if args.filter_level:
        fl = args.filter_level.strip().upper()
        entries_iter = (e for e in entries_iter if (e.get("level") or "").upper() == fl)

    # filter by section id if provided
    if args.section_id is not None:
        sid = args.section_id
        entries_iter = (e for e in entries_iter if e.get("section_id") == sid)

    # collect and sort
    entries = sort_entries(entries_iter, args.sort)

    # output
    out_count = 0
    for e in entries:
        if args.limit and out_count >= args.limit:
            break
        if args.output == "pretty":
            ts = e.get("timestamp") or "-"
            lvl = e.get("level") or "-"
            res = e.get("resource") or "-"
            msg = e.get("message") or "-"
            sect = f"{e.get('section_type')}#{e.get('section_id')}" if e.get("section_id") else "-"
            print(f"[{out_count}] {ts} {lvl:7} {res:25} {sect:12} {msg}")
        else:
            # jsonl
            print(json.dumps(e, ensure_ascii=False))
        out_count += 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
