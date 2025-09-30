# -------------------- section detection wrapper (uses existing functions) --------------------
from typing import MutableMapping
from terraform_log_cli import *




# simple word-based detectors (used as fallback)
def _detect_type_by_words(msg: Optional[str]) -> Optional[str]:
    if not msg:
        return None
    s = msg.lower()
    # prefer explicit markers
    if "apply complete" in s or "apply operation" in s or re.search(r"\bapply\b", s):
        return "apply"
    if re.search(r"\bplan\b", s) or "plan:" in s or "to add" in s:
        return "plan"
    # no decision
    return None

# end patterns for message-based ending (fallback)
def _is_end_message_for_type(msg: Optional[str], t: str) -> bool:
    if not msg:
        return False
    s = msg.lower()
    if t == "plan":
        return bool(re.search(r"plan:.*to add|to change|to destroy", s) or re.search(r"no changes", s))
    if t == "apply":
        return "apply complete" in s or "apply finished" in s
    return False

def detect_sections_and_augment(path: str):
    """
    Использует read_jsonl + parse_entry_from_dict (не меняя их) для
    парсинга и определения секций. Возвращает (entries, sections).
    - entries: list of entries augmented with section_id/section_type/section_event/tf_req_id
    - sections: list of section metadata dicts
    """
    entries: List[Dict[str, Any]] = []
    sections: List[Dict[str, Any]] = []
    # map tf_req_id -> section dict (open)
    open_by_tf: MutableMapping[str, Dict[str, Any]] = {}
    # track last seen index of each tfid
    last_seen_index: Dict[str, int] = {}

    # Read raw JSON objects via read_jsonl, parse via parse_entry_from_dict
    idx = -1
    for obj in read_jsonl(path):
        idx += 1
        entry = parse_entry_from_dict(obj)
        # preserve original json object if needed
        entry_json = obj if isinstance(obj, dict) else entry.get("json")
        # attempt to find tf_req_id in original json (prioritize)
        tfid = None
        if isinstance(entry_json, dict):
            # common key name
            tfid = entry_json.get("tf_req_id")
            # fallback: also accept 'tf_reqID' or 'tfReqId' variants (case-insensitive)
            if tfid is None:
                for k in entry_json.keys():
                    if k.lower() == "tf_req_id" and entry_json.get(k):
                        tfid = entry_json.get(k)
                        break

        entry["tf_req_id"] = tfid

        # assign default section fields
        entry.pop("section_id", None)
        entry.pop("section_type", None)
        entry.pop("section_event", None)

        # If tfid present -> belongs to that section (create if necessary)
        if tfid:
            last_seen_index[tfid] = idx
            # close other sections whose ids stopped? we will not aggressively close here to allow interleaving,
            # but user asked to stop writing to section when id stops coming: so when we encounter a non-tfid line later, we'll close.
            if tfid not in open_by_tf:
                # start section
                sec_id = len(sections) + 1
                stype = _detect_type_by_words(entry.get("message")) or "plan"
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
                open_by_tf[tfid] = sec
                entry["section_id"] = sec_id
                entry["section_type"] = stype
                entry["section_event"] = "start"
                sec["entries"] += 1
            else:
                sec = open_by_tf[tfid]
                entry["section_id"] = sec["id"]
                entry["section_type"] = sec["type"]
                entry["section_event"] = "inside"
                sec["entries"] += 1

            # quick-end detection by message: if entry itself signals end for its type, close immediately
            end_candidate = _detect_type_by_words(entry.get("message"))
            if end_candidate and _is_end_message_for_type(entry.get("message"), end_candidate):
                # close this tfid section now
                sec = open_by_tf.pop(tfid)
                sec["end_index"] = idx
                sec["end_ts"] = entry.get("timestamp")
                entry["section_event"] = "end"
            entries.append(entry)
            continue

        # If no tfid in this line -> close all open sections whose last_seen_index < idx (i.e., id stopped appearing).
        # We close them at their last seen index so that end_index points to last line with the id.
        if open_by_tf:
            to_close = []
            for otf, sec in open_by_tf.items():
                last_idx = last_seen_index.get(otf, None)
                if last_idx is None or last_idx < idx:
                    to_close.append(otf)
            for otf in to_close:
                sec = open_by_tf.pop(otf)
                li = last_seen_index.get(otf, idx - 1 if idx - 1 >= 0 else 0)
                sec["end_index"] = li
                sec["end_ts"] = entries[li].get("timestamp") if 0 <= li < len(entries) else sec.get("start_ts")
                # mark the end entry if exists
                if 0 <= li < len(entries):
                    entries[li]["section_id"] = sec["id"]
                    entries[li]["section_type"] = sec["type"]
                    entries[li]["section_event"] = "end"

        # Fallback: try to detect start by words if no tfid
        stype = _detect_type_by_words(entry.get("message"))
        if stype:
            sec_id = len(sections) + 1
            sec = {
                "id": sec_id,
                "type": stype,
                "tf_req_id": None,
                "start_index": idx,
                "start_ts": entry.get("timestamp"),
                "end_index": None,
                "end_ts": None,
                "entries": 1,
            }
            sections.append(sec)
            # treat as anonymous open section; we'll close by end-pattern or when id logic forces closure
            anon_key = f"_anon_{sec_id}"
            open_by_tf[anon_key] = sec
            entry["section_id"] = sec_id
            entry["section_type"] = stype
            entry["section_event"] = "start"
            entries.append(entry)
            # also check if this same message contains an immediate end indicator
            if _is_end_message_for_type(entry.get("message"), stype):
                # close immediately
                sec["end_index"] = idx
                sec["end_ts"] = entry.get("timestamp")
                entry["section_event"] = "end"
                open_by_tf.pop(anon_key, None)
            continue

        # If we are inside an anon section, attach this entry
        anon_keys = [k for k in open_by_tf.keys() if k.startswith("_anon_")]
        if anon_keys:
            last_anon = anon_keys[-1]
            sec = open_by_tf[last_anon]
            entry["section_id"] = sec["id"]
            entry["section_type"] = sec["type"]
            entry["section_event"] = "inside"
            sec["entries"] += 1

        # If message indicates closing a section (fallback)
        end_type = None
        # check if message contains any ending indicator
        if entry.get("message"):
            if re.search(r"plan:\s*\d+\s*to add", entry["message"], re.I) or re.search(r"no changes", entry["message"], re.I):
                end_type = "plan"
            if "apply complete" in (entry["message"] or "").lower():
                end_type = "apply"
        if end_type:
            # try close most recent open section of that type
            candidate_keys = [k for k, v in open_by_tf.items() if v["type"] == end_type]
            if candidate_keys:
                last_k = candidate_keys[-1]
                sec = open_by_tf.pop(last_k)
                sec["end_index"] = idx
                sec["end_ts"] = entry.get("timestamp")
                entry["section_id"] = sec["id"]
                entry["section_type"] = sec["type"]
                entry["section_event"] = "end"

        entries.append(entry)

    # End of file: close remaining open sections at last seen index (or last entry)
    if open_by_tf:
        last_idx = len(entries) - 1
        for key, sec in list(open_by_tf.items()):
            if sec.get("end_index") is None:
                # prefer last_seen_index for tfid keys, else use last_idx
                if key.startswith("_anon_"):
                    close_idx = last_idx
                else:
                    tfk = key
                    close_idx = last_seen_index.get(tfk, last_idx)
                sec["end_index"] = close_idx
                sec["end_ts"] = entries[close_idx].get("timestamp") if 0 <= close_idx < len(entries) else sec.get("start_ts")
                # mark last entry as end if not already
                if 0 <= close_idx < len(entries):
                    entries[close_idx]["section_id"] = sec["id"]
                    entries[close_idx]["section_type"] = sec["type"]
                    entries[close_idx]["section_event"] = entries[close_idx].get("section_event") or "end"
            open_by_tf.pop(key, None)

    return entries, sections
