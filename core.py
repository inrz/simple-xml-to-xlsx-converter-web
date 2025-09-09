import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict

# ----------- XML helpers (namespace-agnostic) -----------

def localname(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def element_to_dict(el: ET.Element):
    """Element -> Python primitives (strip ns, attrs as @, lists for repeats)."""
    data = {}
    if el.attrib:
        for k, v in el.attrib.items():
            data[f"@{localname(k)}"] = v

    kids = list(el)
    if kids:
        buckets = {}
        for c in kids:
            k = localname(c.tag)
            v = element_to_dict(c)
            if k in buckets:
                if not isinstance(buckets[k], list):
                    buckets[k] = [buckets[k]]
                buckets[k].append(v)
            else:
                buckets[k] = v
        data.update(buckets)
        return data
    else:
        txt = (el.text or "").strip()
        return txt if txt != "" else None

def flatten_dict_all(obj, parent_key: str = "") -> dict:
    """Flatten nested dicts/lists into dotted keys; lists indexed with [i]."""
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                flat.update(flatten_dict_all(v, nk))
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    flat.update(flatten_dict_all(item, f"{nk}[{i}]"))
            else:
                flat[nk] = v
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            flat.update(flatten_dict_all(item, f"{parent_key}[{i}]" if parent_key else f"[{i}]"))
    else:
        if parent_key:
            flat[parent_key] = obj
    return flat

# ----------- Row detection (repeating pattern; no TxId reliance) -----------

def detect_repeating_rows(root: ET.Element):
    """
    Heuristic to find a repeating element (row type) WITHOUT TxId:
      - For each parent, group its direct children by localname.
      - Candidate groups have count >= 2 (repeated siblings).
      - Choose the group with the highest count; tie-breaker = deepest parent.
    Fallbacks: root's children else [root].
    """
    best = {"count": 0, "depth": -1, "name": None, "elems": None}
    stack = [(root, 0)]
    while stack:
        parent, depth = stack.pop()
        children = list(parent)
        if children:
            groups = defaultdict(list)
            for ch in children:
                groups[localname(ch.tag)].append(ch)
            for name, els in groups.items():
                c = len(els)
                if c >= 2:
                    if (c > best["count"]) or (c == best["count"] and depth > best["depth"]):
                        best = {"count": c, "depth": depth, "name": name, "elems": els}
            for ch in children:
                stack.append((ch, depth + 1))
    if best["elems"] is not None:
        return best["elems"]
    rc = list(root)
    return rc if rc else [root]

def xml_rows_to_dataframe(xml_bytes: bytes) -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)
    row_elems = detect_repeating_rows(root)
    rows = []
    for el in row_elems:
        obj = element_to_dict(el)
        flat = flatten_dict_all(obj)
        rows.append(flat)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.reindex(columns=sorted(df.columns))  # deterministic order
    return df