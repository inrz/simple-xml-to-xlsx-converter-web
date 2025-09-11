import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Iterator, Dict, Any, List

try:
    from lxml import etree as LET
    HAS_LXML = True
except Exception:
    HAS_LXML = False

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
        # Keep original order by default; caller can reindex if needed
        pass
    return df


# ----------- Streaming row generator (lxml.iterparse) -----------

def iter_xml_rows(xml_path: str) -> Iterator[Dict[str, Any]]:
    """Yield flattened row dicts by streaming the XML file.

    Heuristic: same as detect_repeating_rows, but in streaming form we need a
    target tag. We first do a light pass to find the most repeated child tag
    name, then do a second pass yielding each element of that tag.
    Falls back to root children if no repetition found.
    """
    if not HAS_LXML:
        # Fallback: load whole file (non-streaming)
        with open(xml_path, "rb") as f:
            data = f.read()
        df = xml_rows_to_dataframe(data)
        for _, row in df.iterrows():
            yield {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        return

    # First pass: detect candidate row tag by counting sibling repetitions
    best_name = None
    best_count = 0
    context = LET.iterparse(xml_path, events=("start", "end"))
    stack_names: List[str] = []
    sibling_counts_stack: List[defaultdict] = []
    for event, elem in context:
        if event == "start":
            stack_names.append(elem.tag)
            sibling_counts_stack.append(defaultdict(int))
        else:  # end
            # update parent sibling count
            if len(sibling_counts_stack) >= 2:
                parent_counts = sibling_counts_stack[-2]
                local = elem.tag.split('}', 1)[1] if '}' in elem.tag else elem.tag
                parent_counts[local] += 1
                c = parent_counts[local]
                if c >= 2 and c > best_count:
                    best_count = c
                    best_name = local
            # pop stacks when element ends
            stack_names.pop()
            sibling_counts_stack.pop()
            # clear from memory
            elem.clear()
    del context

    # Second pass: stream over chosen tag
    target_local = best_name
    # If we didn't find a repeated tag, we try the first child name under root
    if target_local is None:
        context = LET.iterparse(xml_path, events=("start", "end"))
        root_seen = False
        first_child_local = None
        for event, elem in context:
            if event == "start" and not root_seen:
                # root start
                root_seen = True
            elif event == "start" and root_seen and first_child_local is None:
                first_child_local = elem.tag.split('}', 1)[1] if '}' in elem.tag else elem.tag
                break
        del context
        target_local = first_child_local

    if target_local is None:
        # Give up: no structure, parse whole
        with open(xml_path, "rb") as f:
            data = f.read()
        df = xml_rows_to_dataframe(data)
        for _, row in df.iterrows():
            yield {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        return

    # Now iterate and yield per target element end
    ns_agnostic_suffix = "}" + target_local
    context = LET.iterparse(xml_path, events=("end",))
    for event, elem in context:
        tag = elem.tag
        is_match = False
        if '}' in tag:
            is_match = tag.endswith(ns_agnostic_suffix)
        else:
            is_match = (tag == target_local)
        if is_match:
            obj = element_to_dict(elem)
            flat = flatten_dict_all(obj)
            yield flat
            elem.clear()
    del context