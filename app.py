from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
import pandas as pd
import xml.etree.ElementTree as ET
import io
import time
from collections import defaultdict, Counter

app = FastAPI(title="Simple XML to Excel Converter (Web)")

# ----------- XML helpers (namespace-agnostic) -----------

def localname(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def element_to_dict(el: ET.Element):
    """
    Recursively convert an Element to Python primitives.
    - Namespace stripped from tag names
    - Attributes become keys with '@' prefix
    - Repeated child tags become lists
    - Leaf text -> str (or None)
    """
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
    """
    Flatten nested dicts/lists into dotted keys; lists indexed with [i].
    """
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

# ----------- Row detection (repeating pattern) -----------

def detect_repeating_rows(root: ET.Element):
    """
    Find the most repeated child tag among siblings anywhere in the tree.
    Returns the list of matching elements to treat as rows.
    Heuristic:
      - For each parent, group its direct children by localname.
      - Keep groups with count >= 2 (repeated).
      - Pick the group with the highest count; tie-breaker = deepest parent.
    Fallbacks:
      - If nothing repeats, use root's direct children (if any), else [root].
    """
    best = {
        "count": 0,
        "depth": -1,
        "name": None,
        "elems": None,
    }

    # DFS stack to compute depth
    stack = [(root, 0)]
    while stack:
        parent, depth = stack.pop()
        children = list(parent)
        if children:
            # group children by local localname
            groups = defaultdict(list)
            for ch in children:
                groups[localname(ch.tag)].append(ch)
            # evaluate repeating groups
            for name, els in groups.items():
                c = len(els)
                if c >= 2:  # repeating siblings → candidate row set
                    if (c > best["count"]) or (c == best["count"] and depth > best["depth"]):
                        best = {"count": c, "depth": depth, "name": name, "elems": els}
            # continue traversal
            for ch in children:
                stack.append((ch, depth + 1))

    if best["elems"] is not None:
        return best["elems"]

    # fallbacks
    rc = list(root)
    if rc:
        return rc
    return [root]

def xml_rows_to_dataframe(xml_bytes: bytes) -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)
    row_elems = detect_repeating_rows(root)

    rows = []
    for el in row_elems:
        obj = element_to_dict(el)   # nested dict for the row
        flat = flatten_dict_all(obj)
        rows.append(flat)

    df = pd.DataFrame(rows)

    # If there's an obvious id/key field (e.g., *Id or id), move it first.
    # This is optional; purely cosmetic.
    if not df.empty:
        preferred = None
        for key in df.columns:
            lk = key.lower()
            if lk.endswith("id") or lk == "id" or lk.endswith(".id"):
                preferred = key
                break
        if preferred:
            cols = [preferred] + [c for c in df.columns if c != preferred]
            df = df.reindex(columns=cols)

    return df

# ----------- FastAPI routes -----------

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>Simple XML to Excel Converter</title>
        <style>
          body{font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding: 2rem; max-width: 720px; margin:auto;}
          .card{border:1px solid #e5e7eb; border-radius:12px; padding:1.5rem; box-shadow:0 1px 2px rgba(0,0,0,.04)}
          h1{margin-top:0}
          input[type=file]{padding:.75rem; border:1px solid #d1d5db; border-radius:8px; width:100%;}
          button{background:#16a34a; color:white; border:none; padding:.75rem 1rem; border-radius:8px; cursor:pointer;}
          button:hover{background:#15803d}
          .muted{color:#6b7280; font-size:.9rem}
          .row{display:flex; gap:1rem; align-items:center; margin-top:1rem}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>Simple XML to Excel Converter</h1>
          <p class="muted">Upload an XML file. The app auto-detects a repeating element pattern and turns each instance into a row; all nested fields become columns.</p>
          <form action="/convert" method="post" enctype="multipart/form-data">
            <input type="file" name="file" accept=".xml" required />
            <div class="row">
              <button type="submit">Convert to Excel</button>
              <span class="muted">Free-tier tip: keep files &lt;~30–50MB.</span>
            </div>
          </form>
        </div>
      </body>
    </html>
    """

@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xml"):
        raise HTTPException(status_code=400, detail="Please upload a .xml file")

    xml_bytes = await file.read()
    if len(xml_bytes) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large for free tier (limit ~50MB).")

    try:
        df = xml_rows_to_dataframe(xml_bytes)
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Invalid XML file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion error: {e}")

    # Write in-memory Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        MAX_COLS = 16384
        if df.shape[1] > MAX_COLS:
            start = 0
            part = 1
            while start < df.shape[1]:
                end = min(start + MAX_COLS, df.shape[1])
                df.iloc[:, start:end].to_excel(writer, index=False, sheet_name=f"Rows_{part}")
                part += 1
                start = end
        else:
            df.to_excel(writer, index=False, sheet_name="Rows")

    output.seek(0)
    stamp = int(time.time())
    base_filename = file.filename.rsplit(".", 1)[0] if file.filename else "converted"
    filename = f"{base_filename}_{stamp}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"