# app.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, Response, FileResponse
from typing import List, Dict
import pandas as pd
import xml.etree.ElementTree as ET
import io, time, os, json, threading, zipfile
from collections import defaultdict
import logging, sys, json

# ---------------- Logging ----------------
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger("xml2xlsx")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Example usage
logger.info("service_started")
logger.info(json.dumps({"event": "job_start", "job_id": job_id, "files": len(blobs)}))
logger.error(json.dumps({"event": "job_error", "job_id": job_id, "error": str(e)}))

app = FastAPI(title="Simple XML to Excel Converter (Web)")

# ---------------- XML helpers (namespace-agnostic) ----------------

def localname(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def element_to_dict(el: ET.Element):
    """
    Element -> Python primitives:
      - strip namespaces
      - attributes => '@attr'
      - repeated child tags => list
      - leaf text => str or None
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

def detect_repeating_rows(root: ET.Element):
    """
    Heuristic to find a repeating element (row type):
      - For each parent, group its direct children by localname.
      - Candidate groups have count >= 2 (repeated siblings).
      - Choose the group with the highest count; tie-breaker = deepest parent.
    Fallbacks:
      - If nothing repeats, use root's direct children, else [root].
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
    if rc:
        return rc
    return [root]

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
        df = df.reindex(columns=sorted(df.columns))
    return df

def apply_aliases(df: pd.DataFrame, aliases: Dict[str, str]) -> pd.DataFrame:
    """aliases: { 'Original.Path': 'New Header', ... }"""
    if not aliases or df.empty:
        return df
    rename_map = {k: v for k, v in aliases.items() if k in df.columns and v}
    return df.rename(columns=rename_map) if rename_map else df

# ---------------- Job registry for progress (in-memory) ----------------

JOBS: Dict[str, dict] = {}
# JOB:
# {
#   'total': int,
#   'index': int,
#   'stage': str,
#   'file': str|None,
#   'done': bool,
#   'error': str|None,
#   'zip_bytes': io.BytesIO|None
# }

def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

# ---------------- HTML (GUI) ----------------

INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Simple XML → Excel Converter (Web)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;color:#111;margin:24px}
    .card{border:1px solid #e5e7eb;border-radius:12px;padding:16px;max-width:980px}
    h1{margin:0 0 6px}
    .muted{color:#6b7280}
    .row{margin:12px 0}
    input[type=file]{padding:8px;border:1px solid #d1d5db;border-radius:8px;width:100%}
    textarea{width:100%;min-height:120px;font-family:ui-monospace,Menlo,Consolas,monospace}
    button{background:#2563eb;color:#fff;border:0;padding:10px 14px;border-radius:8px;cursor:pointer}
    button:disabled{opacity:.6;cursor:not-allowed}
    .progress{height:12px;background:#eee;border-radius:999px;overflow:hidden}
    .bar{height:100%;width:0%;background:#16a34a;transition:width .15s}
    .right{text-align:right}
    .pill{display:inline-block;background:#eef2ff;color:#3730a3;padding:2px 8px;border-radius:999px;font-size:12px}
    .files{font-size:.9em;margin-top:6px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    @media(max-width:900px){.grid{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="card">
    <h1>Simple XML → Excel Converter</h1>
    <div class="muted">Upload one or more XML files. The app auto-detects the repeating element (no TxId needed), flattens nested fields into columns, and returns a ZIP of Excel files.</div>

    <div class="row">
      <strong>1) Upload XML files</strong><br/>
      <input id="files" type="file" accept=".xml" multiple />
      <div class="files" id="fileList"></div>
    </div>

    <div class="row">
      <strong>2) Column mapping (optional)</strong> <span class="pill">aliases JSON</span>
      <div class="muted">Example: {"Tx.TradDt": "Trade Date", "FinInstrm.Othr.FinInstrmGnlAttrbts.FullNm": "Instrument Name"}</div>
      <textarea id="aliases" placeholder='{"Tx.TradDt": "Trade Date"}'></textarea>
    </div>

    <div class="row grid">
      <div><button id="startBtn">Start Conversion</button></div>
      <div class="right"><a id="downloadLink" style="display:none" download>⬇️ Download results</a></div>
    </div>

    <div class="row">
      <div class="progress"><div class="bar" id="bar"></div></div>
      <div id="status" class="muted" style="margin-top:6px">Idle</div>
    </div>
  </div>

<script>
const filesInput = document.getElementById('files');
const fileList = document.getElementById('fileList');
const startBtn = document.getElementById('startBtn');
const aliasesEl = document.getElementById('aliases');
const bar = document.getElementById('bar');
const statusEl = document.getElementById('status');
const downloadLink = document.getElementById('downloadLink');

filesInput.addEventListener('change', () => {
  const names = Array.from(filesInput.files).map(f => f.name);
  fileList.textContent = names.length ? names.join(', ') : 'No files selected';
});

startBtn.addEventListener('click', async () => {
  if (!filesInput.files.length) { alert('Please choose at least one XML file.'); return; }

  let aliases = {};
  const txt = aliasesEl.value.trim();
  if (txt) {
    try { aliases = JSON.parse(txt); } catch(e) { alert('Aliases must be valid JSON.'); return; }
  }

  const form = new FormData();
  for (const f of filesInput.files) form.append('files', f);
  form.append('aliases', JSON.stringify(aliases));

  startBtn.disabled = true; bar.style.width = '0%'; statusEl.textContent = 'Uploading…'; downloadLink.style.display='none';

  const r = await fetch('/start', { method: 'POST', body: form });
  if (!r.ok) { alert('Failed to start job'); startBtn.disabled = false; return; }
  const { job_id } = await r.json();

  const es = new EventSource(`/progress/${job_id}`);
  es.addEventListener('update', (ev) => {
    const info = JSON.parse(ev.data);
    const { total, index, stage, file, percent } = info;
    bar.style.width = `${percent}%`;
    statusEl.textContent = `${stage} – ${index}/${total} ${file || ''}`;
  });
  es.addEventListener('done', (ev) => {
    es.close();
    const data = JSON.parse(ev.data);
    const url = `/download/${data.job_id}`;
    downloadLink.href = url;
    downloadLink.style.display = 'inline-block';
    statusEl.textContent = 'Done!';
    bar.style.width = '100%';
    startBtn.disabled = false;
  });
  es.addEventListener('error', (ev) => {
    es.close();
    const data = JSON.parse(ev.data);
    alert('Error: ' + data.error);
    statusEl.textContent = 'Error';
    startBtn.disabled = false;
  });
});
</script>
</body>
</html>
"""

# ---------------- Routes ----------------

@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML

@app.post("/start")
async def start(files: List[UploadFile] = File(...), aliases: str = Form("{}")):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    try:
        alias_map = json.loads(aliases) if aliases else {}
    except Exception:
        raise HTTPException(status_code=400, detail="Aliases must be valid JSON")

    # Read files to bytes now (UploadFile streams will close)
    blobs = []
    for f in files:
        if not f.filename.lower().endswith(".xml"):
            raise HTTPException(status_code=400, detail=f"Only .xml files allowed: {f.filename}")
        data = await f.read()
        blobs.append((f.filename, data))

    job_id = str(int(time.time() * 1000))
    JOBS[job_id] = {
        "total": len(blobs),
        "index": 0,
        "stage": "queued",
        "file": None,
        "done": False,
        "error": None,
        "zip_bytes": None,
    }

    t = threading.Thread(target=_run_job, args=(job_id, blobs, alias_map), daemon=True)
    t.start()
    return {"job_id": job_id}

def _run_job(job_id: str, blobs: List[tuple], aliases: Dict[str, str]):
    try:
        total = len(blobs)
        out_zip = io.BytesIO()
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, (name, data) in enumerate(blobs, start=1):
                JOBS[job_id].update({"index": i, "file": name, "stage": "parsing"})
                # parse and flatten
                df = xml_rows_to_dataframe(data)
                # apply aliases
                df = apply_aliases(df, aliases)
                # write xlsx
                xlsx_buf = io.BytesIO()
                with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
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
                xlsx_buf.seek(0)
                base = os.path.splitext(os.path.basename(name))[0]
                zf.writestr(f"{base}.xlsx", xlsx_buf.read())
                JOBS[job_id]["stage"] = "written"
        out_zip.seek(0)
        JOBS[job_id].update({"zip_bytes": out_zip, "done": True, "stage": "complete"})
    except Exception as e:
        JOBS[job_id].update({"error": str(e), "done": True, "stage": "error"})

@app.get("/progress/{job_id}")
async def progress(job_id: str):
    if job_id not in JOBS:
        return StreamingResponse(iter([sse("error", {"error": "unknown job"})]), media_type="text/event-stream")

    def gen():
        total = JOBS[job_id]["total"]
        while True:
            j = JOBS[job_id]
            idx = j["index"]
            stage = j["stage"]
            fname = j["file"]
            done = j["done"]
            err = j["error"]
            percent = int((idx / total) * 100) if total else 0
            if err:
                yield sse("error", {"error": err})
                return
            if done and stage == "complete":
                yield sse("done", {"job_id": job_id})
                return
            yield sse("update", {"total": total, "index": idx, "stage": stage, "file": fname, "percent": percent})
            time.sleep(0.4)

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/download/{job_id}")
async def download(job_id: str):
    j = JOBS.get(job_id)
    if not j or not j.get("zip_bytes"):
        raise HTTPException(status_code=404, detail="Not ready")
    return StreamingResponse(
        j["zip_bytes"],
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="converted_excels.zip"'},
    )

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"
