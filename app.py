from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from typing import List, Dict
import pandas as pd
import xml.etree.ElementTree as ET
import io, time, os, threading, zipfile, json
from collections import defaultdict

app = FastAPI(title="Simple XML to Excel Converter (Web)")

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

# ----------- Row detection (repeating pattern) -----------

def detect_repeating_rows(root: ET.Element):
    """
    Find a repeated child tag among siblings (no TxId dependency).
    - For each parent, group its direct children by localname, keep count >=2.
    - Choose highest count; tie-breaker: deepest parent.
    Fall back to root children, else [root].
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

# ----------- In-memory jobs for progress -----------

JOBS: Dict[str, dict] = {}
# JOBS[job_id] = {
#   "total": int, "index": int, "stage": str, "file": str|None,
#   "done": bool, "error": str|None, "zip_bytes": io.BytesIO|None
# }

def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

# ----------- UI (same style) -----------

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
          .progress{height:12px; background:#eee; border-radius:999px; overflow:hidden; margin-top:12px}
          .bar{height:100%; width:0%; background:#16a34a; transition:width .15s}
          .right{text-align:right}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>Simple XML to Excel Converter</h1>
          <p class="muted">Upload one or more XML files. The app auto-detects a repeating element pattern and turns each instance into a row; all nested fields become columns.</p>

          <input id="files" type="file" accept=".xml" multiple />
          <div class="row">
            <button id="startBtn">Convert</button>
            <span class="muted" id="hint">You can select multiple files.</span>
          </div>

          <div class="progress"><div class="bar" id="bar"></div></div>
          <div class="muted" id="status">Idle</div>

          <div class="row" style="justify-content:space-between;">
            <div class="muted" id="selected"></div>
            <div class="right"><a id="downloadLink" style="display:none;" download>⬇️ Download results</a></div>
          </div>
        </div>

        <script>
          const filesInput = document.getElementById('files');
          const startBtn = document.getElementById('startBtn');
          const statusEl = document.getElementById('status');
          const selectedEl = document.getElementById('selected');
          const bar = document.getElementById('bar');
          const downloadLink = document.getElementById('downloadLink');

          filesInput.addEventListener('change', () => {
            const names = Array.from(filesInput.files).map(f => f.name);
            selectedEl.textContent = names.length ? names.join(', ') : '';
          });

          startBtn.addEventListener('click', async () => {
            if (!filesInput.files.length) { alert('Please choose at least one XML file.'); return; }

            const form = new FormData();
            for (const f of filesInput.files) form.append('files', f);

            startBtn.disabled = true;
            bar.style.width = '0%';
            statusEl.textContent = 'Uploading…';
            downloadLink.style.display = 'none';

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

# ----------- API: start / progress / download -----------

@app.post("/start")
async def start(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    # Read to memory (UploadFile streams close after request scope)
    blobs = []
    for f in files:
        if not f.filename.lower().endswith(".xml"):
            raise HTTPException(status_code=400, detail=f"Only .xml files allowed: {f.filename}")
        data = await f.read()
        if len(data) > 50 * 1024 * 1024:
            raise HTTPException(status_code=413, detail=f"File too large: {f.filename}")
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

    t = threading.Thread(target=_run_job, args=(job_id, blobs), daemon=True)
    t.start()
    return {"job_id": job_id}

def _run_job(job_id: str, blobs: List[tuple]):
    try:
        total = len(blobs)
        out_zip = io.BytesIO()
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, (name, data) in enumerate(blobs, start=1):
                JOBS[job_id].update({"index": i, "file": name, "stage": "parsing"})
                # Parse → DataFrame
                df = xml_rows_to_dataframe(data)
                # Write one XLSX per XML
                xlsx_buf = io.BytesIO()
                with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
                    MAX_COLS = 16384
                    if df.shape[1] > MAX_COLS:
                        start = 0; part = 1
                        while start < df.shape[1]:
                            end = min(start + MAX_COLS, df.shape[1])
                            df.iloc[:, start:end].to_excel(writer, index=False, sheet_name=f"Rows_{part}")
                            part += 1; start = end
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
                yield sse("error", {"error": err}); return
            if done and stage == "complete":
                yield sse("done", {"job_id": job_id}); return
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
