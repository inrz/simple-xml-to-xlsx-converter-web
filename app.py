from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, FileResponse
from typing import List, Dict
import io, time, os, threading, zipfile, json, uuid
from celery.result import AsyncResult

from core import xml_rows_to_dataframe  # parsing utils live in core.py
from tasks import celery, convert_task   # celery app + background task

app = FastAPI(title="Simple XML to Excel Converter (Web)")

# ----------- In-memory helper for SSE formatting (kept for symmetry) -----------
def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

# ----------- UI (kept same style, plus Preview/Column picker/Format) -----------

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
          body{font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding: 2rem; max-width: 900px; margin:auto;}
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
          /* modal */
          .modal{position:fixed; inset:0; background:rgba(0,0,0,.4); display:none; align-items:center; justify-content:center; padding:1rem;}
          .modal-content{background:#fff; border-radius:12px; padding:1rem; max-width:95vw; max-height:85vh; overflow:auto; box-shadow:0 10px 30px rgba(0,0,0,.2)}
          table{border-collapse:collapse; width:100%; margin-bottom:1rem}
          th,td{border:1px solid #e5e7eb; padding:.35rem; font-size:.9rem; vertical-align:top}
          th{background:#f8fafc; position:sticky; top:0}
          #columnsContainer{max-height:220px; overflow:auto; border:1px solid #e5e7eb; border-radius:8px; padding:.5rem;}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>Simple XML to Excel Converter</h1>
          <p class="muted">Upload one or more XML files. The app auto-detects a repeating element pattern and turns each instance into a row; all nested fields become columns.</p>

          <input id="files" type="file" accept=".xml" multiple />
          <div class="row">
            <button id="previewBtn" type="button">Preview</button>
            <button id="convertBtn" type="button" style="display:none;">Export</button>
            <span class="muted" id="hint">You can select multiple files.</span>
            <div class="right" style="margin-left:auto;"><a id="downloadLink" style="display:none;" download>⬇️ Download results</a></div>
          </div>

          <div id="columnOptions" style="display:none; margin-top: 1rem;">
            <p class="muted">Select columns to include:</p>
            <div id="columnsContainer"></div>
            <p class="muted" style="margin-top:.5rem;">
              Output format:
              <select id="formatSelect">
                <option value="xlsx">XLSX (Excel)</option>
                <option value="csv">CSV</option>
                <option value="parquet">Parquet</option>
              </select>
            </p>
          </div>

          <div class="progress"><div class="bar" id="bar"></div></div>
          <div class="muted" id="status">Idle</div>
        </div>

        <!-- Modal -->
        <div class="modal" id="previewModal">
          <div class="modal-content">
            <h3>Preview (first 10 rows)</h3>
            <div id="previewTables"></div>
            <div class="row" style="justify-content:flex-end;">
              <button id="closePreviewBtn" type="button">Continue</button>
            </div>
          </div>
        </div>

        <script>
          const filesInput = document.getElementById('files');
          const previewBtn = document.getElementById('previewBtn');
          const convertBtn = document.getElementById('convertBtn');
          const statusEl = document.getElementById('status');
          const bar = document.getElementById('bar');
          const downloadLink = document.getElementById('downloadLink');
          const columnOptions = document.getElementById('columnOptions');
          const columnsContainer = document.getElementById('columnsContainer');
          const formatSelect = document.getElementById('formatSelect');
          const previewModal = document.getElementById('previewModal');
          const previewTables = document.getElementById('previewTables');
          const closePreviewBtn = document.getElementById('closePreviewBtn');

          let lastPreviewData = null;

          filesInput.addEventListener('change', () => {
            downloadLink.style.display = 'none';
            statusEl.textContent = 'Idle';
            columnOptions.style.display = 'none';
            convertBtn.style.display = 'none';
            bar.style.width = '0%';
          });

          previewBtn.addEventListener('click', async () => {
            if (!filesInput.files.length) { alert('Please select at least one XML file.'); return; }
            const formData = new FormData();
            for (const f of filesInput.files) formData.append('files', f);
            previewBtn.disabled = true;
            statusEl.textContent = 'Generating preview...';
            try {
              const res = await fetch('/preview', { method: 'POST', body: formData });
              if (!res.ok) throw new Error((await res.json()).detail || 'Preview failed');
              lastPreviewData = await res.json();
            } catch (err) {
              alert('Error: ' + err.message);
              previewBtn.disabled = false; statusEl.textContent = 'Idle'; return;
            }
            // Build preview tables
            previewTables.innerHTML = '';
            lastPreviewData.files.forEach(file => {
              const cols = file.columns;
              const rows = file.rows;
              let tableHTML = '<h4>' + file.name + '</h4>';
              tableHTML += '<div style="overflow:auto; max-height:50vh;"><table><thead><tr>';
              cols.forEach(col => { tableHTML += `<th>${col}</th>`; });
              tableHTML += '</tr></thead><tbody>';
              rows.forEach(row => {
                tableHTML += '<tr>';
                cols.forEach((col, j) => {
                  let cell = row[j]; if (cell === null) cell = '';
                  tableHTML += `<td>${cell}</td>`;
                });
                tableHTML += '</tr>';
              });
              tableHTML += '</tbody></table></div>';
              previewTables.innerHTML += tableHTML;
            });
            // Populate columns
            columnsContainer.innerHTML = '';
            lastPreviewData.columns.forEach(col => {
              const id = 'col_' + col.replace(/[^a-zA-Z0-9_\\-\\.]/g, '_');
              columnsContainer.innerHTML += `<label><input type="checkbox" id="${id}" value="${col}" checked> ${col}</label><br>`;
            });
            // Show modal
            previewModal.style.display = 'flex';
            previewBtn.disabled = false;
          });

          closePreviewBtn.addEventListener('click', () => {
            previewModal.style.display = 'none';
            columnOptions.style.display = 'block';
            convertBtn.style.display = 'inline-block';
            statusEl.textContent = 'Preview ready. Select columns and format, then click Export.';
          });

          convertBtn.addEventListener('click', async () => {
            if (!lastPreviewData) { alert('Please run the preview first.'); return; }
            convertBtn.disabled = true; previewBtn.disabled = true;
            // collect selected cols
            const selectedCols = [];
            lastPreviewData.columns.forEach(col => {
              const id = 'col_' + col.replace(/[^a-zA-Z0-9_\\-\\.]/g, '_');
              const cb = document.getElementById(id);
              if (cb && cb.checked) selectedCols.push(col);
            });
            const format = formatSelect.value;
            statusEl.textContent = 'Starting conversion...'; bar.style.width = '0%';
            downloadLink.style.display = 'none';
            const payload = {
              file_ids: lastPreviewData.files.map(f => f.id),
              file_names: lastPreviewData.files.map(f => f.name),
              columns: selectedCols,
              format: format
            };
            let jobId;
            try {
              const res = await fetch('/convert', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
              });
              if (!res.ok) throw new Error((await res.json()).detail || 'Failed to start conversion');
              const data = await res.json();
              jobId = data.job_id;
            } catch (err) {
              alert('Error: ' + err.message);
              statusEl.textContent = 'Error starting job';
              convertBtn.disabled = false; previewBtn.disabled = false; return;
            }
            statusEl.textContent = 'Processing...';
            // poll status
            const timer = setInterval(async () => {
              const r = await fetch(`/status/${jobId}`);
              if (!r.ok) return;
              const s = await r.json();
              if (s.status === 'PROGRESS') {
                const pct = s.progress || 0; bar.style.width = pct + '%';
                if (s.total && s.total > 1) statusEl.textContent = `Processing ${s.file} (${s.current}/${s.total})`;
                else statusEl.textContent = `Processing... ${pct}%`;
              }
              if (s.status === 'SUCCESS') {
                clearInterval(timer);
                bar.style.width = '100%'; statusEl.textContent = 'Done!';
                downloadLink.href = `/download/${jobId}`; downloadLink.style.display = 'inline';
                convertBtn.disabled = false; previewBtn.disabled = false;
              }
              if (s.status === 'FAILURE') {
                clearInterval(timer);
                statusEl.textContent = 'Error during conversion';
                alert('Conversion failed: ' + (s.error || 'Unknown error'));
                convertBtn.disabled = false; previewBtn.disabled = false;
              }
            }, 1000);
          });
        </script>
      </body>
    </html>
    """

# ----------- Preview endpoint (returns 10 rows + union of columns) -----------

@app.post("/preview")
async def preview_files(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    os.makedirs("temp_uploads", exist_ok=True)
    previews = []
    all_columns = set()
    for upload in files:
        if not upload.filename.lower().endswith(".xml"):
            raise HTTPException(status_code=400, detail=f"Only .xml files allowed: {upload.filename}")
        data = await upload.read()
        if len(data) > 50 * 1024 * 1024:
            raise HTTPException(status_code=413, detail=f"File too large: {upload.filename}")
        try:
            df = xml_rows_to_dataframe(data)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Failed to parse {upload.filename}: {e}")
        preview_df = df.head(10) if not df.empty else df
        preview_df = preview_df.fillna("")
        columns = list(preview_df.columns)
        rows = preview_df.values.tolist()
        file_id = str(uuid.uuid4())
        with open(f"temp_uploads/{file_id}.xml", "wb") as f:
            f.write(data)
        previews.append({"id": file_id, "name": upload.filename, "columns": columns, "rows": rows})
        all_columns.update(columns)
    return {"files": previews, "columns": sorted(all_columns)}

# ----------- Background conversion via Celery -----------

@app.post("/convert")
async def start_conversion(payload: Dict):
    try:
        file_ids: List[str] = payload["file_ids"]
        file_names: List[str] = payload["file_names"]
        columns: List[str] = payload.get("columns", [])
        fmt: str = payload.get("format", "xlsx")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")
    if not file_ids or not file_names or len(file_ids) != len(file_names):
        raise HTTPException(status_code=400, detail="file_ids and file_names must be same length")
    if fmt not in ("xlsx", "csv", "parquet"):
        raise HTTPException(status_code=400, detail="format must be xlsx, csv, or parquet")

    task = convert_task.delay(file_ids, file_names, columns, fmt)
    return {"job_id": task.id}

@app.get("/status/{job_id}")
def get_status(job_id: str):
    res = AsyncResult(job_id, app=celery)
    status = res.status
    if status == "SUCCESS":
        return {"status": "SUCCESS"}
    if status == "FAILURE":
        error_msg = str(res.result)
        return {"status": "FAILURE", "error": error_msg}
    info = res.info or {}
    return {
        "status": "PROGRESS",
        "progress": info.get("progress", 0),
        "current": info.get("current", 0),
        "total": info.get("total", 0),
        "file": info.get("file", "")
    }

@app.get("/download/{job_id}")
def download_result(job_id: str):
    base = f"outputs/{job_id}"
    if os.path.exists(base + ".zip"):
        return FileResponse(base + ".zip", media_type="application/zip", filename="converted_files.zip")
    # single-file outputs
    for ext, mime in [
        ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("csv", "text/csv"),
        ("parquet", "application/vnd.apache.parquet"),
    ]:
        p = f"{base}.{ext}"
        if os.path.exists(p):
            # try to pick friendly filename from result
            res = AsyncResult(job_id, app=celery)
            result_meta = res.result if res.status == "SUCCESS" else {}
            download_name = result_meta.get("filename", f"converted.{ext}")
            return FileResponse(p, media_type=mime, filename=download_name)
    raise HTTPException(status_code=404, detail="Result not found")

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"