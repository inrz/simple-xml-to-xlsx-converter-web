from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, FileResponse
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse
from typing import List, Dict
import io, time, os, threading, zipfile, json, uuid, csv, tempfile

from core import xml_rows_to_dataframe, iter_xml_rows  # parsing utils live in core.py
import pyarrow as pa
import pyarrow.parquet as pq
from openpyxl import Workbook

app = FastAPI(title="Simple XML to Excel Converter (Web)", default_response_class=ORJSONResponse)

# In-memory job registry for background threads
jobs = {}
jobs_lock = threading.Lock()

def _update_job(job_id: str, **kwargs):
    with jobs_lock:
        job = jobs.get(job_id, {})
        job.update(kwargs)
        jobs[job_id] = job

def _run_conversion(job_id: str, file_ids: List[str], file_names: List[str], columns: List[str], output_format: str):
    try:
        os.makedirs("outputs", exist_ok=True)
        total = len(file_ids)
        _update_job(job_id, status="PROGRESS", progress=0, current=0, total=total, file="")

        def stream_csv_to_path(xml_path: str, out_path: str, header_cols: List[str] | None):
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = None
                if header_cols is not None:
                    header = list(header_cols)
                    if not header:
                        return
                    writer = csv.DictWriter(f, fieldnames=header, extrasaction='ignore')
                    writer.writeheader()
                    for r in iter_xml_rows(xml_path):
                        if any((r.get(k) is not None and r.get(k) != "") for k in header):
                            writer.writerow({k: r.get(k) for k in header})
                else:
                    first_row = None
                    for r in iter_xml_rows(xml_path):
                        first_row = r
                        break
                    if first_row is None or not first_row:
                        return
                    header = list(first_row.keys())
                    writer = csv.DictWriter(f, fieldnames=header, extrasaction='ignore')
                    writer.writeheader()
                    if any((first_row.get(k) is not None and first_row.get(k) != "") for k in header):
                        writer.writerow({k: first_row.get(k) for k in header})
                    for r in iter_xml_rows(xml_path):
                        if any((r.get(k) is not None and r.get(k) != "") for k in header):
                            writer.writerow({k: r.get(k) for k in header})

        def stream_parquet_to_path(xml_path: str, out_path: str, header_cols: List[str] | None):
            header = header_cols if header_cols else None
            writer = None
            batch_size = 20000
            rows_buffer = []
            for r in iter_xml_rows(xml_path):
                if header is None:
                    header = list(r.keys())
                rows_buffer.append({k: r.get(k) for k in header})
                if len(rows_buffer) >= batch_size:
                    table = pa.Table.from_pylist(rows_buffer)
                    if writer is None:
                        writer = pq.ParquetWriter(out_path, table.schema)
                    writer.write_table(table)
                    rows_buffer.clear()
            if rows_buffer:
                table = pa.Table.from_pylist(rows_buffer)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema)
                writer.write_table(table)
                rows_buffer.clear()
            if writer is not None:
                writer.close()

        def stream_xlsx_to_path(xml_path: str, out_path: str, header_cols: List[str] | None):
            wb = Workbook(write_only=True)
            ws = wb.create_sheet("Rows")
            header = header_cols if header_cols else None
            first_row = None
            it = iter_xml_rows(xml_path)
            for r in it:
                first_row = r
                break
            if header is None:
                header = list(first_row.keys()) if first_row else []
            if not header:
                wb.save(out_path)
                wb.close()
                return
            ws.append(header)
            if first_row is not None:
                row_vals = [first_row.get(k) for k in header]
                if any((v is not None and v != "") for v in row_vals):
                    ws.append(row_vals)
            for r in iter_xml_rows(xml_path):
                row_vals = [r.get(k) for k in header]
                if any((v is not None and v != "") for v in row_vals):
                    ws.append(row_vals)
            wb.save(out_path)
            wb.close()

        if total > 1:
            zip_path = f"outputs/{job_id}.zip"
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for idx, (fid, fname) in enumerate(zip(file_ids, file_names), start=1):
                    xml_path = f"temp_uploads/{fid}.xml"
                    if not os.path.exists(xml_path):
                        raise FileNotFoundError(f"Missing uploaded file: {xml_path}")
                    base = os.path.splitext(os.path.basename(fname))[0]
                    _update_job(job_id, current=idx, file=fname, progress=int((idx - 1) / total * 100))
                    tmp = None
                    if output_format == "csv":
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv"); tmp.close()
                        stream_csv_to_path(xml_path, tmp.name, columns if columns else None)
                        zf.write(tmp.name, arcname=f"{base}.csv")
                    elif output_format == "parquet":
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet"); tmp.close()
                        stream_parquet_to_path(xml_path, tmp.name, columns if columns else None)
                        zf.write(tmp.name, arcname=f"{base}.parquet")
                    else:
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx"); tmp.close()
                        stream_xlsx_to_path(xml_path, tmp.name, columns if columns else None)
                        zf.write(tmp.name, arcname=f"{base}.xlsx")
                    try:
                        os.remove(tmp.name)
                    except Exception:
                        pass
                    _update_job(job_id, current=idx, file=fname, progress=int((idx) / total * 100))
            _update_job(job_id, status="SUCCESS", result_path=zip_path, result_filename="converted_files.zip")
        else:
            fid, fname = file_ids[0], file_names[0]
            xml_path = f"temp_uploads/{fid}.xml"
            if not os.path.exists(xml_path):
                raise FileNotFoundError(f"Missing uploaded file: {xml_path}")
            base = os.path.splitext(os.path.basename(fname))[0]
            _update_job(job_id, current=1, total=1, file=fname, progress=0)
            if output_format == "csv":
                out_path = f"outputs/{job_id}.csv"
                stream_csv_to_path(xml_path, out_path, columns if columns else None)
                result_name = f"{base}.csv"
            elif output_format == "parquet":
                out_path = f"outputs/{job_id}.parquet"
                stream_parquet_to_path(xml_path, out_path, columns if columns else None)
                result_name = f"{base}.parquet"
            else:
                out_path = f"outputs/{job_id}.xlsx"
                stream_xlsx_to_path(xml_path, out_path, columns if columns else None)
                result_name = f"{base}.xlsx"
            _update_job(job_id, status="SUCCESS", result_path=out_path, result_filename=result_name, progress=100)
    except Exception as e:
        _update_job(job_id, status="FAILURE", error=str(e))

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
            <p class="muted" id="columnsNotice" style="display:none; margin-top:.25rem;"></p>
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
            // Show columns limit notice if applicable
            const notice = document.getElementById('columnsNotice');
            if (lastPreviewData.hidden_columns_count && lastPreviewData.hidden_columns_count > 0) {
              notice.style.display = 'block';
              notice.textContent = `${lastPreviewData.hidden_columns_count} additional columns hidden in preview.`;
            } else {
              notice.style.display = 'none';
              notice.textContent = '';
            }
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
              if (!res.ok) {
                let msg = '';
                try { const j = await res.json(); msg = j.detail || JSON.stringify(j); }
                catch (_) { msg = await res.text(); }
                throw new Error(msg || 'Failed to start conversion');
              }
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
              if (!r.ok) {
                try { const t = await r.text(); statusEl.textContent = `Status error: ${t}`; } catch(_) {}
                return;
              }
              let s;
              try { s = await r.json(); }
              catch(_) { statusEl.textContent = 'Status parse error'; return; }
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

@app.head("/")
async def index_head():
    return PlainTextResponse("", status_code=200)

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
    # Limit number of columns returned to frontend for performance
    MAX_PREVIEW_COLUMNS = int(os.getenv("MAX_PREVIEW_COLUMNS", "100"))
    all_columns_list = list(all_columns)
    all_columns_list.sort()
    limited_columns = all_columns_list[:MAX_PREVIEW_COLUMNS]
    hidden_count = max(0, len(all_columns_list) - len(limited_columns))
    return {"files": previews, "columns": limited_columns, "hidden_columns_count": hidden_count}

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

    job_id = str(uuid.uuid4())
    _update_job(job_id, status="QUEUED", progress=0)
    t = threading.Thread(target=_run_conversion, args=(job_id, file_ids, file_names, columns, fmt), daemon=True)
    t.start()
    return {"job_id": job_id}

@app.get("/status/{job_id}")
def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status = job.get("status", "PROGRESS")
    if status == "SUCCESS":
        return {"status": "SUCCESS"}
    if status == "FAILURE":
        return {"status": "FAILURE", "error": job.get("error", "Unknown error")}
    return {
        "status": "PROGRESS",
        "progress": job.get("progress", 0),
        "current": job.get("current", 0),
        "total": job.get("total", 0),
        "file": job.get("file", "")
    }

@app.get("/download/{job_id}")
def download_result(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "SUCCESS":
        raise HTTPException(status_code=400, detail="Job not completed")
    path = job.get("result_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Result not found")
    filename = job.get("result_filename") or os.path.basename(path)
    # infer mime
    if path.endswith(".zip"):
        mime = "application/zip"
    elif path.endswith(".xlsx"):
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif path.endswith(".csv"):
        mime = "text/csv"
    elif path.endswith(".parquet"):
        mime = "application/vnd.apache.parquet"
    else:
        mime = "application/octet-stream"
    return FileResponse(path, media_type=mime, filename=filename)

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"