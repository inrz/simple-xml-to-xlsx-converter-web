import os, io, zipfile
from typing import List
import pandas as pd
from celery import Celery, current_task

from core import xml_rows_to_dataframe

# Use REDIS_URL env var if present (Render: set in both web & worker services)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.update(task_track_started=True)

@celery.task(bind=True)
def convert_task(self, file_ids: List[str], file_names: List[str], columns: List[str], output_format: str):
    """
    Convert uploaded XMLs (by saved file_id) into chosen format.
    If multiple files -> create a zip; single file -> write single artifact.
    Returns JSON with 'filename' for friendly download name.
    """
    os.makedirs("outputs", exist_ok=True)
    total = len(file_ids)
    job_id = self.request.id

    if total > 1:
        zip_path = f"outputs/{job_id}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for idx, (fid, fname) in enumerate(zip(file_ids, file_names), start=1):
                xml_path = f"temp_uploads/{fid}.xml"
                with open(xml_path, "rb") as f:
                    data = f.read()
                df = xml_rows_to_dataframe(data)
                if columns:
                    keep = [c for c in columns if c in df.columns]
                    df = df[keep] if keep else df
                base = os.path.splitext(os.path.basename(fname))[0]
                if output_format == "csv":
                    zf.writestr(f"{base}.csv", df.to_csv(index=False).encode("utf-8"))
                elif output_format == "parquet":
                    buf = io.BytesIO()
                    df.to_parquet(buf, index=False)
                    buf.seek(0)
                    zf.writestr(f"{base}.parquet", buf.read())
                else:  # xlsx
                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        MAX_COLS = 16384
                        if df.shape[1] > MAX_COLS:
                            start = 0; part = 1
                            while start < df.shape[1]:
                                end = min(start + MAX_COLS, df.shape[1])
                                df.iloc[:, start:end].to_excel(writer, index=False, sheet_name=f"Rows_{part}")
                                part += 1; start = end
                        else:
                            df.to_excel(writer, index=False, sheet_name="Rows")
                    buf.seek(0)
                    zf.writestr(f"{base}.xlsx", buf.read())

                # progress
                percent = int((idx / total) * 100)
                current_task.update_state(state="PROGRESS", meta={
                    "current": idx, "total": total, "file": fname, "progress": percent
                })
        return {"filename": "converted_files.zip"}

    # Single file
    fid, fname = file_ids[0], file_names[0]
    xml_path = f"temp_uploads/{fid}.xml"
    with open(xml_path, "rb") as f:
        data = f.read()
    df = xml_rows_to_dataframe(data)
    if columns:
        keep = [c for c in columns if c in df.columns]
        df = df[keep] if keep else df
    base = os.path.splitext(os.path.basename(fname))[0]
    if output_format == "csv":
        out_path = f"outputs/{job_id}.csv"
        df.to_csv(out_path, index=False)
        result_name = f"{base}.csv"
    elif output_format == "parquet":
        out_path = f"outputs/{job_id}.parquet"
        df.to_parquet(out_path, index=False)
        result_name = f"{base}.parquet"
    else:
        out_path = f"outputs/{job_id}.xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            MAX_COLS = 16384
            if df.shape[1] > MAX_COLS:
                start = 0; part = 1
                while start < df.shape[1]:
                    end = min(start + MAX_COLS, df.shape[1])
                    df.iloc[:, start:end].to_excel(writer, index=False, sheet_name=f"Rows_{part}")
                    part += 1; start = end
            else:
                df.to_excel(writer, index=False, sheet_name="Rows")
        result_name = f"{base}.xlsx"
    return {"filename": result_name}