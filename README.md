# Simple XML → Excel/CSV/Parquet (Web)

Fast, low‑memory XML converter with a simple web UI. Upload one or more XML files; the app auto‑detects the repeating element, flattens nested fields to dotted columns, and exports to XLSX, CSV, or Parquet. Conversion runs in a Celery worker so the UI stays responsive.

## Highlights

- **Auto row detection**: Finds the most repeated element (namespace‑agnostic)
- **Flatten nested data**: Dotted column names (e.g. `Buyr.AcctOwnr.Id.LEI`)
- **Streaming exports**:
  - CSV: streams rows (no full DataFrame in memory)
  - Parquet: writes in batches via `pyarrow.ParquetWriter`
  - XLSX: uses `openpyxl` write‑only mode
- **Preview first**: Shows first 10 rows per file and a unified column set (preview caps to a configurable number of columns for performance)
- **Multi‑file zips**: Multiple inputs are returned as a single `.zip`

## Tech

- FastAPI (default responses via `orjson`)
- Celery + Redis (broker & result backend)
- lxml (streaming parse), pandas (utility, not required for streaming paths)
- pyarrow, openpyxl

## Install

```bash
git clone https://github.com/<your-username>/simple-xml-to-xlsx-converter-web.git
cd simple-xml-to-xlsx-converter-web
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run (local)

Start Redis (required for Celery). Example via Docker:

```bash
docker run -p 6379:6379 -d redis:7
```

Run the web app:

```bash
uvicorn app:app --reload
```

Run the Celery worker in another terminal:

```bash
set REDIS_URL=redis://localhost:6379/0  # Windows (PowerShell: $env:REDIS_URL="redis://localhost:6379/0")
celery -A tasks.celery worker --loglevel=info
```

Open: `http://127.0.0.1:8000`

## Configuration

- `REDIS_URL` (default `redis://localhost:6379/0`)
- `MAX_PREVIEW_COLUMNS` (default `100`): caps returned column list on preview to keep payload small

## Usage notes

- Preview returns 10 rows per file and a limited union of columns. Hidden columns are still included in export.
- For CSV/Parquet/XLSX, the worker streams rows to keep memory low. Parquet writes in ~20k row batches.
- For XLSX, extremely wide tables may exceed Excel’s 16,384 column limit; prefer CSV/Parquet for very wide datasets.

## Deploy on Render

The repo includes `render.yaml` and `Procfile` for a simple Render setup.

- Build command: `pip install -r requirements.txt`
- Start command (web): `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Background worker: Celery with the same `REDIS_URL`

## Limitations

- Free tiers commonly cap request size (~30–50 MB). The app rejects very large uploads.
- No authentication (public endpoint). Add your own auth/ACL if deploying publicly.

## License

MIT