# simple-xml-to-xlsx-converter-web
📄 Simple XML to Excel Converter (Web)

Convert complex XML files into Excel spreadsheets right in your browser.
This project uses FastAPI + pandas + openpyxl and auto-detects repeating patterns in XML to decide which elements become rows, with all nested fields flattened into columns.

🚀 Features

🌐 Web interface – upload XML, get .xlsx back instantly

🔍 Auto-detection of repeating elements – no schema assumptions needed

🧩 Flattened structure – nested tags become dotted column names (Buyr.AcctOwnr.Id.LEI)

📑 Handles wide XML – splits across multiple sheets if Excel’s 16,384 column limit is exceeded

⚡ Lightweight FastAPI app – deployable to free hosting (e.g. Render)


🛠 Installation (Local)

Clone repo:

git clone https://github.com/<your-username>/simple-xml-to-xlsx-converter-web.git
cd simple-xml-to-xlsx-converter-web


Create a virtual environment:

python -m venv .venv
source .venv/bin/activate   # macOS/Linux
.venv\Scripts\activate      # Windows


Install dependencies:

pip install -r requirements.txt


Run the app:

uvicorn app:app --reload


Open in browser: http://127.0.0.1:8000

🌐 Deploy on Render

This project is ready for Render free tier:

Push code to GitHub.

Log in to Render
.

Create New Web Service → connect your repo.

Confirm defaults:

Build command: pip install -r requirements.txt

Start command: uvicorn app:app --host 0.0.0.0 --port $PORT

Deploy 🚀

⚠️ Limitations

Free tiers typically limit file size (safe ≤ 30–50 MB).

Parsing very deep XML may flatten into tens of thousands of columns → Excel limits apply.

No authentication: anyone with the URL can use it (see Improvements).