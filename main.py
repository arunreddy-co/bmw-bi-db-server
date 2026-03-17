"""
BMW BI Dashboard — DB Microserver
Runs on Render.com (free tier). Handles all SQLite operations so n8n
never needs npm packages. Everything uses Python's stdlib sqlite3.

Endpoints:
  GET  /health          — liveness check
  GET  /stats           — KPI aggregates (avg price, count, mpg, mileage)
  GET  /schema          — current table schema + sample values for Groq prompt
  POST /execute-sql     — run a SELECT query, return rows as JSON
  POST /upload-csv      — replace inventory table with user-uploaded CSV
"""

import os, re, csv, io, json, sqlite3, tempfile, logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── Logging ────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────
DB_PATH     = Path(os.getenv('DB_PATH', r'C:\Users\\' + os.environ['USERNAME'] + r'\OneDrive\Desktop\bmw-dashboard\bmw.db'))
CSV_PATH    = Path(os.getenv('CSV_PATH', r'C:\Users\\' + os.environ['USERNAME'] + r'\OneDrive\Desktop\bmw-dashboard\BMW_Vehicle_Inventory.csv'))
API_TOKEN   = os.getenv("API_TOKEN", "")        # optional — set in Render env vars
MAX_ROWS    = int(os.getenv("MAX_RESULT_ROWS", "500"))

app = FastAPI(title="BMW BI DB Server", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth (optional bearer token) ───────────────────────────────────
def check_token(request: Request):
    if not API_TOKEN:
        return  # no auth configured — open access
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

# ── DB helpers ─────────────────────────────────────────────────────
@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def db_exists() -> bool:
    if not DB_PATH.exists():
        return False
    with get_db() as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inventory'")
        return cur.fetchone() is not None

# ── CSV parser (handles the Safari WebArchive wrapper) ─────────────
def parse_bmw_csv(raw: bytes) -> tuple[list[str], list[list]]:
    """Extract CSV rows from the Safari WebArchive wrapper."""
    idx = raw.find(b"model")
    if idx == -1:
        raise ValueError("Could not find 'model' column in CSV")

    text = raw[idx:].decode("utf-8", errors="replace")
    text = text.split("</pre>")[0].split("<br")[0]
    lines = text.strip().split("\r\n")

    # Fix header — strip HTML artefacts
    header = re.sub(r"^[^m]*", "", lines[0]).strip().strip('"')
    lines[0] = header

    reader = csv.DictReader(io.StringIO("\r\n".join(lines)))
    rows_raw = list(reader)

    # Handle trailing quote on model column name
    model_key = next((k for k in rows_raw[0].keys() if "model" in k.lower()), None)
    if not model_key:
        raise ValueError(f"No model column found. Keys: {list(rows_raw[0].keys())}")

    columns = ["model", "year", "price", "transmission", "mileage",
               "fuelType", "tax", "mpg", "engineSize"]

    rows = []
    for r in rows_raw:
        def f(v):
            v = v.strip() if v else ""
            return float(v) if v else None
        def i(v):
            v = v.strip() if v else ""
            return int(float(v)) if v else None
        try:
            rows.append([
                r[model_key].strip(),
                i(r["year"]), f(r["price"]), r["transmission"].strip(),
                f(r["mileage"]), r["fuelType"].strip(), f(r["tax"]),
                f(r["mpg"]), f(r["engineSize"]),
            ])
        except Exception:
            pass

    return columns, rows

def parse_generic_csv(raw: bytes) -> tuple[list[str], list[list]]:
    """Parse any generic CSV file."""
    text = raw.decode("utf-8", errors="replace")
    lines = text.replace("\r\n", "\n").replace("\r", "\n").strip().split("\n")

    def parse_line(line):
        cols, cur, in_q = [], "", False
        for ch in line:
            if ch == '"':
                in_q = not in_q
            elif ch == "," and not in_q:
                cols.append(cur.strip())
                cur = ""
            else:
                cur += ch
        cols.append(cur.strip())
        return cols

    raw_headers = parse_line(lines[0])
    headers = [re.sub(r"[^a-zA-Z0-9_]", "_", h.strip()) or f"col_{i}"
               for i, h in enumerate(raw_headers)]

    def infer_type(values):
        non_empty = [v for v in values if v.strip()]
        if not non_empty:
            return "TEXT"
        if all(v.replace(".", "", 1).replace("-", "", 1).isdigit() for v in non_empty):
            if all("." not in v for v in non_empty):
                return "INTEGER"
            return "REAL"
        return "TEXT"

    data_lines = [parse_line(l) for l in lines[1:] if l.strip()]
    col_types = [infer_type([r[i] if i < len(r) else "" for r in data_lines])
                 for i in range(len(headers))]

    def cast(v, t):
        v = v.strip()
        if not v:
            return None
        if t == "INTEGER":
            try: return int(float(v))
            except: return None
        if t == "REAL":
            try: return float(v)
            except: return None
        return v

    rows = []
    for r in data_lines:
        if len(r) == len(headers):
            rows.append([cast(r[i], col_types[i]) for i in range(len(headers))])

    return headers, rows, col_types

def load_csv_into_db(columns, rows, col_types=None):
    """Create or replace the inventory table from parsed CSV data."""
    if col_types is None:
        col_types = ["TEXT", "INTEGER", "REAL", "TEXT", "REAL", "TEXT", "REAL", "REAL", "REAL"]

    schema_def = ", ".join(f"{c} {t}" for c, t in zip(columns, col_types))

    with get_db() as conn:
        conn.execute("DROP TABLE IF EXISTS inventory")
        conn.execute(f"CREATE TABLE inventory ({schema_def})")
        placeholders = ", ".join(["?"] * len(columns))
        conn.executemany(f"INSERT INTO inventory VALUES ({placeholders})", rows)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]

    log.info(f"Loaded {count} rows into inventory table")
    return count

def build_schema_info() -> dict:
    """Build schema metadata for Groq prompt context."""
    with get_db() as conn:
        cur = conn.execute("PRAGMA table_info(inventory)")
        cols = [{"name": row["name"], "type": row["type"]} for row in cur.fetchall()]

        total = conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]
        samples, ranges = {}, {}

        for col in cols:
            n, t = col["name"], col["type"].upper()
            if t == "TEXT":
                vals = [r[0] for r in conn.execute(
                    f"SELECT DISTINCT TRIM({n}) FROM inventory WHERE {n} IS NOT NULL ORDER BY {n} LIMIT 20"
                ).fetchall()]
                samples[n] = vals
            else:
                row = conn.execute(
                    f"SELECT MIN({n}), MAX({n}), ROUND(AVG({n}),1) FROM inventory"
                ).fetchone()
                ranges[n] = {"min": row[0], "max": row[1], "avg": row[2]}

    return {
        "tableName": "inventory",
        "totalRows": total,
        "columns": cols,
        "categoricalSamples": samples,
        "numericRanges": ranges,
    }

# ── Startup: load BMW CSV ──────────────────────────────────────────
@app.on_event("startup")
async def startup():
    if db_exists():
        with get_db() as conn:
            count = conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]
        log.info(f"DB already loaded — {count} rows")
        return

    if not CSV_PATH.exists():
        log.warning(f"CSV not found at {CSV_PATH} — DB will be empty until /upload-csv is called")
        return

    log.info(f"Loading {CSV_PATH} ...")
    raw = CSV_PATH.read_bytes()
    try:
        columns, rows = parse_bmw_csv(raw)
        col_types = ["TEXT","INTEGER","REAL","TEXT","REAL","TEXT","REAL","REAL","REAL"]
    except Exception as e:
        log.error(f"BMW parser failed: {e} — trying generic parser")
        columns, rows, col_types = parse_generic_csv(raw)

    count = load_csv_into_db(columns, rows, col_types)
    log.info(f"Startup complete — {count} rows loaded")

# ── Routes ─────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "db_exists": db_exists()}

@app.get("/stats", dependencies=[Depends(check_token)])
def stats():
    if not db_exists():
        raise HTTPException(503, "Database not initialised. POST to /upload-csv first.")

    with get_db() as conn:
        def scalar(sql):
            r = conn.execute(sql).fetchone()
            return r[0] if r else None

        # Try BMW-specific columns, fall back to first numeric columns
        try:
            avg_price   = scalar("SELECT ROUND(AVG(price),0) FROM inventory")
            total       = scalar("SELECT COUNT(*) FROM inventory")
            avg_mpg     = scalar("SELECT ROUND(AVG(mpg),1) FROM inventory")
            avg_mileage = scalar("SELECT ROUND(AVG(mileage),0) FROM inventory")

            return {
                "avg_price":   f"£{int(avg_price or 0):,}",
                "total":       f"{int(total or 0):,}",
                "avg_mpg":     str(avg_mpg or "—"),
                "avg_mileage": f"{int(avg_mileage or 0):,}",
            }
        except Exception:
            # Generic fallback for non-BMW datasets
            total = scalar("SELECT COUNT(*) FROM inventory")
            return {
                "avg_price": "—", "total": f"{int(total or 0):,}",
                "avg_mpg": "—",   "avg_mileage": "—",
            }

@app.get("/schema", dependencies=[Depends(check_token)])
def schema():
    if not db_exists():
        raise HTTPException(503, "Database not initialised.")
    return build_schema_info()

class SqlRequest(BaseModel):
    sql: str

@app.post("/execute-sql", dependencies=[Depends(check_token)])
def execute_sql(body: SqlRequest):
    if not db_exists():
        raise HTTPException(503, "Database not initialised.")

    sql = body.sql.strip()

    # Safety: SELECT only
    if not re.match(r"^\s*SELECT\b", sql, re.IGNORECASE):
        raise HTTPException(400, "Only SELECT statements are permitted.")

    # Block destructive keywords even inside CTEs
    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "ATTACH"]
    sql_upper = sql.upper()
    for kw in blocked:
        if re.search(rf"\b{kw}\b", sql_upper):
            raise HTTPException(400, f"Keyword '{kw}' is not allowed.")

    try:
        with get_db() as conn:
            cur = conn.execute(sql)
            col_names = [desc[0] for desc in (cur.description or [])]
            raw_rows  = cur.fetchmany(MAX_ROWS)
            rows = [dict(zip(col_names, r)) for r in raw_rows]

        return {
            "columns": col_names,
            "rows":    rows,
            "row_count": len(rows),
            "truncated": len(rows) == MAX_ROWS,
        }
    except sqlite3.Error as e:
        raise HTTPException(400, f"SQL error: {str(e)}")

@app.post("/upload-csv", dependencies=[Depends(check_token)])
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only .csv files are supported.")

    raw = await file.read()
    if len(raw) > 50 * 1024 * 1024:  # 50 MB limit
        raise HTTPException(413, "File too large. Maximum size is 50 MB.")

    try:
        # Try BMW parser first, fall back to generic
        try:
            columns, rows = parse_bmw_csv(raw)
            col_types = ["TEXT","INTEGER","REAL","TEXT","REAL","TEXT","REAL","REAL","REAL"]
        except Exception:
            columns, rows, col_types = parse_generic_csv(raw)

        count = load_csv_into_db(columns, rows, col_types)
        schema = build_schema_info()

        return {
            "success":  True,
            "filename": file.filename,
            "rows":     count,
            "columns":  columns,
            "schema":   schema,
            "message":  f'Loaded {count:,} rows from "{file.filename}". Ready to query!',
        }
    except Exception as e:
        log.exception("Upload failed")
        raise HTTPException(500, f"Failed to process CSV: {str(e)}")
