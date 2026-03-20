"""
ProcureIQ — Automated Multi-Vendor Price Discovery & RFQ Agent
Powered by TinyFish Web Agent API
"""

import asyncio
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
TINYFISH_API_KEY  = os.getenv("TINYFISH_API_KEY", "")
TINYFISH_URL      = "https://agent.tinyfish.ai/v1/automation/run-sse"
AGENT_TIMEOUT_SEC = 200   # per-vendor timeout
MAX_VENDORS       = 10    # cap concurrent agents per search

app = FastAPI(title="ProcureIQ")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://0.0.0.0:8000", "http://127.0.0.1:8000"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _extract_domain(url: str) -> str:
    """Safe domain extraction from a URL."""
    try:
        host = urlparse(url).hostname or url
        return host.removeprefix("www.")
    except Exception:
        return url

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/procureiq.db")

def db() -> sqlite3.Connection:
    """Return a connection with WAL mode (allows concurrent reads + writes)."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def db_execute(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    """Execute with automatic retry on SQLite 'database is locked'."""
    for attempt in range(5):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 4:
                time.sleep(0.1 * (attempt + 1))
            else:
                raise

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            id           TEXT PRIMARY KEY,
            product      TEXT NOT NULL,
            quantity     INTEGER,
            specs        TEXT,
            created_at   TEXT NOT NULL,
            status       TEXT DEFAULT 'running',
            vendor_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id           TEXT PRIMARY KEY,
            search_id    TEXT NOT NULL,
            vendor_name  TEXT NOT NULL,
            vendor_url   TEXT NOT NULL,
            product      TEXT,
            unit_price   TEXT,
            moq          TEXT,
            lead_time    TEXT,
            availability TEXT,
            currency     TEXT,
            notes        TEXT,
            raw_json     TEXT,
            created_at   TEXT NOT NULL,
            FOREIGN KEY (search_id) REFERENCES searches(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rfq_jobs (
            id           TEXT PRIMARY KEY,
            quote_id     TEXT NOT NULL,
            vendor_url   TEXT NOT NULL,
            vendor_name  TEXT NOT NULL,
            status       TEXT DEFAULT 'running',
            submitted    INTEGER DEFAULT 0,
            confirmation TEXT,
            notes        TEXT,
            raw_json     TEXT,
            created_at   TEXT NOT NULL
        )
    """)
    # Indexes for fast lookups
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_search_id ON quotes(search_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rfq_quote_id ON rfq_jobs(quote_id)")
    # Migrations for existing DBs
    for migration in [
        "ALTER TABLE searches ADD COLUMN vendor_count INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(migration)
        except Exception:
            pass
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

init_db()

# ── Models ────────────────────────────────────────────────────────────────────
class SearchRequest(BaseModel):
    product:  str
    quantity: int = 1
    specs:    str = ""
    vendors:  list[str]

    @field_validator("product")
    @classmethod
    def product_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Product name is required")
        if len(v) > 500:
            raise ValueError("Product name too long (max 500 chars)")
        return v

    @field_validator("quantity")
    @classmethod
    def quantity_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Quantity must be at least 1")
        if v > 1_000_000:
            raise ValueError("Quantity too large")
        return v

    @field_validator("specs")
    @classmethod
    def specs_max_length(cls, v: str) -> str:
        if len(v) > 1000:
            raise ValueError("Specs too long (max 1000 chars)")
        return v

    @field_validator("vendors")
    @classmethod
    def validate_vendors(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one vendor URL is required")
        if len(v) > MAX_VENDORS:
            raise ValueError(f"Maximum {MAX_VENDORS} vendors per search")
        validated = []
        seen = set()
        for url in v:
            url = url.strip()
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL (must start with http/https): {url}")
            if url not in seen:
                seen.add(url)
                validated.append(url)
        return validated


class RFQRequest(BaseModel):
    quote_id:      str
    contact_name:  str
    contact_email: str
    company:       str
    message:       str = ""

    @field_validator("contact_name", "company")
    @classmethod
    def not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("This field is required")
        if len(v) > 200:
            raise ValueError("Too long (max 200 chars)")
        return v

    @field_validator("contact_email")
    @classmethod
    def valid_email(cls, v: str) -> str:
        v = v.strip()
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("Invalid email address")
        return v

    @field_validator("message")
    @classmethod
    def message_max_length(cls, v: str) -> str:
        if len(v) > 2000:
            raise ValueError("Message too long (max 2000 chars)")
        return v


# ── TinyFish Agent ────────────────────────────────────────────────────────────
def build_price_goal(product: str, quantity: int, specs: str) -> str:
    spec_line = f" with specifications: {specs}" if specs else ""
    return f"""
Search for "{product}"{spec_line} on this website.
Find the product and extract the following details:
- Product name (exact)
- Unit price (number + currency)
- Minimum order quantity (MOQ)
- Lead time / delivery estimate
- Stock availability (in stock / out of stock / quantity)
- Any bulk discount tiers if shown

We need {quantity} units.

Return ONLY valid JSON in this exact format:
{{
  "vendor_name": "string",
  "product": "string",
  "unit_price": "string",
  "currency": "string",
  "moq": "string",
  "lead_time": "string",
  "availability": "string",
  "bulk_discounts": "string or null",
  "notes": "string or null"
}}

If the product is not found or the site requires login to see pricing, return:
{{"error": "not found", "reason": "brief explanation"}}
""".strip()


def build_rfq_goal(product: str, contact_name: str, contact_email: str,
                   company: str, quantity: int, message: str) -> str:
    return f"""
Fill out and submit the RFQ (Request for Quote) or contact/inquiry form on this page.
Use these details:
- Name: {contact_name}
- Email: {contact_email}
- Company: {company}
- Product: {product}
- Quantity: {quantity}
- Message: "We are interested in purchasing {quantity} units of {product}. {message} Please provide your best quote including lead time and bulk pricing."

Steps:
1. Find the RFQ, Get a Quote, or Contact Us form
2. Fill in all required fields
3. Submit the form
4. Confirm submission was successful

Return JSON:
{{"submitted": true/false, "confirmation": "any confirmation message or reference number shown", "notes": "any issues encountered"}}
""".strip()


async def run_agent(vendor_url: str, goal: str) -> AsyncGenerator[dict, None]:
    """Stream events from TinyFish agent for one vendor."""
    headers = {
        "X-API-Key": TINYFISH_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "url": vendor_url,
        "goal": goal,
        "browser_profile": "stealth",
    }

    async with httpx.AsyncClient(timeout=AGENT_TIMEOUT_SEC) as client:
        async with client.stream("POST", TINYFISH_URL, headers=headers, json=payload) as resp:
            if resp.status_code != 200:
                body = await resp.aread()
                yield {"type": "ERROR", "message": f"HTTP {resp.status_code}: {body.decode()}"}
                return

            async for line in resp.aiter_lines():
                if not line:
                    continue
                if line.startswith("data: "):
                    raw = line[6:]
                    try:
                        event = json.loads(raw)
                        yield event
                        if event.get("type") == "COMPLETE":
                            return
                    except json.JSONDecodeError:
                        continue


async def extract_result(vendor_url: str, goal: str) -> dict:
    """Run agent and return only the final resultJson."""
    async for event in run_agent(vendor_url, goal):
        if event.get("type") == "COMPLETE" and event.get("status") == "COMPLETED":
            result = event.get("resultJson") or event.get("result") or {}
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except Exception:
                    result = {"raw": result}
            return result
        if event.get("type") in ("COMPLETE", "ERROR"):
            return {"error": event.get("message", "Agent failed"), "status": event.get("status")}
    return {"error": "No response from agent"}


# ── API Routes ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "ProcureIQ"}


@app.post("/api/searches")
async def create_search(req: SearchRequest):
    """Start a new multi-vendor price search. Returns search_id immediately."""
    if not TINYFISH_API_KEY:
        raise HTTPException(400, "TINYFISH_API_KEY not configured")

    search_id = str(uuid.uuid4())
    conn = db()
    try:
        db_execute(conn,
            "INSERT INTO searches VALUES (?,?,?,?,?,?,?)",
            (search_id, req.product, req.quantity, req.specs,
             now_iso(), "running", len(req.vendors))
        )
        for vendor_url in req.vendors:
            db_execute(conn,
                """INSERT INTO quotes
                   (id, search_id, vendor_name, vendor_url, product,
                    unit_price, moq, lead_time, availability, currency, notes, raw_json, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (str(uuid.uuid4()), search_id, _extract_domain(vendor_url), vendor_url,
                 req.product, None, None, None, None, None,
                 "searching...", None, now_iso())
            )
        conn.commit()
    finally:
        conn.close()

    # Store task reference to prevent garbage collection
    task = asyncio.create_task(run_vendor_searches(search_id, req))
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    return {"search_id": search_id, "vendor_count": len(req.vendors), "status": "running"}


async def run_vendor_searches(search_id: str, req: SearchRequest):
    """Run all vendor searches concurrently and save results."""
    goal = build_price_goal(req.product, req.quantity, req.specs)

    async def search_one(vendor_url: str):
        try:
            result = await asyncio.wait_for(
                extract_result(vendor_url, goal),
                timeout=AGENT_TIMEOUT_SEC
            )
        except asyncio.TimeoutError:
            result = {"error": f"Agent timed out after {AGENT_TIMEOUT_SEC}s"}
        except Exception as e:
            result = {"error": f"Unexpected error: {str(e)}"}

        vendor_name = result.get("vendor_name") or _extract_domain(vendor_url)
        conn = db()
        try:
            db_execute(conn,
                """UPDATE quotes SET
                   vendor_name=?, product=?, unit_price=?, moq=?, lead_time=?,
                   availability=?, currency=?, notes=?, raw_json=?
                   WHERE search_id=? AND vendor_url=?""",
                (
                    vendor_name,
                    result.get("product", req.product),
                    result.get("unit_price"),
                    result.get("moq"),
                    result.get("lead_time"),
                    result.get("availability"),
                    result.get("currency"),
                    result.get("notes") or result.get("error"),
                    json.dumps(result),
                    search_id, vendor_url,
                )
            )
            conn.commit()
        finally:
            conn.close()

    try:
        await asyncio.gather(*[search_one(v) for v in req.vendors])
    except Exception:
        pass  # each search_one handles its own errors
    finally:
        # Always mark complete so frontend stops polling
        conn = db()
        try:
            db_execute(conn, "UPDATE searches SET status='complete' WHERE id=?", (search_id,))
            conn.commit()
        finally:
            conn.close()


@app.get("/api/searches/{search_id}")
def get_search(search_id: str):
    """Get search status and all quotes found so far."""
    conn = db()
    try:
        row = conn.execute(
            "SELECT * FROM searches WHERE id=?", (search_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "Search not found")

        quotes = conn.execute(
            "SELECT * FROM quotes WHERE search_id=? ORDER BY created_at", (search_id,)
        ).fetchall()
    finally:
        conn.close()

    quote_cols = ["id", "search_id", "vendor_name", "vendor_url", "product",
                  "unit_price", "moq", "lead_time", "availability", "currency",
                  "notes", "raw_json", "created_at"]
    quotes_list = [dict(zip(quote_cols, q)) for q in quotes]

    return {
        "search_id":   row[0],
        "product":     row[1],
        "quantity":    row[2],
        "specs":       row[3],
        "created_at":  row[4],
        "status":      row[5],
        "vendor_count": row[6],
        "quotes":      quotes_list,
        "quote_count": len(quotes_list),
    }


@app.get("/api/searches/{search_id}/stream")
async def stream_search_progress(search_id: str):
    """SSE endpoint — streams live agent progress for all vendors in a search."""
    conn = db()
    row = conn.execute("SELECT * FROM searches WHERE id=?", (search_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Search not found")

    async def event_gen():
        product  = row[1]
        quantity = row[2]
        specs    = row[3]

        conn2 = db()
        vendor_rows = conn2.execute(
            "SELECT DISTINCT vendor_url FROM quotes WHERE search_id=?", (search_id,)
        ).fetchall()
        conn2.close()

        if not vendor_rows:
            yield f"data: {json.dumps({'type':'ERROR','message':'No vendors found'})}\n\n"
            return

        goal = build_price_goal(product, quantity, specs)

        async def stream_one(vendor_url):
            async for event in run_agent(vendor_url, goal):
                event["vendor_url"] = vendor_url
                yield f"data: {json.dumps(event)}\n\n"

        queue: asyncio.Queue = asyncio.Queue()

        async def enqueue(gen):
            try:
                async for item in gen:
                    await queue.put(item)
            except Exception as e:
                await queue.put(f"data: {json.dumps({'type':'ERROR','message':str(e)})}\n\n")
            finally:
                await queue.put(None)

        producers = [asyncio.create_task(enqueue(stream_one(v[0]))) for v in vendor_rows]
        done = 0
        while done < len(producers):
            item = await queue.get()
            if item is None:
                done += 1
            else:
                yield item

        yield f"data: {json.dumps({'type':'SEARCH_COMPLETE','search_id':search_id})}\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.get("/api/searches")
def list_searches():
    """List all searches with quote counts."""
    conn = db()
    try:
        rows = conn.execute("""
            SELECT s.id, s.product, s.quantity, s.created_at, s.status,
                   COUNT(q.id) as quote_count
            FROM searches s
            LEFT JOIN quotes q ON q.search_id = s.id
            GROUP BY s.id
            ORDER BY s.created_at DESC
            LIMIT 50
        """).fetchall()
    finally:
        conn.close()
    cols = ["search_id", "product", "quantity", "created_at", "status", "quote_count"]
    return [dict(zip(cols, r)) for r in rows]


@app.post("/api/rfq")
async def submit_rfq(req: RFQRequest):
    """Start an async RFQ job — returns rfq_id immediately, agent runs in background."""
    conn = db()
    try:
        quote = conn.execute(
            "SELECT * FROM quotes WHERE id=?", (req.quote_id,)
        ).fetchone()
        search = conn.execute(
            "SELECT * FROM searches WHERE id=?", (quote[1],)
        ).fetchone() if quote else None
    finally:
        conn.close()

    if not quote or not search:
        raise HTTPException(404, "Quote not found")

    vendor_url  = quote[3]
    vendor_name = quote[2]
    product     = search[1]
    quantity    = search[2]

    rfq_id = str(uuid.uuid4())
    conn = db()
    try:
        db_execute(conn,
            "INSERT INTO rfq_jobs VALUES (?,?,?,?,?,?,?,?,?,?)",
            (rfq_id, req.quote_id, vendor_url, vendor_name,
             "running", 0, None, None, None, now_iso())
        )
        conn.commit()
    finally:
        conn.close()

    goal = build_rfq_goal(
        product, req.contact_name, req.contact_email,
        req.company, quantity, req.message
    )
    task = asyncio.create_task(_run_rfq_job(rfq_id, vendor_url, goal))
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    return {"rfq_id": rfq_id, "status": "running", "vendor_url": vendor_url}


async def _run_rfq_job(rfq_id: str, vendor_url: str, goal: str):
    try:
        result = await asyncio.wait_for(
            extract_result(vendor_url, goal),
            timeout=AGENT_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        result = {"submitted": False, "error": f"Agent timed out after {AGENT_TIMEOUT_SEC}s"}
    except Exception as e:
        result = {"submitted": False, "error": str(e)}

    conn = db()
    try:
        db_execute(conn,
            """UPDATE rfq_jobs SET status='complete', submitted=?, confirmation=?, notes=?, raw_json=?
               WHERE id=?""",
            (
                1 if result.get("submitted") else 0,
                result.get("confirmation"),
                result.get("notes") or result.get("error"),
                json.dumps(result),
                rfq_id,
            )
        )
        conn.commit()
    finally:
        conn.close()


@app.get("/api/rfq/{rfq_id}")
def get_rfq_status(rfq_id: str):
    """Poll RFQ job status."""
    conn = db()
    try:
        row = conn.execute("SELECT * FROM rfq_jobs WHERE id=?", (rfq_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, "RFQ job not found")
    cols = ["id", "quote_id", "vendor_url", "vendor_name", "status",
            "submitted", "confirmation", "notes", "raw_json", "created_at"]
    d = dict(zip(cols, row))
    return {
        "rfq_id":       d["id"],
        "status":       d["status"],
        "vendor_name":  d["vendor_name"],
        "submitted":    bool(d["submitted"]),
        "confirmation": d["confirmation"],
        "notes":        d["notes"],
    }


# ── Serve Frontend ────────────────────────────────────────────────────────────
frontend_dir = os.path.join(os.path.dirname(__file__), "../frontend")
if os.path.isdir(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
