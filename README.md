# ProcureIQ

Automated multi-vendor price discovery and RFQ submission powered by the **TinyFish Web Agent API**.

ProcureIQ sends autonomous web agents to browse supplier websites in parallel, extracts live pricing data, and lets you submit RFQ (Request for Quote) forms — all without manual browsing.

---

## Features

- **Parallel price discovery** — search up to 10 vendor websites simultaneously
- **Live results** — quotes appear in the table as each agent finishes, no waiting for all vendors
- **Smart comparison** — highlights the best price, shows currency badges for non-USD quotes (INR, EUR, etc.)
- **Async RFQ submission** — agents fill and submit contact/RFQ forms in the background; you get a toast notification when done
- **Recent searches** — history panel with one-click reload of previous results
- **Responsive UI** — works on desktop, tablet, and mobile

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.11+, FastAPI, SQLite (WAL mode) |
| Agent | TinyFish Web Agent API (SSE streaming) |
| Frontend | Vanilla HTML/CSS/JS (no framework, single file) |
| Server | Uvicorn (ASGI) |

---

## Project Structure

```
procureiq/
├── backend/
│   └── main.py          # FastAPI app — all routes, agent calls, DB logic
├── frontend/
│   └── index.html       # Single-file UI — search form, results table, RFQ modal
├── data/
│   └── procureiq.db     # SQLite database (auto-created on first run)
├── .env                 # Your API key (not committed)
├── .env.example         # Template for .env
└── run.sh               # Start script
```

---

## Setup

### 1. Install dependencies

```bash
pip install fastapi uvicorn httpx python-dotenv pydantic
```

### 2. Configure your API key

```bash
cp .env.example .env
```

Edit `.env` and set your TinyFish API key:

```
TINYFISH_API_KEY=sk-tinyfish-your-key-here
```

Get a key at [tinyfish.ai](https://tinyfish.ai).

### 3. Run

```bash
./run.sh
```

Or directly:

```bash
python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Open **http://localhost:8000** in your browser.

---

## Usage

### Price Discovery

1. Enter a **product name** (e.g. "1TB NVMe SSD")
2. Set **quantity** and optional **specs** (e.g. "PCIe 4.0, M.2 2280")
3. Add **vendor websites** using the quick-add buttons or by typing a URL
4. Click **Start Price Discovery**

Agents launch in parallel. Results appear in the comparison table as each vendor is scraped (typically 2–5 min per vendor).

> **Note:** Indian vendors (IndiaMart, Flipkart) return prices in INR. A currency badge and a mixed-currency warning appear automatically when this happens.

### Submitting an RFQ

1. Once quotes are available, click the **RFQ** button on any vendor row
2. Fill in your name, email, and company
3. Click **Submit RFQ via Web Agent**

The modal closes immediately. A toast notification in the bottom-right corner tracks progress and shows the result when the agent finishes (typically 2–5 min).

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/api/searches` | Start a new price search (returns `search_id` immediately) |
| `GET` | `/api/searches` | List recent searches (last 50) |
| `GET` | `/api/searches/{id}` | Get search status + all quotes |
| `GET` | `/api/searches/{id}/stream` | SSE stream of live agent events |
| `POST` | `/api/rfq` | Queue an RFQ submission (returns `rfq_id` immediately) |
| `GET` | `/api/rfq/{id}` | Poll RFQ job status |

### POST /api/searches

```json
{
  "product": "1TB NVMe SSD",
  "quantity": 10,
  "specs": "PCIe 4.0",
  "vendors": [
    "https://www.amazon.com",
    "https://www.newegg.com"
  ]
}
```

### POST /api/rfq

```json
{
  "quote_id": "<uuid from quotes table>",
  "contact_name": "Jane Smith",
  "contact_email": "jane@company.com",
  "company": "Acme Corp",
  "message": "Please include bulk pricing for 100+ units."
}
```

---

## Limits & Constraints

| Parameter | Limit |
|-----------|-------|
| Vendors per search | 10 max |
| Agent timeout per vendor | 200 seconds |
| Product name length | 500 chars |
| Specs length | 1000 chars |
| RFQ message length | 2000 chars |

---

## Database Schema

**searches** — one row per search job
**quotes** — one row per vendor per search (pre-inserted as placeholders, updated when agent completes)
**rfq_jobs** — one row per RFQ submission

SQLite WAL mode is enabled for safe concurrent reads/writes during parallel agent execution.

---

## Notes for Hackathon Judges

- All web browsing is performed by TinyFish autonomous agents — no scraping libraries or site-specific parsers
- The backend uses `asyncio.gather` so all vendor agents run truly in parallel
- RFQ form submission is fully async: the backend returns a job ID in milliseconds and the agent works in the background
- The frontend polls for results every 3 seconds with overlap prevention and a 10-minute hard timeout
