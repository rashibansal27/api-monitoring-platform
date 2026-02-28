# API Monitoring & Anomaly Detection Platform

A production-grade monitoring and anomaly detection platform for banking payment APIs. Ingests transaction logs from Elasticsearch, Prometheus, and Kafka; classifies errors; aggregates real-time metrics; detects anomalies using five ML/statistical algorithms; and fires risk-scored alerts.

---

## Features

- **Multi-source ingestion** — Elasticsearch, Prometheus PromQL, and Kafka (mock or real)
- **Two-stage error classification**
  - *Technical errors* — HTTP 5xx, timeouts, circuit breakers, database failures
  - *Business errors* — HTTP 200 responses carrying domain error codes (insufficient funds, OTP failure, AML rejection, etc.)
- **Real-time metrics** — TPS, success rate, p50/p95/p99 latency, technical & business error rates aggregated per minute
- **Client profiling** — per-merchant transaction volume, failure ratio, average amount, geographic entropy, burst/silence detection
- **Five anomaly detectors** running in parallel:
  | Detector | Algorithm |
  |---|---|
  | Z-Score | Rolling window (60 points), 3σ threshold |
  | EWMA | Exponentially weighted mean + Welford variance |
  | Percentile | Rolling p5/p95 band (100 points) |
  | Isolation Forest | Per-API sklearn model, retrains hourly |
  | Burst/Silence | Client-level volume and geo entropy |
- **Risk scoring** — weighted formula: `0.4×error + 0.3×txn_pattern + 0.2×latency + 0.1×client`
- **Alerting** — deduplication, 5-minute cooldown, severity escalation, log + webhook notifiers
- **REST API** — 9 endpoints for health, TPS, latency, error rates, and anomaly profiles

---

## Tech Stack

| Layer | Technology |
|---|---|
| API framework | FastAPI + Uvicorn |
| Database | TimescaleDB (PostgreSQL hypertables) |
| Cache / signals | Redis |
| ML / statistics | scikit-learn, numpy, scipy |
| Async HTTP | httpx |
| Scheduler | APScheduler |
| Validation | Pydantic v2 |
| ORM | SQLAlchemy (async) |
| Migrations | Alembic |

---

## Architecture

```
Elasticsearch ──┐
Prometheus    ──┼──► Normalise ──► Classify ──► Aggregate ──► Anomaly ──► Score ──► Alert
Kafka (mock)  ──┘    LogEvent     Technical     MetricPoint   ZScore      Risk     Manager
                                  Business      ClientSnap    EWMA        Score
                                                              Percentile
                                                              IsoForest
                                                              BurstSilence
                                                                    │
                                                               TimescaleDB
                                                               Redis
                                                               REST API
```

---

## Quickstart

### Option 1 — Python only (no database)

```bash
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The mock Kafka consumer starts immediately at 50 TPS. The classification and anomaly pipeline run fully in memory. DB-dependent API endpoints return errors until a database is connected.

### Option 2 — Full stack with Docker

```bash
docker compose up --build
```

Starts TimescaleDB, Redis, and the application. All endpoints work after ~60 seconds (one aggregation window).

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/ping` | Service health check |
| `GET` | `/api/health/payment` | Health score (0–100) for one API |
| `GET` | `/api/health/payment/all` | Health scores for all active APIs |
| `GET` | `/api/tps` | TPS time-series with anomaly flags |
| `GET` | `/api/latency` | p95/p99/avg latency with SLO breach flags |
| `GET` | `/api/error/technical-rate` | Technical error rate time-series |
| `GET` | `/api/error/business-rate` | Business error rate time-series |
| `GET` | `/api/anomaly/client/{merchant_id}` | Full anomaly profile for a merchant |
| `GET` | `/api/anomaly/signals` | Recent anomaly signals |

Interactive docs: `http://localhost:8000/docs`

**Common query parameters**: `api_name` (required), `hours` (1–168, default 24), `limit` (default 1440)

---

## Configuration

Copy `.env.example` to `.env` and edit as needed:

```bash
cp .env.example .env
```

Key settings:

```bash
KAFKA_USE_MOCK=true          # true = synthetic traffic; false = real Kafka
APP_ENV=development          # auto-creates DB tables in development
DB_HOST=localhost            # set to 'db' when using Docker
REDIS_HOST=localhost         # set to 'redis' when using Docker
WEBHOOK_URL=https://...      # optional — enables webhook alerts
```

---

## Project Structure

```
├── main.py                  # FastAPI app, lifespan startup/shutdown
├── scheduler.py             # Background polling jobs
├── common/                  # Shared enums, schemas, constants
├── config/                  # Settings + bank error code registry
├── ingestion/               # ES, Prometheus, Kafka adapters + normaliser
├── classification/          # Technical & business error classifiers
├── metrics/                 # Window aggregation, latency, geo tracking
├── anomaly/                 # Five anomaly detectors + pipeline
├── scoring/                 # Weighted risk scorer
├── alerting/                # Alert manager, store, log + webhook notifiers
├── storage/                 # TimescaleDB ORM, Redis client, query helpers
├── api/                     # FastAPI routes and response schemas
└── tests/                   # 58 unit tests
```

---

## Tests

```bash
python -m pytest tests/ -v
```

58 tests across classification (technical + business) and risk scoring.

---

## Supported Payment APIs

`UPI_COLLECT` · `UPI_PAY` · `IMPS_TRANSFER` · `NEFT_TRANSFER` · `RTGS_TRANSFER` · `PAYMENT_GATEWAY`

---

## SLO Thresholds

| Metric | Warn | Critical |
|---|---|---|
| Technical error rate | 1% | 5% |
| Business error rate | 5% | 15% |
| p95 latency | — | 500ms |
| p99 latency | — | 1000ms |
