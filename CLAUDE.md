# API Monitoring & Anomaly Detection Platform — CLAUDE.md

Authoritative reference for this codebase. Read this before making any changes.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack & Dependencies](#3-tech-stack--dependencies)
4. [Directory Structure](#4-directory-structure)
5. [Data Models & Schemas](#5-data-models--schemas)
6. [Module Reference](#6-module-reference)
   - 6.1 Config
   - 6.2 Ingestion
   - 6.3 Classification
   - 6.4 Metrics
   - 6.5 Anomaly Detection
   - 6.6 Scoring
   - 6.7 Alerting
   - 6.8 Storage
   - 6.9 API Routes
7. [API Endpoints](#7-api-endpoints)
8. [Configuration & Environment](#8-configuration--environment)
9. [Running the Application](#9-running-the-application)
10. [Testing](#10-testing)
11. [Key Constants & Thresholds](#11-key-constants--thresholds)
12. [Patterns & Conventions](#12-patterns--conventions)
13. [Known Bugs Fixed](#13-known-bugs-fixed)

---

## 1. Project Overview

A **production-grade API Monitoring and Anomaly Detection Platform** built for banking payment APIs (UPI, IMPS, NEFT, RTGS, Payment Gateway). It ingests raw transaction logs from multiple sources, classifies errors into technical and business categories, aggregates real-time metrics, detects anomalies using five ML/statistical algorithms, scores risk using a weighted formula, and fires alerts via log + webhook notifiers.

### What it monitors

- **API health**: TPS, success rate, error rates, p95/p99 latency
- **Error classification**: HTTP-level technical errors AND HTTP 200 responses that carry a domain error code in the body ("business errors")
- **Client/merchant behaviour**: transaction volume, failure ratio, frequency, average amount, geographic entropy, burst/silence patterns
- **SLO compliance**: p95 latency > 500ms or p99 > 1000ms flags SLO breach

### Payment APIs supported

`UPI_COLLECT`, `UPI_PAY`, `IMPS_TRANSFER`, `NEFT_TRANSFER`, `RTGS_TRANSFER`, `PAYMENT_GATEWAY`

---

## 2. Architecture

### Data flow (left to right)

```
Sources                  Ingestion            Classification       Metrics
──────────               ─────────            ──────────────       ───────
Elasticsearch    ──→     ES Ingestor    ──→
Prometheus       ──→     Prom Ingestor  ──→   Technical       ──→  Aggregator ──→ MetricPoint
Kafka (mock/real)──→     Kafka Consumer ──→   Classifier      ──→  ClientProfiler → ClientSnapshot
                         normalizer.py  ──→   Business        ──→  Writer (DB + Redis)
                         LogEvent            Classifier
                                            ClassifiedEvent

Anomaly                  Scoring              Alerting             Storage
───────                  ───────              ────────             ───────
ZScore          ──→
EWMA            ──→      RiskScorer    ──→   AlertManager  ──→   AlertStore (DB)
Percentile      ──→       risk_score =        dedup + cooldown    Redis (active signals)
IsolationForest ──→       0.4*error +         LogNotifier
BurstSilence    ──→       0.3*txn +           WebhookNotifier
                          0.2*latency +
                          0.1*client

API Layer
─────────
GET /api/health/payment          → health.py
GET /api/tps                     → tps.py
GET /api/latency                 → latency.py
GET /api/error/technical-rate    → errors.py
GET /api/error/business-rate     → errors.py
GET /api/anomaly/client/{id}     → anomaly.py
GET /api/anomaly/signals         → anomaly.py
```

### Startup sequence (`main.py` lifespan)

1. `create_tables()` — auto-migrate ORM tables in development (skipped gracefully if DB unavailable)
2. `InMemoryEventBus.start()` — asyncio dispatch loop
3. `MockKafkaConsumer.start()` — synthetic 50 TPS payment event stream
4. `ClassificationPipeline` subscribed to bus
5. `MetricsAggregator`, `ClientProfiler`, `MetricsWriter` started
6. `AnomalyPipeline` wired to aggregator + profiler outputs
7. `RiskScorer → AlertManager → LogNotifier [+ WebhookNotifier]` chained
8. `APScheduler` starts ES poll (30s) and Prometheus poll (60s)

### Shutdown sequence

Graceful drain in reverse order: scheduler → kafka → bus → aggregator → profiler → writer → ES/Prom ingestors.

---

## 3. Tech Stack & Dependencies

### Backend (Python 3.11+)

| Package | Version | Purpose |
|---|---|---|
| `fastapi` | 0.115.0 | HTTP API framework |
| `uvicorn[standard]` | 0.30.6 | ASGI server |
| `pydantic` | 2.9.2 | Data validation and schemas |
| `pydantic-settings` | 2.5.2 | Settings from env vars / .env file |
| `sqlalchemy[asyncio]` | 2.0.36 | ORM + async DB access |
| `asyncpg` | 0.30.0 | Async PostgreSQL/TimescaleDB driver |
| `psycopg2-binary` | 2.9.10 | Sync driver (migrations) |
| `alembic` | 1.13.3 | Schema migrations |
| `redis[hiredis]` | 5.1.1 | Async Redis client with C parser |
| `httpx` | 0.27.2 | Async HTTP (ES + Prometheus polling, webhooks) |
| `apscheduler` | 3.10.4 | Background polling scheduler |
| `numpy` | 2.1.2 | Numerical operations |
| `pandas` | 2.2.3 | DataFrame support |
| `scikit-learn` | 1.5.2 | IsolationForest, StandardScaler |
| `scipy` | 1.14.1 | Statistical utilities |
| `pytest` | 8.3.3 | Test framework |
| `pytest-asyncio` | 0.24.0 | Async test support |

### Infrastructure (Docker)

| Service | Image | Purpose |
|---|---|---|
| TimescaleDB | `timescale/timescaledb:latest-pg16` | Time-series metric storage, hypertables |
| Redis | `redis:7-alpine` | Real-time signals, sliding windows, cooldowns |

### No frontend

This is a **backend-only platform**. The API is consumed by dashboards, monitoring tools, or other services. Interactive API docs available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## 4. Directory Structure

```
api_monitoring/
├── main.py                        # FastAPI app entry point, lifespan wiring
├── scheduler.py                   # APScheduler setup for ES + Prom polls
├── requirements.txt
├── pyproject.toml                 # pytest config, ruff config
├── docker-compose.yml             # TimescaleDB + Redis + app services
├── Dockerfile                     # Python 3.12-slim image
├── .env                           # Local dev secrets (git-ignored)
├── .env.example                   # Template for env vars
├── .gitignore
│
├── common/                        # Shared enums, schemas, constants
│   ├── enums.py                   # All Enum types (ErrorType, Severity, etc.)
│   ├── schemas.py                 # Pydantic pipeline schemas (LogEvent → ScoredAlert)
│   └── constants.py               # Tunable thresholds and weights
│
├── config/                        # Application configuration
│   ├── settings.py                # Pydantic BaseSettings, get_settings()
│   └── error_codes.py             # Bank domain error code registry + path config
│
├── ingestion/                     # Data source adapters
│   ├── base.py                    # BaseIngestor ABC, InMemoryEventBus
│   ├── normalizer.py              # ES/Prometheus/Kafka → LogEvent
│   ├── elasticsearch_ingestor.py  # Polls ES for payment logs
│   ├── prometheus_ingestor.py     # Polls Prometheus PromQL metrics
│   ├── mock_kafka_consumer.py     # Synthetic 50 TPS event generator
│   └── kafka_consumer.py          # Real aiokafka consumer skeleton
│
├── classification/                # Error classification pipeline
│   ├── base.py                    # BaseClassifier ABC
│   ├── technical_classifier.py    # HTTP 5xx, timeout, infra errors
│   ├── business_classifier.py     # HTTP 200 + domain error code detection
│   ├── pipeline.py                # Chains Technical → Business, EventBus handler
│   └── rules/
│       ├── technical_rules.py     # Status code rules, latency thresholds, regex patterns
│       └── business_rules.py      # Eligible status codes, severity map, success codes
│
├── metrics/                       # Per-window aggregation
│   ├── aggregator.py              # 1-min window accumulator → MetricPoint
│   ├── client_profiler.py         # Per-merchant accumulator → ClientSnapshot
│   ├── writer.py                  # Dual-write: Redis (immediate) + TimescaleDB (batched)
│   ├── latency.py                 # LatencyBuffer, compute_percentiles()
│   └── geo_tracker.py             # Shannon entropy, GeoProfile
│
├── anomaly/                       # Anomaly detection algorithms
│   ├── base.py                    # MetricDetector / ClientDetector ABCs, helpers
│   ├── zscore_detector.py         # Rolling Z-score (window=60 MetricPoints)
│   ├── ewma_detector.py           # EWMA mean + Welford variance
│   ├── percentile_detector.py     # Rolling p5/p95 band detector
│   ├── isolation_forest_detector.py # Per-API sklearn IsolationForest
│   ├── burst_silence_detector.py  # Client burst/silence/failure/geo/amount
│   └── pipeline.py                # AnomalyPipeline orchestrator
│
├── scoring/
│   └── risk_scorer.py             # Weighted formula → ScoredAlert + Severity
│
├── alerting/
│   ├── alert_manager.py           # Dedup, cooldown, escalation, fan-out
│   ├── alert_store.py             # Async CRUD for AlertORM + AnomalySignalORM
│   └── notifiers/
│       ├── base.py                # BaseNotifier ABC
│       ├── log_notifier.py        # Logs to Python logging (severity → log level)
│       └── webhook_notifier.py    # httpx POST with 3-retry exponential backoff
│
├── storage/
│   ├── database.py                # SQLAlchemy async engine, session factory
│   ├── models.py                  # ORM: MetricPointORM, ClientSnapshotORM, AlertORM, etc.
│   ├── redis_client.py            # Connection pool, key builders, sliding window helpers
│   └── timeseries.py              # Query helpers for routes (get_metric_series, etc.)
│
├── api/
│   ├── router.py                  # Root APIRouter, mounts all sub-routers under /api
│   ├── schemas.py                 # Route-specific response Pydantic models
│   ├── dependencies.py            # parse_time_range(), get_db(), get_redis()
│   └── routes/
│       ├── health.py              # GET /api/health/payment[/all]
│       ├── tps.py                 # GET /api/tps
│       ├── latency.py             # GET /api/latency
│       ├── errors.py              # GET /api/error/technical-rate|business-rate
│       └── anomaly.py             # GET /api/anomaly/client/{id}, /signals
│
└── tests/
    ├── test_classification/
    │   ├── test_technical_classifier.py   # 18 tests
    │   └── test_business_classifier.py    # 18 tests
    └── test_scoring/
        └── test_risk_scorer.py            # 22 tests
```

---

## 5. Data Models & Schemas

### Pipeline schemas (`common/schemas.py`)

Data flows through these schemas in order:

```
LogEvent
  └─→ ClassifiedEvent          (extends LogEvent)
        └─→ MetricPoint         (aggregated per api_name × window)
        └─→ ClientSnapshot      (aggregated per merchant_id × window)
              └─→ AnomalySignal (one per detector hit)
                    └─→ ScoredAlert (one per risk scoring cycle)
```

#### `LogEvent` — unified raw event (all sources)
| Field | Type | Description |
|---|---|---|
| `event_id` | str | UUID4, auto-generated |
| `timestamp` | datetime | Event time (UTC) |
| `source` | DataSource | ES / PROMETHEUS / KAFKA |
| `api_name` | str | e.g. `UPI_COLLECT` |
| `api_path` | str | e.g. `/payment/upi/collect` |
| `http_method` | str | Default `POST` |
| `http_status_code` | int | 100–599 (validated) |
| `response_time_ms` | float | Must be ≥ 0 |
| `merchant_id` | str? | Optional |
| `transaction_amount` | float? | Optional |
| `request_geo_country` | str? | ISO country code |
| `response_body` | dict? | For HTTP 200 business error detection |
| `error_message` | str? | Gateway/infra error text |

#### `ClassifiedEvent` — extends `LogEvent`
| Field | Type | Description |
|---|---|---|
| `error_type` | ErrorType | NONE / TECHNICAL / BUSINESS |
| `is_success` | bool | True if no error detected |
| `technical_error_category` | TechnicalErrorCategory? | e.g. TIMEOUT, DATABASE |
| `business_error_category` | BusinessErrorCategory? | e.g. INSUFFICIENT_FUNDS, AML_REJECTION |
| `domain_error_code` | str? | Raw code from response body e.g. `RR` |
| `severity` | Severity | INFO / LOW / MEDIUM / HIGH / CRITICAL |
| `classification_confidence` | float | 0.0–1.0 |

#### `MetricPoint` — 1-minute aggregate per API
| Field | Type | Description |
|---|---|---|
| `window_start` | datetime | Inclusive minute boundary |
| `api_name` | str | |
| `total_requests` | int | |
| `tps` | float | total_requests / window_seconds |
| `success_rate` | float | 0.0–1.0 |
| `technical_error_rate` | float | |
| `business_error_rate` | float | |
| `p50/p95/p99/avg_latency_ms` | float | Percentiles for this window |

#### `ClientSnapshot` — per merchant per window
| Field | Type | Description |
|---|---|---|
| `merchant_id` | str | |
| `txn_volume` | int | Transactions in window |
| `txn_frequency_per_min` | float | |
| `failure_ratio` | float | |
| `avg_txn_amount` | float | |
| `geo_entropy` | float | Shannon entropy of country distribution |
| `unique_countries` | list[str] | |
| `burst_detected` | bool | Volume > 3× EWMA baseline |
| `silence_detected` | bool | Volume < 10% EWMA baseline |

#### `AnomalySignal`
| Field | Type | Description |
|---|---|---|
| `signal_id` | str | UUID4 |
| `detected_at` | datetime | |
| `detector` | DetectorName | ZSCORE / EWMA / PERCENTILE / ISOLATION_FOREST / BURST_SILENCE |
| `anomaly_type` | AnomalyType | TPS_SPIKE, ERROR_RATE_SPIKE, LATENCY_INCREASE, etc. |
| `metric_name` | MetricName | TPS / P95_LATENCY_MS / etc. |
| `observed_value` | float | |
| `expected_value` | float | Baseline estimate |
| `deviation_score` | float | Z-score or IF score |
| `confidence` | float | 0.0–1.0 |
| `is_anomaly` | bool | Always True when signal is emitted |

#### `ScoredAlert`
| Field | Type | Description |
|---|---|---|
| `alert_id` | str | UUID4 |
| `api_name` | str | |
| `risk_score` | float | 0.0–1.0, weighted formula |
| `severity` | Severity | |
| `signals` | list[AnomalySignal] | All contributing signals |
| `title` | str | Auto-generated human-readable title |
| `description` | str | Signal breakdown |
| `status` | AlertStatus | OPEN / ACKNOWLEDGED / RESOLVED |

### ORM models (`storage/models.py`)

| Table | Primary key | TimescaleDB hypertable | Notes |
|---|---|---|---|
| `metric_points` | UUID | Yes, on `window_start` | 1-day chunks, compress after 7 days |
| `client_snapshots` | UUID | Yes, on `window_start` | Same compression policy |
| `alerts` | UUID | No | Indexed on `api_name`, `severity`, `created_at` |
| `anomaly_signals` | UUID | No | FK → `alerts.id` with cascade delete |

### Redis key schema (`storage/redis_client.py`)

| Key pattern | Type | TTL | Purpose |
|---|---|---|---|
| `apm:tps:{api}:{window_s}` | ZSET | `REDIS_METRIC_TTL` | Sliding window TPS count |
| `apm:err:tech:{api}:{window_s}` | ZSET | `REDIS_METRIC_TTL` | Technical error timestamps |
| `apm:err:biz:{api}:{window_s}` | ZSET | `REDIS_METRIC_TTL` | Business error timestamps |
| `apm:latency:{api}` | LIST | `REDIS_METRIC_TTL` | Last 1000 latency samples |
| `apm:client:{merchant}:txn_count` | STRING | 24h | Rolling txn count |
| `apm:client:{merchant}:geos` | SET | 24h | Recent country codes |
| `apm:baseline:{api}:{metric}:{h}:{d}` | STRING | 1h | Cached baseline float |
| `apm:anomaly:active:{api}` | HASH | 1h | signal_id → JSON payload |
| `apm:alert:cooldown:{api}:{type}` | STRING | 300s | Dedup cooldown per alert type |

---

## 6. Module Reference

### 6.1 Config

#### `config/settings.py`

Pydantic `BaseSettings` with sub-settings per concern. Each sub-settings class reads from its env prefix. Cached singleton via `@lru_cache`.

```python
settings = get_settings()
settings.database.async_url   # postgresql+asyncpg://...
settings.redis.url            # redis://host:port/db
settings.kafka.use_mock       # True in dev
settings.app.env              # "development" | "staging" | "production"
```

Sub-settings classes and their env prefixes:

| Class | Prefix | Key fields |
|---|---|---|
| `AppSettings` | `APP_` | `name`, `env`, `log_level`, `host`, `port` |
| `ElasticsearchSettings` | `ES_` | `host`, `username`, `password`, `api_key`, `index_pattern` |
| `PrometheusSettings` | `PROM_` | `url`, `step_seconds` |
| `KafkaSettings` | `KAFKA_` | `bootstrap_servers`, `topics`, `use_mock` |
| `DatabaseSettings` | `DB_` | `host`, `port`, `name`, `user`, `password` |
| `RedisSettings` | `REDIS_` | `host`, `port`, `db`, `password` |

#### `config/error_codes.py`

Bank domain error code registry. Three data structures:

- `ERROR_CODE_REGISTRY` — universal NPCI/UPI codes (e.g. `"RR"` → `INSUFFICIENT_FUNDS`)
- `API_ERROR_CODE_MAP` — per-API overrides that shadow the global registry
- `RESPONSE_BODY_PATHS` — ordered JSON dot-notation paths to probe per API (e.g. `"data.errorCode"`)
- `SUCCESS_INDICATOR_PATHS` — paths that indicate a true success even if an error code is present

Key functions:
- `resolve_error_code(api_name, code)` — API-specific lookup → global fallback → `UNKNOWN`
- `get_body_paths(api_name)` — returns ordered path list for this API
- `extract_nested(data, path)` — walks `"data.errorCode"` style dot paths

**Rule**: When adding a new bank API, add entries to all three dicts in `error_codes.py` and `RESPONSE_BODY_PATHS` in `config/error_codes.py`.

---

### 6.2 Ingestion

#### `ingestion/base.py`

`InMemoryEventBus` — asyncio queue-based pub/sub that acts as a Kafka drop-in:

```python
await bus.start()              # starts _dispatch_loop()
await bus.publish(log_event)   # puts event on queue
await bus.subscribe(handler)   # registers async handler
await bus.stop()               # sets stop event, waits for drain
```

`BaseIngestor` ABC — all ingestors implement `fetch()` and `normalize()`. The `run()` method chains them and publishes to the bus.

#### `ingestion/normalizer.py`

Three normalizers, each returns `Optional[LogEvent]` (returns None on unrecoverable parse error):

- `normalize_es_hit(hit)` — handles ECS field aliases, nanosecond timestamps, nested body parsing
- `normalize_prometheus_sample(labels, ts, value, metric_name)` — creates synthetic LogEvent from a PromQL sample
- `normalize_kafka_payload(payload)` — flat gateway dict → LogEvent

#### `ingestion/mock_kafka_consumer.py`

Generates realistic synthetic payment traffic at `target_tps=50.0`. Uses weighted random selection across APIs, merchants, status codes. Supports `inject_anomaly()` to spike errors/latency/burst for testing.

**API distribution**: UPI_COLLECT 40%, UPI_PAY 25%, IMPS_TRANSFER 20%, others 15%
**Merchant pool**: 100 merchants (`MERCH_0001` through `MERCH_0100`)
**Normal success rate**: ~94%

---

### 6.3 Classification

Pipeline chains: `TechnicalErrorClassifier → BusinessErrorClassifier`

**Rule**: Business classifier only runs if technical classifier passes the event through (no technical error found first).

#### `classification/technical_classifier.py` — 4-stage evaluation

1. **Exact status code rule** — `STATUS_CODE_RULE_MAP` (502→UPSTREAM_FAILURE, 504→TIMEOUT, etc.)
2. **5xx range fallback** — any 5xx not in the explicit map → `HTTP_5XX` / HIGH
3. **Latency threshold** — only for non-error codes; 30s → CRITICAL, 10s → HIGH, 5s → MEDIUM
4. **Error message regex** — pattern-matched against `error_message` field (DB, circuit breaker, OOM, connection refused)

#### `classification/business_classifier.py` — 7-stage evaluation

1. Check status code eligibility (`BUSINESS_ERROR_ELIGIBLE_STATUS_CODES`: 200, 201, 202, 400, 422)
2. Check response body exists and is a dict
3. Extract domain error code from body using `get_body_paths(api_name)` ordered list
4. Check success code whitelist (`SUCCESS_DOMAIN_CODES`: `"00"`, `"SUCCESS"`, `"APPROVED"`, etc.)
5. Check success indicator fields (`"success": true`, `"authorized": true`, etc.)
6. Resolve code → `BusinessErrorCategory` via `resolve_error_code()`
7. Assign severity from `BUSINESS_ERROR_SEVERITY` table

**Confidence**: known code = 1.0, unknown code = 0.7. Events below `MIN_BUSINESS_CLASSIFICATION_CONFIDENCE` (0.70) are passed through as NONE.

**Critical pattern**: `ClassifiedEvent` extends `LogEvent`. When the business classifier receives a `ClassifiedEvent` from the technical classifier, `event.model_dump()` already contains `error_type`. Always use `ClassifiedEvent(**{**event.model_dump(), "error_type": ..., ...})` dict-merge — never pass `error_type` as a separate kwarg alongside `**event.model_dump()` or you'll get `TypeError: multiple values for keyword argument`.

---

### 6.4 Metrics

#### `metrics/aggregator.py`

`_WindowAccumulator` — one per `(api_name, window_start)` tuple. Accumulates counts and latency samples. Flushed every `window_seconds` (default 60s) to produce a `MetricPoint`.

`MetricsAggregator` — floors incoming event timestamps to the nearest window boundary, routes to the right accumulator, runs flush loop.

#### `metrics/client_profiler.py`

`_ClientAccumulator` — one per `(merchant_id, window_start)`. Tracks txn volume, failures, amounts, geo codes, hour distribution. EWMA α=0.1 on `txn_frequency_per_min` for per-merchant baseline.

#### `metrics/writer.py`

Dual-write strategy:
- **Redis** — immediate write on each `MetricPoint` / `ClientSnapshot` (used for real-time anomaly detection)
- **TimescaleDB** — batched write every 10s or 50 records (for API time-series queries)

#### `metrics/latency.py`

`LatencyBuffer` — collects samples per `(api_name, window_start)`, computes p50/p95/p99/avg/min/max via `numpy.percentile` on flush.

#### `metrics/geo_tracker.py`

Shannon entropy formula: `H = -Σ p_i * log2(p_i)` over country code distribution. Higher entropy = more geographic spread = potential anomaly signal.

---

### 6.5 Anomaly Detection

All detectors output `list[AnomalySignal]` — empty list means no anomaly. Signals are only emitted when `is_anomaly=True`.

**Rule**: Always update rolling buffers/state AFTER scoring the current observation, not before. This prevents the current point from inflating its own baseline (self-inflation problem).

#### `anomaly/zscore_detector.py`

- Rolling `deque(maxlen=60)` per `(api_name, metric_name)`
- Scores 5 metrics: `tps`, `technical_error_rate`, `business_error_rate`, `p95_latency_ms`, `success_rate`
- Threshold: `ZSCORE_ANOMALY_THRESHOLD = 3.0` std deviations
- Flat history guard: if `std < 1e-10`, no signal emitted
- Confidence calibration: sigmoid, at threshold → 0.70, at 2× → 0.92, at 3× → 0.98

#### `anomaly/ewma_detector.py`

- Welford-style EWMA variance: `state.variance = (1-α)(state.variance + α*δ²)`
- EWMA mean: `state.mean = α*current + (1-α)*state.mean`
- α = `EWMA_ALPHA = 0.1` (smooth, slow-moving baseline)
- Minimum 5 observations before emitting signals
- Same 5 metrics as Z-score

#### `anomaly/percentile_detector.py`

- Rolling `deque(maxlen=100)` per `(api_name, metric_name)`
- Anomaly if value < p5 band or > p95 band
- Requires ≥ 20 observations before emitting

#### `anomaly/isolation_forest_detector.py`

- One `sklearn.IsolationForest` + `StandardScaler` per `api_name`
- Features: `[tps, technical_error_rate, business_error_rate, p95_latency_ms, avg_txn_amount]`
- Training buffer: last 500 MetricPoints per API
- Retrains every `ISOLATION_FOREST_RETRAIN_INTERVAL = 3600s`
- Contamination: `ISOLATION_FOREST_CONTAMINATION = 0.05` (5% anomaly assumption)
- `n_estimators = 100`
- Reports the most deviant feature for explainability

#### `anomaly/burst_silence_detector.py`

ClientDetector — operates on `ClientSnapshot`, not MetricPoint. Detects 5 signal types:

| Signal | Condition |
|---|---|
| `CLIENT_BURST` | `txn_volume > BURST_MULTIPLIER(3.0) × EWMA baseline` |
| `CLIENT_SILENCE` | `txn_volume < SILENCE_FRACTION(0.1) × EWMA baseline` |
| `CLIENT_FAILURE_SPIKE` | `failure_ratio > 0.5` (50% failure rate) |
| `AMOUNT_ANOMALY` | amount > EWMA mean + 3σ (Welford, α=0.15) |
| `GEO_DEVIATION` | `geo_entropy` deviates > 2σ from per-merchant baseline |

#### `anomaly/pipeline.py`

`AnomalyPipeline` — orchestrates all detectors:

```python
anomaly_pipeline.handle_metric(metric_point)    # runs ZScore, EWMA, Percentile, IF
anomaly_pipeline.handle_snapshot(client_snapshot)  # runs BurstSilence
anomaly_pipeline.stats()  # metrics_processed, signals_emitted, IF model status
```

---

### 6.6 Scoring

#### `scoring/risk_scorer.py`

Weighted formula:
```
risk_score = 0.4 × error_rate_component
           + 0.3 × txn_pattern_component
           + 0.2 × latency_component
           + 0.1 × client_failure_component
```

Component assignment:

| Component | AnomalyTypes mapped |
|---|---|
| `error_rate` (0.4) | `ERROR_RATE_SPIKE`, `TECHNICAL_ERROR_SPIKE`, `BUSINESS_ERROR_SPIKE` |
| `txn_pattern` (0.3) | `TPS_SPIKE`, `TPS_DROP`, `AMOUNT_ANOMALY`, `CLIENT_BURST`, `CLIENT_SILENCE`, `MULTIVARIATE` |
| `latency` (0.2) | `LATENCY_INCREASE` |
| `client_failure` (0.1) | `CLIENT_FAILURE_SPIKE`, `GEO_DEVIATION` |

Each component takes the **maximum confidence** across all signals of that type. Score is clamped to [0.0, 1.0].

Severity thresholds:
- `CRITICAL`: risk_score ≥ 0.80
- `HIGH`: risk_score ≥ 0.60
- `MEDIUM`: risk_score ≥ 0.40
- `LOW`: risk_score ≥ 0.20
- `INFO`: risk_score < 0.20

---

### 6.7 Alerting

#### `alerting/alert_manager.py`

Processing flow:
1. Noise floor check: `risk_score < 0.20` → drop silently
2. Dedup key: `{api_name}:{dominant_anomaly_type}`
3. Cooldown check via Redis (`ALERT_COOLDOWN_SECONDS = 300s`)
4. Escalation override: if active alert has lower severity → bypass cooldown and fire
5. Save to DB via `alert_store.save_alert()`
6. Fan-out to all registered `BaseNotifier` instances
7. Set Redis cooldown key with TTL

#### `alerting/notifiers/log_notifier.py`

Maps severity → Python log level:

| Severity | log level |
|---|---|
| CRITICAL | `logging.CRITICAL` |
| HIGH | `logging.ERROR` |
| MEDIUM | `logging.WARNING` |
| LOW / INFO | `logging.INFO` |

Logs structured extra dict (logstash/fluentd compatible). For CRITICAL: logs each contributing signal individually.

#### `alerting/notifiers/webhook_notifier.py`

- 3 retries with exponential backoff: 1s, 2s, 4s
- 4xx responses → no retry (client error, won't self-heal)
- Generic JSON payload compatible with Slack / PagerDuty webhook format
- `min_severity` filter: default `MEDIUM` (INFO/LOW alerts not sent to webhook)
- Optional HMAC-SHA256 signature header if `secret` is configured

#### `alerting/alert_store.py`

Full async CRUD over `AlertORM` and `AnomalySignalORM`. Key methods:

```python
await alert_store.save_alert(scored_alert)              # → alert_id str
await alert_store.get_alerts_for_api(api_name, limit, since_hours)
await alert_store.get_alerts_for_merchant(merchant_id, limit, since_hours)
await alert_store.get_signals_for_alert(alert_id)
await alert_store.count_open_alerts(api_name, since_hours)
```

---

### 6.8 Storage

#### `storage/database.py`

Async engine singleton. Session pattern:

```python
async with get_session() as session:
    session.add(record)
    # auto-commits on clean exit, rolls back on exception
```

FastAPI dependency: `db: AsyncSession = Depends(get_db)`

`create_hypertables()` — run once after initial migration to convert `metric_points` and `client_snapshots` to TimescaleDB hypertables with 1-day chunks and 7-day compression.

#### `storage/timeseries.py`

All query helpers used by API routes:

```python
get_metric_series(api_name, metric_attr, from_dt, to_dt, limit)   # [(datetime, float)]
get_windowed_averages(api_name, metric_attr, hours)                # {"mean", "min", "max", "latest"}
get_top_failing_merchants(limit, since_hours)                      # [{"merchant_id", "failure_ratio", ...}]
get_latest_client_snapshot(merchant_id)                            # Optional[ClientSnapshotORM]
get_all_api_names(since_hours)                                     # [str]
```

The `metric_attr` parameter is a string matching the ORM column name (e.g. `"tps"`, `"p95_latency_ms"`, `"technical_error_rate"`).

---

### 6.9 API Routes

All routes are under the `/api` prefix. Query params common to all time-series endpoints:

| Param | Type | Default | Constraint |
|---|---|---|---|
| `api_name` | str | required | — |
| `hours` | int | 24 | 1–168 |
| `limit` | int | 1440 | 1–5000 |

**Rule**: All route handlers use `hours: int = Query(...)` directly and call `parse_time_range(hours)` inline. Do NOT use the `TimeRange = Annotated[tuple, Depends(parse_time_range)]` pattern as a default parameter — Python raises `SyntaxError: non-default argument follows default argument` when it's mixed with other `Query()` defaults.

---

## 7. API Endpoints

### `GET /ping`
System health check. Always returns 200.
```json
{"status": "ok", "service": "API Monitoring Platform", "env": "development"}
```

### `GET /api/health/payment`
Payment API health score (0–100) with component breakdown.

**Query**: `api_name` (required), `hours` (default 24)

**Health formula**:
```
latency_slo_ratio = min(p95_latency_ms / 500.0, 3.0)
penalty = (tech_error_rate×40 + biz_error_rate×20 + latency_slo_ratio×25 + (1-success_rate)×15) × 100
health_score = max(0, 100 - penalty)
```

**Response**: `ApiHealthResponse` — health_score, status (HEALTHY/DEGRADED/CRITICAL), current TPS, error rates, latency.

### `GET /api/health/payment/all`
Sweeps all APIs active in the last hour and returns a `PlatformHealthResponse`.

### `GET /api/tps`
TPS time-series with per-point anomaly flags.

**Response**: `TpsResponse` — data points with `tps`, `is_anomaly`, plus `baseline_mean`, `baseline_max`, `current_tps`, `anomaly_count`.

Anomaly flag logic: point flagged if `has_tps_anomaly` (active Redis signal) AND value > 3× mean OR < 10% mean.

### `GET /api/latency`
p95/p99/avg latency time-series with SLO breach flags.

**Response**: `LatencyResponse` — `p95_ms`, `p99_ms`, `avg_ms`, `is_anomaly` per point, plus `baseline_p95_mean`, `slo_p95_ms=500`, `slo_p99_ms=1000`, `slo_breach_count`.

`is_anomaly = p95 > 500ms OR p99 > 1000ms`

### `GET /api/error/technical-rate`
Technical error rate time-series (HTTP 5xx, timeouts, infra failures).

**Response**: `ErrorRateResponse` — `error_rate`, `error_count`, `total_requests`, `is_anomaly` per point, plus `baseline_mean`, `top_categories`.

`is_anomaly = rate > 2 × TECHNICAL_ERROR_RATE_WARN`

### `GET /api/error/business-rate`
Business error rate time-series (HTTP 200 with domain error codes).

Same shape as technical-rate. `is_anomaly = rate > 2 × BUSINESS_ERROR_RATE_WARN`

### `GET /api/anomaly/client/{merchant_id}`
Full anomaly profile for a single merchant.

**Response**: `ClientAnomalyResponse`:
- `risk_score` — re-computed from active Redis signals
- `severity` — derived from risk score
- `active_signals` — list of `SignalSummary`
- `latest_snapshot` — `ClientSnapshotSummary` from DB
- `recent_alerts` — last 10 alerts from DB (24h window)

Returns 404 if no snapshot and no active signals found.

### `GET /api/anomaly/signals`
Recent anomaly signals across all or one API.

**Query**: `api_name` (optional filter), `hours` (default 24), `limit` (default 100, max 1000)

**Response**: `AnomalyListResponse` — list of `SignalSummary` unpacked from recent alerts.

---

## 8. Configuration & Environment

All configuration via environment variables or `.env` file. Full reference:

```bash
# Application
APP_NAME="API Monitoring Platform"
APP_ENV=development               # development | staging | production
APP_LOG_LEVEL=INFO
APP_HOST=0.0.0.0
APP_PORT=8000
APP_ENABLE_SCHEDULER=true

# Elasticsearch (polling)
ES_HOST=http://localhost:9200
ES_USERNAME=elastic
ES_PASSWORD=changeme
ES_INDEX_PATTERN=payment-logs-*
ES_POLL_LOOKBACK_SECONDS=60
ES_MAX_HITS=1000
ES_TIMEOUT_SECONDS=10

# Prometheus (polling)
PROM_URL=http://localhost:9090
PROM_STEP_SECONDS=60

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPICS=["payment.transactions","gateway.logs"]
KAFKA_USE_MOCK=true               # true = MockKafkaConsumer; false = real aiokafka

# Database (TimescaleDB)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=api_monitoring
DB_USER=postgres
DB_PASSWORD=postgres
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
DB_ECHO_SQL=false

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Optional alerting
WEBHOOK_URL=https://hooks.slack.com/...   # enables WebhookNotifier
WEBHOOK_SECRET=secret_for_hmac            # optional HMAC signing
```

**In Docker**, `docker-compose.yml` overrides `DB_HOST=db` and `REDIS_HOST=redis` to use container service names.

---

## 9. Running the Application

### Without Docker (local dev, no DB/Redis)

```bash
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

- DB unavailable: startup logs a warning and continues; DB-dependent API endpoints return 500
- Redis unavailable: first Redis operation will fail with a connection error
- ES/Prometheus unavailable: scheduler logs errors and retries — does not crash

### With Docker (full stack)

```bash
docker compose up --build
```

Services start in dependency order: `db` (healthcheck) → `redis` (healthcheck) → `app`.

### Verifying startup

```bash
curl http://localhost:8000/ping
# {"status":"ok","service":"API Monitoring Platform","env":"development"}
```

Expected startup log sequence:
```
INFO  main  Starting API Monitoring Platform [development]
WARNING main  DB unavailable at startup — skipping table creation   # only if no DB
INFO  ingestion.base  InMemoryEventBus started
INFO  ingestion.mock_kafka_consumer  MockKafkaConsumer started at 50.0 TPS
INFO  metrics.writer  MetricsWriter started
INFO  metrics.aggregator  MetricsAggregator started (window=60s)
INFO  metrics.client_profiler  ClientProfiler started (window=60s)
INFO  apscheduler.scheduler  Scheduler started
INFO  main  All services started
INFO  Application startup complete.
```

---

## 10. Testing

### Running tests

```bash
python -m pytest tests/ -v
```

### Test coverage

| Suite | File | Count | What it tests |
|---|---|---|---|
| Technical classifier | `test_technical_classifier.py` | 18 | Status code rules, latency thresholds, error message regex, classifier robustness |
| Business classifier | `test_business_classifier.py` | 18 | Known error codes, success codes, API-specific paths, nested paths, unknown codes |
| Risk scorer | `test_risk_scorer.py` | 22 | Component mapping, weighted formula, severity assignment, alert metadata |
| **Total** | | **58** | |

### Test conventions

- `_event(**kwargs)` helper creates a minimal valid `LogEvent` with sensible defaults
- `@pytest.fixture` provides a fresh classifier/scorer instance per test
- Classifier tests call `.classify()` directly (synchronous), no async needed
- To test except-handler robustness: use `unittest.mock.patch.object(classifier, "_classify", side_effect=RuntimeError(...))`
- To create intentionally invalid events (bypass Pydantic): use `LogEvent.model_construct(...)` not `LogEvent(...)`

### pytest configuration (`pyproject.toml`)

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
python_files = ["test_*.py"]
```

---

## 11. Key Constants & Thresholds

All in `common/constants.py`. Change here to tune behaviour globally.

### Anomaly detection

| Constant | Default | Description |
|---|---|---|
| `ZSCORE_ANOMALY_THRESHOLD` | 3.0 | Z-score to trigger anomaly |
| `ZSCORE_WARNING_THRESHOLD` | 2.0 | Z-score for lower-confidence warning |
| `EWMA_ALPHA` | 0.1 | EWMA smoothing factor (lower = slower baseline) |
| `EWMA_ANOMALY_THRESHOLD_MULTIPLIER` | 3.0 | Std deviations for EWMA anomaly |
| `PERCENTILE_LOWER_BAND` | 0.05 | p5 lower band |
| `PERCENTILE_UPPER_BAND` | 0.95 | p95 upper band |
| `PERCENTILE_WINDOW_SIZE` | 100 | Rolling buffer size |
| `ISOLATION_FOREST_CONTAMINATION` | 0.05 | Expected anomaly fraction |
| `ISOLATION_FOREST_N_ESTIMATORS` | 100 | Trees in forest |
| `ISOLATION_FOREST_RETRAIN_INTERVAL` | 3600 | Seconds between model retrains |
| `BURST_MULTIPLIER` | 3.0 | Volume > N× baseline = burst |
| `SILENCE_FRACTION` | 0.1 | Volume < N× baseline = silence |

### Risk scoring weights

| Constant | Default |
|---|---|
| `SCORE_WEIGHT_ERROR_RATE` | 0.4 |
| `SCORE_WEIGHT_TXN_PATTERN` | 0.3 |
| `SCORE_WEIGHT_LATENCY` | 0.2 |
| `SCORE_WEIGHT_CLIENT_SPIKE` | 0.1 |

### SLO thresholds

| Constant | Default | Description |
|---|---|---|
| `LATENCY_P95_SLO_MS` | 500 | p95 breach threshold |
| `LATENCY_P99_SLO_MS` | 1000 | p99 breach threshold |
| `TECHNICAL_ERROR_RATE_WARN` | 0.01 | 1% warn |
| `TECHNICAL_ERROR_RATE_CRITICAL` | 0.05 | 5% critical |
| `BUSINESS_ERROR_RATE_WARN` | 0.05 | 5% warn |
| `BUSINESS_ERROR_RATE_CRITICAL` | 0.15 | 15% critical |

### Alerting

| Constant | Default |
|---|---|
| `ALERT_COOLDOWN_SECONDS` | 300 |
| `ALERT_NOISE_FLOOR` | 0.20 |
| `REDIS_ANOMALY_SIGNAL_TTL` | 3600 |
| `REDIS_METRIC_TTL` | 86400 |
| `REDIS_BASELINE_CACHE_TTL` | 3600 |

### Aggregation

| Constant | Default |
|---|---|
| `PRIMARY_AGGREGATION_WINDOW` | 60 |
| `DB_WRITE_BATCH_SIZE` | 50 |
| `DB_WRITE_INTERVAL_SECONDS` | 10 |

---

## 12. Patterns & Conventions

### Singleton modules

Heavy stateful objects are module-level singletons, imported directly:

```python
from ingestion.base import bus
from classification.pipeline import pipeline
from metrics.aggregator import aggregator
from metrics.client_profiler import client_profiler
from metrics.writer import writer
from anomaly.pipeline import anomaly_pipeline
from scoring.risk_scorer import risk_scorer
from alerting.alert_manager import alert_manager
from alerting.alert_store import alert_store
```

`main.py` wires them together at startup — never instantiate these elsewhere.

### Handler chaining

All pipeline stages use an `add_handler(async_fn)` pattern:

```python
aggregator.add_handler(writer.handle_metric_point)
pipeline.add_handler(aggregator.handle)
anomaly_pipeline.add_handler(risk_scorer.handle)
risk_scorer.add_handler(alert_manager.handle)
```

This makes the pipeline composable and testable in isolation.

### Async context managers for DB sessions

```python
async with get_session() as session:
    session.add(orm_object)
    # commits automatically on exit, rolls back on exception
```

Never use `AsyncSessionFactory()` directly outside of `get_session()`.

### ClassifiedEvent construction (dict-merge pattern)

Because `ClassifiedEvent` extends `LogEvent` and `event.model_dump()` includes all fields, always use:

```python
# CORRECT
ClassifiedEvent(**{**event.model_dump(), "error_type": ErrorType.BUSINESS, ...})

# WRONG — raises TypeError: multiple values for keyword argument 'error_type'
ClassifiedEvent(**event.model_dump(), error_type=ErrorType.BUSINESS, ...)
```

### Adding a new anomaly detector

1. Create `anomaly/my_detector.py` subclassing `MetricDetector` or `ClientDetector`
2. Implement `detect(self, metric: MetricPoint) -> list[AnomalySignal]`
3. Update `anomaly/pipeline.py` `__init__` to instantiate and add to `self._metric_detectors`
4. Add new `AnomalyType` values to `common/enums.py` if needed
5. Map new types to scoring components in `scoring/risk_scorer.py`

### Adding a new API to monitor

1. Add API name to `API_ERROR_CODE_MAP` in `config/error_codes.py`
2. Add response body paths to `RESPONSE_BODY_PATHS`
3. Add domain error codes to the API-specific map
4. Optionally add success indicators to `SUCCESS_INDICATOR_PATHS`
5. The mock Kafka consumer will include it automatically if added to its `_API_NAMES` list

### Environment promotion

- `APP_ENV=development` — auto-creates DB tables, uses mock Kafka, verbose logging
- `APP_ENV=staging` — uses real Kafka, Alembic migrations, structured JSON logs
- `APP_ENV=production` — all external services required; no mock fallback

---

## 13. Known Bugs Fixed

### Bug 1 — EWMA syntax error (`anomaly/ewma_detector.py`)
Missing `*` operator in EWMA mean update. Fixed in initial implementation.
```python
# WRONG:  state.mean = alpha * current + (1 - alpha) state.mean
# FIXED:  state.mean = alpha * current + (1 - alpha) * state.mean
```

### Bug 2 — ClassifiedEvent duplicate kwargs (`classification/business_classifier.py`)
`BusinessErrorClassifier.classify()` received a `ClassifiedEvent` (not bare `LogEvent`) from the technical classifier. `event.model_dump()` already contained `error_type`, causing `TypeError: multiple values for keyword argument 'error_type'`. Fixed by using dict-merge pattern in all three call sites.

### Bug 3 — Missing `data.errorCode` path for `UPI_COLLECT` (`config/error_codes.py`)
`RESPONSE_BODY_PATHS["UPI_COLLECT"]` was missing `"data.errorCode"`. AML001 and similar codes nested under `{"data": {"errorCode": ...}}` were not detected. Fixed by adding the path.

### Bug 4 — Unknown code confidence below threshold (`classification/business_classifier.py`)
Unknown domain codes got `confidence=0.6` but `MIN_BUSINESS_CLASSIFICATION_CONFIDENCE=0.70`, causing them to silently fall through as NONE instead of being classified as `BUSINESS / UNKNOWN`. Fixed by raising unknown confidence to `0.7`.

### Bug 5 — Robustness test used invalid `LogEvent` (`tests/test_classification/test_technical_classifier.py`)
`_event(response_time_ms=-1.0)` raised Pydantic `ValidationError` before reaching the classifier. Fixed by using `unittest.mock.patch.object` to inject a `RuntimeError` directly into `_classify`, cleanly exercising the except handler.

### Bug 6 — DB crash on startup without Docker (`main.py`)
`create_tables()` raised `ConnectionRefusedError` and crashed the entire lifespan when TimescaleDB was not running locally. Fixed by wrapping in `try/except` with a `WARNING` log.

### Bug 7 — `TimeRange` Annotated Depends in route signatures (`api/routes/*.py`)
`time_range: TimeRange = TimeRange` caused FastAPI to raise `SyntaxError: non-default argument follows default argument` when mixed with `Query()` parameters. Fixed across all route files by replacing with `hours: int = Query(default=24, ...)` and calling `parse_time_range(hours)` directly in the handler body.
