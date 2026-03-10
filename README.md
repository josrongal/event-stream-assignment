# Event Stream Data Pipeline

Data ingestion and transformation pipeline for event streaming data with RESTful API.

## 🏗️ Architecture

```
src/
├── cleaner.py          # Data cleaning
├── aggregator.py       # Metrics aggregation
├── pipeline.py         # Main orchestrator
├── api/
│   ├── endpoints/      # Organized endpoint modules
│   │   ├── health.py   # Health check endpoints
│   │   ├── metrics.py  # Metrics query endpoints
│   │   └── events.py   # Event query endpoints
│   ├── main.py         # FastAPI application
│   ├── models.py       # Pydantic models
│   └── dependencies.py # Dependency injection
└── utils/
    └── logger.py       # Logging utilities

tests/                  # Pytest test suite
data/
├── bronze/             # Raw input data (JSONL)
├── silver/             # Cleaned events (parquet) + rejected events (JSONL)
└── gold/               # Aggregated metrics (parquet)
```

---

## Quick Start

### Option 1: Local (without Docker)

#### 1. Install dependencies

```bash
# Using the setup script (recommended)
chmod +x setup_local.sh run_api.sh run_tests.sh
./setup_local.sh

# Or manually
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### 2. Run the data pipeline

```bash
python src/pipeline.py
```

This will:
- Clean and validate events from `data/bronze/`
- Generate aggregated metrics
- Save outputs to `data/silver/` and `data/gold/`

#### 3. Start the API

```bash
# Using the run script
./run_api.sh

# Or manually
uvicorn src.api.main:app --reload
```

API will be available at: http://localhost:8000

---

### Option 2: Docker

#### 1. Build and start

```bash
# Build
docker build -t event-pipeline .

# Start (runs pipeline + API automatically)
docker run --rm -p 8000:8000 --name event-pipeline event-pipeline
```

#### 2. Stop

```bash
docker stop event-pipeline
```

---

### Access Points

- **API Base**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

---

## Part 1: Data Cleaning & Metrics Aggregation

### Cleaning Pipeline

The pipeline reads raw JSONL events from the bronze layer and applies a multi-step cleaning strategy before writing to silver.

#### Step 1 — Drop events with missing critical fields

Events are dropped entirely if any of the following fields are null:

| Field | Reason |
| :--- | :--- |
| `event_id` | Primary identifier — without it the event cannot be tracked or deduplicated |
| `timestamp` | Required for time-windowed aggregations and ordering |
| `service` | Required for per-service metrics |
| `event_type` | Required for event classification |

Dropped events are written to the DLQ with `rejection_reason = missing_critical_fields`.

#### Step 2 — Reject events with invalid timestamps

After the null check, timestamps are parsed with `pd.to_datetime(..., utc=True, errors='coerce')`. Any value that cannot be parsed (e.g. `"2025-13-40"`, `"not-a-date"`) produces a `NaT`. These events are sent to the DLQ with `rejection_reason = invalid_timestamp`, preserving the **original raw string** so the rejection reason is auditable.

#### Step 3 — Deduplication

Events are deduplicated on the composite key `(event_id, service, event_type)`. Deduplicating on `event_id` alone would incorrectly discard legitimate lifecycle stages — for example, a payment that emits both a `request_started` and a `request_completed` event shares the same `event_id` but represents two distinct steps, and both must be preserved.

For each group that shares the same `(event_id, service, event_type)`, events are sorted by `timestamp DESC` and only the **most recent** copy is kept in silver. All older copies are written to the DLQ with `rejection_reason = duplicate_event_id`.

#### Step 4 — Schema contract (`TARGET_COLUMNS`)

A fixed silver schema is enforced:

```python
TARGET_COLUMNS = [
    "event_id", "timestamp", "service", "event_type",
    "user_id", "latency_ms", "status_code"
]
```

- **Unknown columns** (e.g. `extra_field`) are **dropped** — the event itself is kept if critical fields are valid
- **Missing optional columns** are added as `NULL`
- **Types** are cast: string columns to `pd.StringDtype`, `latency_ms` to `float64`, `status_code` to nullable `Int64`, `timestamp` to `datetime64[UTC]`

#### Step 5 — Imputation and normalization

| Field | Rule |
| :--- | :--- |
| `user_id` | Null → `'unknown_user'` |
| `latency_ms` | Negative → `NULL` |
| `status_code` | Outside `[100, 599]` → `NULL` |
| `service`, `event_type`, `user_id`, `event_id` | Lowercased and stripped |

#### Silver output files

| File | Content |
| :--- | :--- |
| `data/silver/slv_streaming_events.parquet` | All valid, cleaned events |
| `data/silver/rejected_events.jsonl` | All rejected events with `rejection_reason` column |

The `rejected_events.jsonl` DLQ is a JSONL file (one JSON object per line) that preserves the **original field values** before any transformation — notably the raw invalid timestamp string — making it suitable for audit and reprocessing.

---

### Metrics Aggregation

After cleaning, events are aggregated per service per 1-minute window:

| Metric | Description |
| :--- | :--- |
| `request_count` | Total events in the window |
| `avg_latency_ms` | Mean of `latency_ms` for the window |
| `error_count` | Events where `status_code` is outside `[200, 299]` |
| `error_rate` | `error_count / request_count` |

Output: `data/gold/gld_aggregated_metrics.parquet`

---

## Part 2: Storage & Modeling Design

### 1. Data Placement Strategy (Medallion Architecture)
I propose a **Medallion Architecture** to balance cost-efficiency, data traceability, and high-performance analytics.

| Layer | Technology | Storage Pattern / Partitioning | Rationale |
| :--- | :--- | :--- | :--- |
| **Raw (Bronze)** | **Cloud Storage (GCS)** | `gs://bucket/landing/year=YYYY/month=MM/day=DD/hour=HH/` | **Hive-style partitioning** ensures process isolation and "Partition Pruning." Inmutable storage of JSONL files allows for full re-processing. |
| **Cleaned (Silver)** | **BigQuery (BigLake)** | Partitioned by `timestamp` (Day) & Clustered by `service` | Typed and validated events. Using **BigLake** allows querying GCS files with BigQuery's engine without moving the data and using Iceberg / Delta Lake format. |
| **Aggregated (Gold)** | **BigQuery / Bigtable** | Partitioned by derived `partition_date = DATE(time_window)` (Day) & Clustered by `service` / Key-Value pairs | A `partition_date` column is derived at write time (`DATE(time_window)`), the same pattern Silver uses with `DATE(timestamp)`. Bigtable row key `service#partition_date#timestamp_minute` preserves efficient prefix scans. |

---

### 2. Analytical vs. Operational Storage
The choice of technology depends on the end-consumer requirements:

* **BigQuery (Analytical Hub):** * **Use Case:** Long-term trend analysis and complex SQL joins (e.g., "Monthly average latency per service").
    * **Optimization:** Time-unit partitioning on the `timestamp` column reduces scan costs, while clustering by `service` speeds up filtered queries.
* **Bigtable (Operational Serving):** * **Use Case:** Real-time monitoring dashboards or automated scaling triggers.
    * **Row Key Design:** A lexicographical key like `service#timestamp_minute` allows for extremely fast prefix scans to retrieve the most recent state of a specific service.

---

### 3. Schema Evolution & Table Formats
Handling changes in event structure (e.g., adding new fields) is critical for pipeline stability.

* **Schema Evolution:** * I would implement **Schema-on-Read** in the Bronze layer.
    * In **BigQuery**, I would enable `schema_update_options` to allow automatic addition of new `NULLABLE` columns, ensuring backward compatibility with existing downstream consumers.
* **Modern Table Formats (Apache Iceberg / Delta Lake):**
    * To achieve **Time Travel** (querying data as it was at a specific point in time) and **ACID transactions** over GCS, I would use **Apache Iceberg**.
    * BigQuery's **BigLake** integration allows us to treat these GCS-resident Iceberg tables as native tables, supporting full schema evolution (including column renames/deletions) without rewriting the underlying files which is not possible with BigQuery.

---

### 4. GCS Partitioning for Scalability
Instead of a flat file structure, the Bronze layer is partitioned by arrival time (`year/month/day/hour`).

**Why?**
1. **Efficiency:** Tools like BigQuery or Spark only scan the folders relevant to the query's time range, significantly reducing I/O and cost.
2. **Scalability:** Prevents the "Small File Problem" and avoids GCS performance degradation caused by having millions of files in a single prefix.
3. **Reliability:** Facilitates "backfilling" or re-running specific time intervals if a failure is detected in a specific window.

### 5. Incremental Load Strategy

Instead of a full-table reprocessing on every pipeline run, each layer is updated only for the partitions that contain new data.

**Core mechanic — `load_timestamp` watermark:**

Each Silver record is stamped with a `load_timestamp` at the moment the pipeline writes it. This acts as the watermark for the next run.

**Step-by-step flow:**

| Step | Action |
| :--- | :--- |
| **1. Detect new data** | Query `WHERE load_timestamp > last_successful_run_timestamp` on the Silver table (or read Bronze files newer than the watermark). |
| **2. Derive affected partitions** | Extract the distinct `partition_date` values from those records: `SELECT DISTINCT DATE(timestamp) AS partition_date FROM new_records`. |
| **3. Silver upsert** | `MERGE INTO silver USING new_records ON (event_id = event_id AND service = service AND event_type = event_type)` — if matched and incoming `timestamp` is newer, `UPDATE`; if not matched, `INSERT`. This mirrors the composite dedup key and preserves lifecycle stages as separate rows. |
| **4. Gold overwrite** | Re-aggregate only those `partition_date` values: `MERGE INTO gold … WHERE gold.partition_date IN (affected_dates)`. Because Gold derives its own `partition_date = DATE(time_window)`, the affected partitions map 1-to-1 with Silver. |

**Benefits:** No full Bronze/Silver scans after the first load; idempotent re-runs (safe to re-overwrite a partition on failure); partition pruning in both BigQuery layers reduces cost proportionally to the ingestion window.

---

## Part 3: RESTful API

### Design

The API is built with **FastAPI** and loads pipeline output files into memory at startup using the `lifespan` context manager. All data is served from in-process pandas DataFrames injected via `dependencies.py`.

- **Startup**: reads `slv_streaming_events.parquet`, `rejected_events.jsonl`, and `gld_aggregated_metrics.parquet` into memory. Missing critical files cause a fast-fail `RuntimeError` so the container never starts in a broken state.
- **Dependency injection**: `get_events_df()`, `get_metrics_df()`, `get_rejected_events_df()` are module-level getters backed by global stores, overridable in tests via `set_*` setters.
- **Validation**: request/response models use **Pydantic v2** (`model_config = ConfigDict(...)`).

---

### Endpoints

#### Health & Status

##### `GET /` — API Info
```bash
curl http://localhost:8000/
```

##### `GET /health` — Health Check
```bash
curl http://localhost:8000/health | jq
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-12T10:30:00Z",
  "total_events": 1996,
  "total_metrics": 285
}
```

---

#### Metrics

##### `GET /metrics` — Get Aggregated Metrics

```bash
# All metrics
curl http://localhost:8000/metrics | jq

# Filter by service
curl "http://localhost:8000/metrics?service=checkout" | jq

# Filter by time range
curl "http://localhost:8000/metrics?service=checkout&from=2025-01-12T09:00:00Z&to=2025-01-12T12:00:00Z" | jq
```

Query parameters:
- `service` (optional): Filter by service name
- `from` (optional): Start timestamp (ISO 8601)
- `to` (optional): End timestamp (ISO 8601)

Response:
```json
[
  {
    "service": "checkout",
    "time_window": "2025-01-12T09:00:00Z",
    "request_count": 45,
    "avg_latency_ms": 156.23,
    "error_count": 2,
    "error_rate": 0.0444
  }
]
```

##### `GET /metrics/summary` — Metrics Summary

```bash
curl "http://localhost:8000/metrics/summary?service=checkout" | jq
```

Response:
```json
{
  "total_time_windows": 12,
  "total_requests": 540,
  "avg_latency_ms": 162.4,
  "avg_error_rate": 0.037,
  "services": ["checkout"],
  "time_range": {
    "start": "2025-01-12T09:00:00+00:00",
    "end": "2025-01-12T09:11:00+00:00"
  }
}
```

---

#### Events

##### `GET /events` — Get Cleaned Events

```bash
# All events (default limit 100)
curl "http://localhost:8000/events?limit=50"

# Filter by service and event_type
curl "http://localhost:8000/events?service=payments&event_type=request_completed&limit=50"
```

Query parameters:
- `service` (optional): Filter by service
- `event_type` (optional): Filter by event type
- `limit` (optional): Max events (default: 100, max: 1000)

##### `GET /events/count` — Count Cleaned Events

```bash
curl "http://localhost:8000/events/count?service=auth"
```

Response:
```json
{
  "count": 312,
  "filters": { "service": "auth", "event_type": null }
}
```

##### `GET /events/rejected` — Get Rejected Events

Returns events that were excluded from silver during the cleaning pipeline, including their original field values and the reason for rejection.

```bash
# All rejected events
curl "http://localhost:8000/events/rejected?limit=50" | jq

# Filter by rejection reason
curl "http://localhost:8000/events/rejected?rejection_reason=invalid_timestamp" | jq

# Filter by service
curl "http://localhost:8000/events/rejected?service=payments" | jq
```

Query parameters:
- `service` (optional): Filter by service
- `event_type` (optional): Filter by event type
- `rejection_reason` (optional): `missing_critical_fields` | `invalid_timestamp` | `duplicate_event_id`
- `limit` (optional): Max events (default: 100, max: 1000)

Response:
```json
[
  {
    "event_id": "e4",
    "timestamp": "2025-13-40T25:61:61Z",
    "service": "auth",
    "event_type": "request_completed",
    "user_id": "u789",
    "latency_ms": 80,
    "status_code": 200,
    "rejection_reason": "invalid_timestamp"
  }
]
```

> Note: `timestamp` is preserved as the **original raw string** for rejected events, not coerced to a datetime — this makes the rejection auditable.

##### `GET /events/rejected/count` — Count Rejected Events

```bash
# Total rejected
curl "http://localhost:8000/events/rejected/count"

# By reason
curl "http://localhost:8000/events/rejected/count?rejection_reason=duplicate_event_id"

# By service and reason
curl "http://localhost:8000/events/rejected/count?service=payments&rejection_reason=invalid_timestamp"
```

Response:
```json
{
  "count": 4,
  "filters": {
    "service": "payments",
    "event_type": null,
    "rejection_reason": "invalid_timestamp"
  }
}
```

---

### Testing the API

```bash
# Run all tests (48 tests, 87% coverage)
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=term-missing

# API tests only
pytest tests/test_api.py -v
```

Tests use `TestClient` (HTTPX-backed) with in-memory fixtures injected via dependency setters. The lifespan startup is patched so no pipeline output files are required on disk during test runs.

```bash
# Run linter
pylint src/ --disable=C0111,R0903
```

---

## Part 4: Code Quality & Reliability

### Data Quality Decisions (drop vs keep)

I used a pragmatic schema-on-read approach with three layers of quality handling:

- **Drop event** when critical identifiers are missing or invalid:
  - `event_id`, `timestamp`, `service`, `event_type`
- **Keep event with imputation/normalization** for non-critical attributes:
  - `user_id`: imputed to `UNKNOWN_USER` when missing
  - `latency_ms`: set to null when negative
  - `status_code`: set to null when outside `[100, 599]`
- **Unexpected fields** (e.g., `extra_field`):
  - Event is kept if critical fields are valid
  - Extra fields are excluded from the curated silver schema
  - Presence of unknown fields is tracked as a data-quality signal

This preserves usable data while keeping analytics outputs stable and schema-consistent.

---

### What I would monitor in production

#### Data Pipeline

- **Pipeline reliability**
  - Ingestion rate (events/sec)
  - End-to-end latency (arrival to silver/gold availability)
  - Job failures/retries and checkpoint lag
- **Data quality KPIs**
  - Rejection rate overall and by reason
  - Null rates per key column (`user_id`, `latency_ms`, `status_code`)
  - Invalid timestamp rate
  - Unknown field occurrence rate (schema drift indicator)
- **Output integrity**
  - Row counts between bronze/silver/gold
  - Metric freshness (latest `time_window`)
  - Distribution checks (latency and error-rate anomalies)

#### API

- **API health**
  - Request rate (req/sec) and error rate (4xx/5xx) per endpoint
  - P50/P95/P99 response latency per endpoint
  - Startup time and last successful data load timestamp
- **Resource utilization**
  - RSS memory consumed by the in-process DataFrame store
  - CPU usage per uvicorn worker (spike = large DataFrame serialization)
  - Open file descriptors (parquet reads on each restart)
- **Data freshness**
  - Age of the loaded DataFrame vs. latest pipeline run (`time_window` lag)
  - Stale data alert if silver/gold files haven't been updated within SLA window
- **Request quality**
  - Rate of 503 responses (events/metrics store not loaded)
  - Distribution of `limit` values used — high values signal missing pagination
  - Most-queried filter combinations (informs caching strategy)

---

### What could break as data volume grows

#### Data Pipeline

- **Memory pressure** from pandas in a single process
- **Small file problem** if writing too many tiny output files
- **Slow group-bys** for per-minute/service aggregations at high cardinality
- **Backfill/reprocessing cost** without partitioning and incremental logic
- **Schema drift impact** if unknown fields increase and no governance exists

#### API

- **API memory bottleneck** from loading the entire events/metrics DataFrame into RAM on startup — all requests share the same in-process copy with no eviction
- **No pagination on large responses** — `/events` and `/metrics` return up to 1000 rows per request; at high volume this increases serialization time and HTTP response size significantly
- **Global state contention** — the in-memory store uses module-level globals; under concurrent load (multiple uvicorn workers or threads) reads are safe but any future write path would require locking
- **No query-time filtering pushdown** — all filters (`service`, `event_type`, `rejection_reason`) are applied in Python after loading the full DataFrame into a local copy; at millions of rows this is O(n) per request
- **Single uvicorn process** — the current setup runs one worker; CPU-bound filter/serialization on large DataFrames blocks the async event loop, degrading latency for all concurrent requests
- **No caching layer** — repeated identical queries (e.g. `/metrics?service=checkout`) recompute the filter and serialization on every call; a TTL cache (e.g. `functools.lru_cache` or Redis) would eliminate this cost
- **Cold start latency** — if the API is restarted, parquet files must be re-read from disk before serving any request; at large file sizes this increases the unavailability window

---

### Next improvements with more time

1. **Incremental + partitioned processing**
   - Partition by event date/hour and process only new windows
   - Add idempotent writes and replay-safe logic
   - Stamp every record with a `load_timestamp` (pipeline execution time) at write time in the cleaner; use it as the watermark for incremental runs — only events with `load_timestamp > last_run` are reprocessed, avoiding full bronze/silver scans
2. **Stronger quality contracts**
   - Introduce explicit schema validation rules and versioning
   - Emit a formal DQ report artifact and alert thresholds
3. **API scalability**
   - Replace the in-process DataFrame store with a lightweight query layer (e.g. DuckDB over parquet) to eliminate the full-file load on startup and enable real filter pushdown
   - Add cursor-based pagination (`after_id`, `page_size`) to `/events` and `/metrics` instead of relying solely on `limit`
   - Introduce a TTL response cache (e.g. `functools.lru_cache` or Redis) for high-frequency read queries like `/metrics?service=checkout`
   - Run multiple uvicorn workers (`--workers N`) to prevent CPU-bound serialization from blocking the async event loop under concurrent load

### Optional improvements

- Add contract tests for schema changes and edge-case fixtures
- Move heavy transformations to a scalable engine (Beam/Spark) while preserving current business rules

---

## Disclaimer

Parts of this project were developed with the assistance of **LLM-based tools**. Specifically:

- **API layer** (`src/api/`): endpoint structure and lifespan pattern were iterated with AI assistance.
- **Test suite** (`tests/`): test design, fixture patterns, and mock strategies for the FastAPI `lifespan` context were co-developed with AI tooling.
- **Documentation**: sections of this README were drafted and refined with AI assistance.

All code was reviewed, understood, and validated by me.
