# Stock Market Data Analysis Platform

A full end-to-end big data platform that combines 30 years of historical US stock market data with a real-time trade feed, running entirely on a single machine via Docker Compose.

The platform ingests, transforms, and analyses ~4,200 stock tickers across four exchanges (NASDAQ, NYSE, S&P 500, Forbes 2000) through a batch pipeline, while simultaneously processing live trade events through a streaming pipeline. Results are served through a PostgreSQL database and visualised in Metabase.

---

## Tech Stack

| Layer | Technologies |
|---|---|
| Storage | Apache Hadoop HDFS, Apache Parquet |
| Processing | Apache Spark (PySpark), Apache Hive |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Orchestration | Apache Airflow |
| Serving | PostgreSQL |
| Visualisation | Metabase |
| Infrastructure | Docker Compose |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│   CSV files (4,194 files, 4 exchanges, 1993–2022)               │
│   Finnhub WebSocket API (50 live tickers)                       │
└──────────────────┬──────────────────────┬───────────────────────┘
                   │                      │
                   ▼                      ▼
         ┌─────────────────┐    ┌──────────────────────┐
         │  Batch Pipeline │    │  Streaming Pipeline   │
         │  (Apache Spark) │    │  Kafka + Spark        │
         │                 │    │  Structured Streaming │
         │ Bronze (Raw)    │    │  5 consumers          │
         │      ↓          │    │  windowed aggregation │
         │ Silver (Clean)  │    └──────────┬───────────┘
         │      ↓          │               │
         │ Gold (Metrics)  │               │
         │      ↓          │               │
         │ 10 SQL queries  │               │
         └────────┬────────┘               │
                  │                        │
                  ▼                        ▼
         ┌─────────────────────────────────────────┐
         │            PostgreSQL                    │
         │  q01–q10 (batch results)                 │
         │  sq1–sq5 (streaming aggregations)        │
         └──────────────────┬──────────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │   Metabase    │
                    │  Dashboards   │
                    └───────────────┘
```

---

## Data

**Historical batch data** — 4,194 CSV files (~9.6 GB) covering daily OHLCV prices from 1993 to 2022:

| Exchange | Tickers | Size |
|---|---|---|
| NASDAQ | 1,564 | 3.4 GB |
| NYSE | 1,145 | 2.7 GB |
| Forbes 2000 | 1,076 | 2.2 GB |
| S&P 500 | 409 | 1.3 GB |

**Real-time streaming data** — Live trade events from the [Finnhub WebSocket API](https://finnhub.io) for 50 major US tickers, published to Kafka and consumed by 5 Spark Structured Streaming jobs.

---

## Batch Pipeline

Six PySpark jobs run in sequence, implementing the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture):

```
01_load_raw_layer.py              CSV files → HDFS Parquet (Bronze)
02_process_stock_prices.py        Deduplicate, clean, derive columns (Silver)
03_daily_stock_metrics.py         Rolling window metrics per ticker (Gold)
04_monthly_stock_performance.py   Monthly aggregations (Gold)
05_yearly_stock_performance.py    Yearly aggregations (Gold)
06_export_to_postgres.py          10 analytical queries → PostgreSQL
```

Key transformations in the Silver layer:
- Deduplication on `(ticker, trade_date)` — removes overlap between exchanges
- Data quality filters (zero prices, impossible OHLCV relationships)
- Derived columns: `daily_range`, `daily_change`, `daily_change_pct`
- Dimension tables: `exchange_dim`, `ticker_exchanges`

Key metrics computed in the Gold layer:
- 20-day and 7-day average volume, rolling 7-day high/low
- Volume ratio + volume spike flag (> 2× 20-day average)
- 30-day rolling volatility (stddev of daily return)
- Monthly and yearly return percentages, intraday range statistics

---

## Analytical Queries

Ten SQL queries run against the Hive Gold layer and persist results in PostgreSQL:

| Query | Question |
|---|---|
| q01 | Top 3 performing stocks per exchange per year |
| q02 | Year-over-year return change per ticker |
| q03 | Biggest monthly drop relative to each stock's own historical average |
| q04 | Largest 3-month recovery after a down month |
| q05 | Cumulative return ranking within each year |
| q06 | Stocks that consistently outperformed their exchange average |
| q07 | Trading days where rolling volatility exceeded the stock's annual average |
| q08 | Volume spike days and next-day price direction (up/down/flat) |
| q09 | Intraday volatility percentile rank within exchange and month |
| q10 | Longest consecutive positive-return month streaks |

---

## Streaming Pipeline

A Python producer subscribes to 50 tickers via the Finnhub WebSocket and publishes trade events to a Kafka topic (`stock_trades`). Five Spark Structured Streaming consumers process micro-batches and write windowed results to PostgreSQL:

| Consumer | Window | Output |
|---|---|---|
| sq1 — Price Volatility | 10 min tumbling | Avg price, stddev, volatility coefficient per ticker |
| sq2 — Market Sentiment | 1 min tumbling | Rising/falling stock counts, sentiment ratio per exchange |
| sq3 — Trade Frequency | 10 min tumbling | Trade count vs 7-day baseline, frequency spike flag |
| sq4 — Price vs 7d Range | 5 min tumbling | Distance from 7-day high/low, position classification |
| sq5 — vs Previous Close | 1 min tumbling | Pct change from yesterday's close, direction classification |

sq2, sq3, sq4, and sq5 use a **stream-batch join** pattern: a static Hive table (baseline prices and 7-day extremes) is broadcast-joined against each micro-batch to provide historical context the stream alone cannot supply.

---

## Prerequisites

- Docker Desktop (16 GB RAM recommended, 14 GB minimum)
- A [Finnhub](https://finnhub.io) API key (free tier) for the streaming pipeline
- Git

---

## Getting Started

### 1. Clone and configure

```bash
git clone https://github.com/fikcija/stock-market-data-analysis.git
cd stock-market-data-analysis
```

Add your Finnhub API key to `src/config/config.env`:

```
FINNHUB_API_KEY=your_key_here
```

### 2. Start all services

```bash
docker-compose up -d
```

Wait ~60 seconds for services to initialise, then force HDFS out of safe mode:

```bash
docker exec namenode hdfs dfsadmin -safemode leave
```

### 3. Register Hive tables

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1000m --executor-cores 1 \
  /batch/jobs/00_register_hive_tables.py
```

> **Note:** Re-run this step after every `docker-compose down` — the Hive metastore loses its state when the container is recreated.

### 4. Run the batch pipeline

**Option A — Airflow (recommended):**

Open [http://localhost:8090](http://localhost:8090) (user: `airflow`, pass: `airflow`), find the `stock_batch_pipeline` DAG, and click **Trigger DAG**.

**Option B — Manual:**

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1000m --executor-cores 1 --num-executors 2 \
  --conf spark.sql.shuffle.partitions=50 \
  --conf spark.sql.adaptive.enabled=true \
  /batch/jobs/01_load_raw_layer.py
```

Repeat for jobs 02 through 06 in order. Job 06 additionally requires `--jars /batch/lib/postgresql-42.5.1.jar`.

### 5. Start the streaming pipeline

```bash
# Load daily baseline data (run once per day before starting consumers)
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /batch/jobs/11_load_streaming_baselines.py

# Start all 5 consumers
docker exec spark-master bash /stream/consumer/start_streaming_pipeline.sh
```

The Finnhub producer starts automatically with `docker-compose up`. Live data is only available during US market hours (Monday–Friday, 9:30 AM – 4:00 PM ET).

---

## Web Interfaces

| Service | URL | Credentials |
|---|---|---|
| Metabase | http://localhost:3000 | Set on first login |
| Airflow | http://localhost:8090 | airflow / airflow |
| Spark Master UI | http://localhost:8080 | — |
| HDFS NameNode | http://localhost:9870 | — |
| Hue | http://localhost:8888 | — |
| YARN | http://localhost:8088 | — |

---

## Project Structure

```
├── airflow/
│   ├── dags/
│   │   ├── batch_pipeline_dag.py       # 6-task linear DAG
│   │   └── streaming_pipeline_dag.py
│   └── Dockerfile
├── src/
│   ├── batch/
│   │   ├── jobs/                       # 7 PySpark jobs (01–06 + 11)
│   │   ├── queries/                    # 10 analytical SQL files (q01–q10)
│   │   └── hive_ddl/                   # Hive EXTERNAL table definitions
│   ├── stream/
│   │   ├── producer/                   # Finnhub WebSocket → Kafka
│   │   ├── consumer/                   # 5 Spark Structured Streaming consumers
│   │   └── init_pg.sql                 # Streaming tables DDL
│   └── config/
│       ├── config.env                  # Environment variables
│       ├── spark-defaults.conf
│       └── hive-site.xml
├── data/
│   └── stock_market_data/              # Raw CSV files (nasdaq, nyse, sp500, forbes2000)
├── docs/
│   └── plans/
│       └── project_documentation.pdf  # Full project guide
└── docker-compose.yml
```

---

## Key Design Decisions

- **Medallion Architecture** — Raw, processed, and aggregated data live in separate HDFS layers. Any layer can be reprocessed without touching upstream data.
- **EXTERNAL Hive tables** — Hive metadata is ephemeral (lost on container restart); data in HDFS is permanent. EXTERNAL tables ensure a metastore reset never deletes data.
- **oom_score_adj: -1000 on HDFS nodes** — When Linux kills a process under memory pressure, it should kill a Spark executor (which retries automatically) rather than a DataNode (which can corrupt in-flight writes).
- **HDFS replication factor 1** — Reduces memory pressure during heavy Spark writes on a memory-constrained machine. Acceptable trade-off for a development environment.
- **Kafka between Finnhub and Spark** — Decouples the producer from 5 consumers; no trades lost when a consumer restarts; a single API connection fans out to all consumers.
- **foreachBatch for streaming writes** — PostgreSQL JDBC is not a native Structured Streaming sink. foreachBatch provides a regular DataFrame per micro-batch that can be written anywhere, and also enables the stream-batch join pattern inside the loop.
