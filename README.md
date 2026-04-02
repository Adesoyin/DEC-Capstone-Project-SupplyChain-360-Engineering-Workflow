# SupplyChain360 — Data Engineering Platform

A production-grade data platform that centralizes supply chain data from multiple systems into a single, analytics-ready warehouse using an ELT architecture.

---

## Problem

SupplyChain360 operates with fragmented data across multiple systems:
- No single source of truth  
- Manual reporting  
- Poor visibility into inventory, suppliers, and sales  
- CSV/JSON files in S3 (external account)
- Google Sheets for store data
- PostgreSQL for daily sales

This results in:
- Stockouts  
- Overstocking  
- Delayed shipments  
- Slow decision-making  

---

## Solution

This platform unifies all data into a structured pipeline:

**Sources → S3 (Parquet) → Snowflake → dbt models → Analytics**

- Airflow orchestrates the entire workflow  
- Terraform provisions infrastructure  
- Docker ensures consistent environments  
- GitHub Actions handles CI/CD  

---

## Architecture (Simplified)

![alt text](images/Architectural%20Image.png)

Sources (S3, Google Sheets, PostgreSQL)
│
▼
Python Ingestion (Airflow DAGs)
│
▼
S3 (Parquet - Raw Layer)
│
▼
Airbyte (S3 → Snowflake)
│
▼
Snowflake (RAW → STAGING → MARTS)
│
▼
dbt (Transform + Test)


---

## Tech Stack (and WHY)

| Component | Tool | Why |
|----------|------|-----|
| Storage | S3 (Parquet) | Cheap, durable, and preserves schema vs CSV |
| Ingestion | Python (Airflow) | Full control to clean/validate messy source data |
| Data Movement | Airbyte | Handles Snowflake loading efficiently |
| Warehouse | Snowflake | Scales compute independently, strong SQL engine |
| Transformation | dbt | SQL-based, testable, version-controlled |
| Orchestration | Airflow (Docker) | Flexible scheduling + dependency management |
| Infra | Terraform | Reproducible, version-controlled infrastructure |
| CI/CD | GitHub Actions | Automates testing and Docker builds |

---

## Key Design Decisions (with WHY)

### 1. S3 as Raw Layer (Parquet)
- Acts as a durable, reusable data source  
- Enables reprocessing without re-extraction  
- Parquet ensures typed and compressed data  

![alt text](images/s3%20parquet%20objects.png)

---

### 2. Python Before Airbyte
- Source data is messy (JSON, inconsistent schema)  
- Python layer:
  - Cleans  
  - Flattens JSON 
  - Validates and added Metadata such as loaded date 

Prevents bad data from reaching Snowflake  

---

### 3. Airbyte Only for S3 → Snowflake (Triggered by Airflow)
**What this actually means in this project:**
- Airbyte is **NOT scheduled**
- All connections are set to **Manual**
- Airflow triggers Airbyte via DAG

![alt text](images/Airbyte%20s3%20to%20warehouse%20orchestration.png)

**Why:**
- Avoid conflicts (Airbyte allows only one active job per connection)
- Keep orchestration in one place (Airflow)

**What Airbyte handles:**
- Loading Parquet files into Snowflake  
- Managing incremental loads (new partitions only)  
- Using Snowflake `COPY INTO` efficiently  

**Why not build custom loaders?**
- Would require handling:
  - File staging  
  - Incremental logic  
  - Schema evolution  

Airbyte removes this complexity.

---

### 4. ELT with dbt (Not ETL)
- Transform inside Snowflake for scalability  
- Provides:
  - Modular SQL models  
  - Testing  
  - Documentation  

![alt text](images/dbt%20docs.png)

![alt text](images/database.png)

![alt text](images/dbt%20dag%20run.png)

---

### 5. Airflow in Docker (Self-hosted)
- Full control and zero infrastructure cost  
- Same environment across dev and production  

**Trade-off:**
- No high availability  
- Requires manual restart if services stop  

---

### 6. Terraform for Infrastructure
- Infrastructure is reproducible and version-controlled  
- Eliminates manual setup errors  

**Important Note:**
- Terraform state bucket was created manually  
- Reason: Terraform cannot manage its own backend initially 

![alt text](images/terraform%20tfstate.png)

**Trade-off:**
- No high availability  
- Requires manual restart if services stop  
---

## Trade-offs

| Decision | Trade-off |
|--------|----------|
| S3 → Snowflake (2-step pipeline) | Adds latency but improves reliability and reprocessing |
| Python ingestion + Airbyte | More components but cleaner data vs simpler direct ingestion |
| Airflow self-hosted | No high availability, requires manual management but zero cost |
| Airbyte Cloud | External dependency and authentication complexity |
| dbt staging as views | Query-time cost vs storage savings |

---

## Data Model

Star schema design:

- **Dimensions:** products, suppliers, stores, warehouses  
- **Facts:** sales, shipments, inventory  

![alt text](images/lineage%20graph.png)


Supports:
- Stockout analysis  
- Supplier performance  
- Demand trends  

---

## Orchestration Flow

1. Ingest data → S3  
2. Trigger Airbyte sync  
3. Load into Snowflake  
4. Run dbt models  
5. Run dbt tests  

![alt text](images/dags.png)

---

## Data Quality

Three layers:

1. **Ingestion (Python)**  
   - Validate Parquet before upload  
   - Prevent corrupt files 

2. **dbt Tests**  
   - `not_null`, `unique`, relationships  

3. **Pipeline Enforcement**  
   - Fail pipeline if tests fail  

---

## Key Learnings

- Airbyte must be manual-triggered to avoid conflicts  
- Parquet issues often come from incorrect data types  
- Separating ingestion from transformation improves reliability  
- Partitioning of the daily tables in s3 simplifies debugging and reprocessing  

---