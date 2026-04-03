# 🌬️ AirWatch MX — Air Quality Data Pipeline

![Pipeline Status](https://img.shields.io/badge/pipeline-active-brightgreen)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apache-airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-deployed-FF4B4B?logo=streamlit)

End-to-end data pipeline that extracts real-time air quality data from the OpenAQ API, 
processes and stores it in PostgreSQL, orchestrates execution with Apache Airflow, 
and visualizes results in an interactive Streamlit dashboard.

**🔗 [Live Dashboard](https://air-quality-pipeline.streamlit.app)** | 
**📊 Data source:** [OpenAQ API](https://openaq.org)

---

## 📐 Architecture
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   OpenAQ API    │───▶│  Extract Layer   │───▶│   data/raw/     │
│  (REST + Auth)  │    │  requests + auth │    │   JSON files    │
└─────────────────┘    └──────────────────┘    └────────┬────────┘
│
▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│ Transform Layer  │◀───│  Raw JSON data  │
│  (Neon Cloud)   │    │ pandas + pydantic│    │                 │
└────────┬────────┘    └──────────────────┘    └─────────────────┘
│
▼
┌─────────────────┐    ┌──────────────────┐
│    Streamlit    │◀───│ Apache Airflow   │
│  Live Dashboard │    │  DAG @hourly     │
└─────────────────┘    └──────────────────┘

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Extraction | Python + Requests | REST API consumption with auth |
| Transformation | Pandas + Pydantic | Data cleaning and validation |
| Storage | PostgreSQL 15 (Neon) | Cloud relational database |
| Orchestration | Apache Airflow 2.9 | Pipeline scheduling and monitoring |
| Visualization | Streamlit + Plotly | Interactive dashboard |
| Infrastructure | Docker + Docker Compose | Local development environment |

---

## 📁 Project Structure
air-quality-pipeline/
│
├── dags/
│   └── air_quality_dag.py       # Airflow DAG definition
│
├── etl/
│   ├── extract/
│   │   └── openaq_extractor.py  # API extraction with error handling
│   ├── transform/
│   │   └── cleaner.py           # Data cleaning and validation
│   └── load/
│       └── postgres_loader.py   # PostgreSQL UPSERT loader
│
├── dashboard/
│   └── app.py                   # Streamlit dashboard
│
├── data/
│   ├── raw/                     # Raw JSON from API (gitignored)
│   └── processed/               # Cleaned data (gitignored)
│
├── tests/                       # Unit tests
├── docs/                        # Architecture diagrams
├── docker-compose.yml           # PostgreSQL + Airflow infrastructure
└── requirements.txt             # Python dependencies

---

## ⚙️ Technical Decisions

**Why Airflow over cron jobs?**  
Airflow provides retry logic, dependency management, and execution history — 
critical for production pipelines where silent failures are unacceptable.

**Why UPSERT instead of INSERT?**  
The pipeline runs hourly. Using `INSERT ON CONFLICT DO UPDATE` prevents 
duplicate records while keeping the latest station metadata updated.

**Why separate raw and processed data layers?**  
Following the medallion architecture pattern (raw → clean → serving). 
Raw data is immutable — if transformation logic changes, we can reprocess 
without re-hitting the API.

**Why Neon for cloud PostgreSQL?**  
Serverless PostgreSQL with connection pooling built-in. Zero-config SSL, 
compatible with any psycopg2/pg8000 client, generous free tier.

---

## 🚀 Local Setup

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### 1. Clone and configure
```bash
git clone https://github.com/EmilianoMAl/air-quality-pipeline.git
cd air-quality-pipeline
```

Create `.env` file:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=air_quality
DB_USER=pipeline_user
DB_PASSWORD=pipeline123
OPENAQ_API_KEY=your_api_key_here
NEON_DATABASE_URL=your_neon_url_here
```

### 2. Start infrastructure
```bash
docker compose up -d
```

### 3. Initialize database
```bash
# Create database and user
docker exec -it air-quality-pipeline-postgres-1 psql -U airflow -c "CREATE DATABASE air_quality;"
docker exec -it air-quality-pipeline-postgres-1 psql -U airflow -c "CREATE USER pipeline_user WITH PASSWORD 'pipeline123';"
docker exec -it air-quality-pipeline-postgres-1 psql -U airflow -c "GRANT ALL PRIVILEGES ON DATABASE air_quality TO pipeline_user;"

# Apply schema
Get-Content etl/load/schema.sql | docker exec -i air-quality-pipeline-postgres-1 psql -U pipeline_user -d air_quality
```

### 4. Run the pipeline manually
```bash
python -m etl.extract.openaq_extractor
python -m etl.transform.cleaner
python -m etl.load.postgres_loader
```

### 5. Access services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Streamlit | http://localhost:8501 | — |

---

## 📊 Pipeline Details

The Airflow DAG `air_quality_pipeline` runs **@hourly** and executes these tasks in sequence:
start → extract_openaq → transform_clean → load_postgres → end

Each task has:
- **3 automatic retries** with 5-minute delay
- **Full execution logs** in Airflow UI
- **XCom** for passing data between tasks

---

## 🔑 Getting an OpenAQ API Key

1. Register at [explore.openaq.org/register](https://explore.openaq.org/register)
2. Confirm your email
3. Copy your API key from [explore.openaq.org/account](https://explore.openaq.org/account)

---

*Built by Emiliano — Data Engineering Portfolio*


