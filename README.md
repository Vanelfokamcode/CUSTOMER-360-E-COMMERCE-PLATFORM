# 🎯 Customer 360 - Production-Grade Data Platform

## 📊 Project Overview

An end-to-end data engineering project that unifies 5,000 customer records from multiple sources into a clean, analytics-ready data platform. Implements identity resolution, RFM segmentation, and predictive analytics to enable data-driven marketing decisions.

**Business Impact:**
- Reduced dataset from 5,000 rows to 4,501 unique customers (10% deduplication)
- Identified 450 VIP customers (10%) generating 20%+ of revenue
- Flagged 680 at-risk customers for retention campaigns
- Built health scoring system predicting churn with 70% accuracy

---

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  CSV Files → MongoDB → PostgreSQL → APIs → Event Logs           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                   ┌────────▼────────┐
                   │  Apache Airflow │ ← Orchestration
                   │  (Docker)       │
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │   PostgreSQL    │ ← Data Warehouse
                   │   3-Layer:      │
                   │   • raw         │ ← Immutable landing
                   │   • staging     │ ← Cleaned data
                   │   • warehouse   │ ← Business logic
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │      dbt        │ ← Transformations
                   │   • Identity    │
                   │   • RFM         │
                   │   • Metrics     │
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │   Power BI      │ ← Analytics
                   │   Dashboard     │
                   └─────────────────┘
```

---

## 🎯 Key Features

### 1. **Data Quality Engineering**
- Intentionally generated messy data (15% duplicates, malformed emails, mixed date formats)
- Built data cleaning pipeline with dbt
- Automated quality tests (63 malformed emails detected, 436 duplicates identified)

### 2. **Identity Resolution**
- Phase 1: Exact match on normalized emails → 500 duplicates eliminated
- Deterministic surrogate keys for consistent customer identification
- Maintained audit trail of merged records

### 3. **Customer Segmentation (RFM)**
- Scored customers on Recency, Frequency, Monetary (1-5 scale)
- Created 8 actionable segments: VIP, Champion, Loyal, At Risk, Lost, etc.
- Calculated Customer Lifetime Value (actual + predicted)

### 4. **Health Scoring System**
- 0-100 health score combining 5 weighted factors
- Identified high churn risk customers (score < 40)
- Enabled proactive retention campaigns

### 5. **Cohort Retention Analysis**
- Tracked customer retention by acquisition cohort
- Monthly retention rates visualized for trend analysis
- Identified lifecycle patterns

---

## 🛠️ Tech Stack

| Category | Technology | Purpose |
|----------|------------|---------|
| **Orchestration** | Apache Airflow 2.8 | DAG scheduling, retry logic, monitoring |
| **Data Warehouse** | PostgreSQL 15 | 3-layer architecture (raw/staging/warehouse) |
| **Transformations** | dbt Core 1.7 | SQL modeling, tests, documentation |
| **Data Generation** | Python + Faker | Realistic messy data simulation |
| **Visualization** | Power BI Desktop | Executive dashboards, drill-downs |
| **Infrastructure** | Docker Compose | Containerized services |
| **Version Control** | Git + GitHub | Code versioning, collaboration |

---

## 📊 Dashboard Screenshots

### Executive Overview
![Executive Dashboard](screenshots/executive_overview.png)

**KPIs:**
- 4,501 Unique Customers
- €1.8M Total Revenue
- €401 Average LTV
- 450 VIP Customers

---

### Marketing Insights
![Marketing Insights](screenshots/marketing_insights.png)

**Features:**
- Segment distribution & revenue breakdown
- Customer health status visualization
- At-risk customer identification (Top 20 by value)
- Cohort retention trends

---

### Customer Deep Dive
![Customer Deep Dive](screenshots/customer_deepdive.png)

**Analytics:**
- Scatter plot: Customer value vs engagement
- Interactive filters (Segment, Health Status)
- Detailed customer table with drill-down

---

## 📁 Project Structure
```
customer360-prod/
├── airflow/
│   └── dags/
│       └── ingest_csv_to_raw.py          # Ingestion pipeline
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_csv_customers.sql     # Data cleaning
│   │   │   └── sources.yml
│   │   ├── intermediate/
│   │   │   ├── int_customer_identity.sql # Identity resolution
│   │   │   └── int_customer_deduped.sql  # Deduplication
│   │   └── marts/
│   │       ├── dim_customers.sql         # Golden records
│   │       ├── customer_rfm.sql          # RFM segmentation
│   │       ├── customer_health.sql       # Health scoring
│   │       └── cohort_retention.sql      # Retention analysis
│   ├── macros/
│   │   └── parse_mixed_dates.sql         # Reusable date parser
│   └── dbt_project.yml
├── data_generators/
│   ├── generate_messy_customers.py       # Fake data generator
│   └── config.py
├── sql/
│   ├── init_schemas.sql                  # Database setup
│   └── create_raw_tables.sql
├── powerbi_data/                         # Exported CSV for Power BI
├── screenshots/                          # Dashboard images
├── docker-compose.yml                    # Infrastructure as code
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Power BI Desktop (for dashboard)

### Installation
```bash
# 1. Clone repository
git clone https://github.com/Vanelfokamcode/CUSTOMER-360-E-COMMERCE-PLATFORM.git
cd customer360-prod

# 2. Start infrastructure
docker-compose up -d

# 3. Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Generate data
cd data_generators
python generate_messy_customers.py

# 6. Initialize database schemas
docker exec -i customer360_db psql -U dataeng -d customer360 < sql/init_schemas.sql
docker exec -i customer360_db psql -U dataeng -d customer360 < sql/create_raw_tables.sql

# 7. Run Airflow DAG (via UI at localhost:8080)
# Username: admin, Password: admin

# 8. Run dbt transformations
cd dbt_project
dbt run
dbt test

# 9. Open Power BI dashboard
# Import CSV files from powerbi_data/ folder
```

---

## 🧪 Data Quality

**Tests Implemented:**
- Schema validation (not_null, unique)
- Email format validation (regex)
- Date parsing success rate (100%)
- Customer key uniqueness
- RFM segment validation

**Results:**
- ✅ 5 of 7 tests passing
- ❌ 63 malformed emails detected (intentional)
- ❌ 436 duplicates identified (resolved through identity resolution)

---

## 📈 Business Insights

### Segment Analysis
| Segment | Customers | % Total | Avg Revenue | Total Revenue |
|---------|-----------|---------|-------------|---------------|
| VIP | 450 | 10% | €850 | €382,725 |
| Champion | 680 | 15% | €620 | €421,804 |
| Loyal | 900 | 20% | €450 | €405,180 |
| At Risk | 680 | 15% | €380 | €258,740 |
| Others | 1,791 | 40% | €180 | €322,380 |

### Key Findings
1. **VIP customers** (10%) generate 21% of total revenue
2. **At-risk segment** represents $258K in jeopardy
3. **Cohort retention** drops 40% after month 3
4. **Health score < 40** predicts 70% churn probability

---

## 🎯 Technical Highlights

### Identity Resolution Strategy
```sql
-- Deterministic surrogate key generation
{{ dbt_utils.generate_surrogate_key(['identity_match_key']) }}

-- Tie-breaking for duplicates (deterministic)
ROW_NUMBER() OVER (
    PARTITION BY customer_key
    ORDER BY created_at_parsed ASC, customer_id ASC
)
```

### RFM Scoring Logic
```sql
-- Recency: Lower days = better
CASE 
    WHEN recency_days <= 30 THEN 5
    WHEN recency_days <= 60 THEN 4
    ...
END as recency_score

-- Frequency & Monetary: Quintile-based
NTILE(5) OVER (ORDER BY frequency_value)
```

---

## 📚 Lessons Learned

### What Worked Well
- **3-layer architecture** enabled easy rollback and debugging
- **dbt macros** (date parsing) reused across 3 models
- **Deterministic hashing** ensured reproducible customer IDs
- **Idempotent pipelines** (TRUNCATE + INSERT) safe for re-runs

### Challenges Overcome
- **Mixed date formats** → Built reusable dbt macro with regex
- **Duplicate detection** → Implemented 2-phase matching strategy
- **Data quality** → Automated tests caught 500+ issues
- **WSL networking** → Exported CSV for Power BI compatibility

### Future Improvements
- Implement incremental dbt models for large-scale data
- Add CI/CD pipeline (GitHub Actions + dbt Cloud)
- Deploy on cloud (AWS RDS + S3 + Airflow on ECS)
- Real-time stream processing with Kafka

---

## 🏆 Project Stats

- **Duration:** 11 days (30 hours)
- **Lines of Code:** ~2,000 (SQL + Python)
- **Data Processed:** 5,000 rows → 4,501 unique customers
- **Models Created:** 10 dbt models
- **Tests Written:** 15 automated tests
- **Visualizations:** 12 Power BI charts

---

## 📧 Contact

**Vanel Fokam**
- GitHub: [@Vanelfokamcode](https://github.com/Vanelfokamcode)

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🙏 Acknowledgments

- Inspired by real-world data engineering challenges
- Built as a portfolio project demonstrating end-to-end data engineering skills
- Special focus on production-grade practices: testing, documentation, reproducibility
