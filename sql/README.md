# Customer 360 - Production-Grade Data Platform

## Architecture Decision Record (ADR)

### Why 3-layer architecture (raw/staging/warehouse)?

**Problem**: Need to balance data freshness, auditability, and transformation complexity.

**Solution**: Medallion architecture with clear separation of concerns.

- **raw/**: Immutable landing zone (as-is from sources)
- **staging/**: Light cleanup (typing, normalization)
- **warehouse/**: Business logic (deduplication, metrics)

**Trade-offs**:
- ✅ Easy rollback (re-run staging without re-ingesting)
- ✅ Audit trail (raw data never changes)
- ✅ Clear ownership (raw = ingestion, staging = cleanup, warehouse = analytics)
- ❌ More storage (3x data duplication)
- ❌ Slightly more complex DAGs

**Alternatives considered**:
- Single schema: Rejected (no separation of concerns, hard to debug)
- Two schemas: Rejected (no safety net between raw and business logic)

### Why PostgreSQL over Snowflake/BigQuery?

**For learning/portfolio**: 
- Free (no cloud costs)
- Runs locally (no internet required)
- Same SQL as Redshift (transferable skills)

**Production considerations**: 
- PostgreSQL good up to ~100M rows
- Beyond that, consider columnar DBs (Snowflake, BigQuery, ClickHouse)

## Quick Start
```bash
# Start infrastructure
docker-compose up -d

# Verify Postgres is ready
docker exec customer360_db pg_isready -U dataeng

# Connect to database
psql -h localhost -U dataeng -d customer360
```

## Project Structure
```
customer360-prod/
├── docker-compose.yml    # Infrastructure definition
├── sql/
│   └── init_schemas.sql  # Database initialization
└── README.md             # This file
```
