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
- ❌ More storage (3x data duplication)

### Why PostgreSQL?

**For learning**: Free, local, same SQL as Redshift
**Production**: Good up to ~100M rows

## Quick Start
```bash
docker-compose up -d
docker exec customer360_db pg_isready -U dataeng
psql -h localhost -U dataeng -d customer360
```

## Day 1 Completed ✅
- PostgreSQL with 3-layer architecture
- Docker infrastructure
- Init schemas created
