# DataGraft

DataGraft is a deterministic data ingestion pipeline for uploading messy CSV or Excel files, detecting column types, mapping source columns to a target schema with RapidFuzz, validating row-level data quality, and exporting only clean rows through chunked CSV streaming. The backend is designed around explicit pipeline states so users cannot skip detection, mapping, validation, or export steps.

## Tech Stack

- FastAPI
- PostgreSQL
- SQLAlchemy async
- Pandas
- RapidFuzz
- Docker Compose
- React
- Vite
- Axios
- Tailwind CSS
- Pytest

## Run Locally

From the project root:

```bash
docker compose up --build
```

The backend runs at:
```bash
http://localhost:8000
```

The frontend runs at:
```bash
http://localhost:3000
```

API docs are available at:
```bash
http://localhost:8000/docs
```

## Basic Backend Flow

1. Upload a file
2. Detect source columns
3. Generate mapping suggestions
4. Confirm mappings
5. Validate rows
6. Export clean CSV

The export endpoint only returns rows that passed validation.

## Verify

Run:

```bash
git diff README.md
```
