# Employee Attendance & Analytics API

A FastAPI + MongoDB service for employee punch-in / punch-out, manual corrections with an audit trail, and analytics built on MongoDB aggregation pipelines.

All application code is in a single file: `app/main.py`.

## Repository contents

| File | Purpose |
|---|---|
| `app/main.py` | The whole application |
| `requirements.txt` | Python dependencies |
| `REVIEW.md` | Part A: defects found in the starter code and how each was fixed |
| `DECISIONS.md` | Part C: answers to the five design questions |
| `README.md` | This file |

## Requirements
- Python 3.11+
- MongoDB 6.0 or newer (needed for `$densify`, `$setWindowFields`, `$rank`)

## How to run
```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="attendance_db"

uvicorn app.main:app --port 8000
```
- `MONGO_URI` and `MONGO_DB` are read from environment variables (a local `.env` is also loaded; real environment variables take priority). Nothing is hard-coded.
- All indexes are created automatically at startup, and creating them again is safe.
- `GET /health` returns 200 only if MongoDB answers, otherwise 503.
- Interactive docs: http://localhost:8000/docs
- Quick local MongoDB: `docker run -p 27017:27017 mongo:7`

## Endpoints implemented

| Method | Path | Notes |
|---|---|---|
| GET | `/health` | 200 / 503 |
| POST | `/employees` | 201, 409 duplicate `emp_code`, 422 invalid |
| GET | `/employees` | filter by department, paginated, sorted by `emp_code` |
| POST | `/attendance/punch-in` | R1, R2; race-safe |
| POST | `/attendance/punch-out` | R3, R4, R5; works across midnight; race-safe |
| GET | `/attendance` | filters, pagination, sorted `date` desc then `emp_code` asc |
| PATCH | `/attendance/{emp_code}/{date}` | recomputes derived fields, appends to `history` |
| GET | `/analytics/employees/{emp_code}/monthly` | aggregation |
| GET | `/analytics/departments/summary` | aggregation across both collections |
| GET | `/analytics/leaderboard/late` | `$rank`, ties share a rank |
| GET | `/analytics/departments/{department}/trend` | `$densify` + moving average |
| GET | `/admin/explain/{endpoint}` | MongoDB `explain` (executionStats) |

## How it works (short)
- **Time:** instants are stored as UTC BSON dates and exchanged as epoch milliseconds. Dates, months and shift times are IST strings. PyMongo runs with `tz_aware=True`, so there are no naive datetimes.
- **Business rules R1-R5** are pure functions at the top of `main.py`; one function (`derive_fields`) decides every stored derived value for punch-in, punch-out and PATCH.
- **Concurrency:** duplicates are prevented by unique indexes (`emp_code`; `emp_code + date`), not by check-then-insert. Punch-out uses a compare-and-set update. PATCH uses optimistic concurrency with retry, so history entries are never lost.
- **Analytics** read the stored derived values and are single aggregation pipelines. Department summary starts from `employees` so people with no logs are counted. The leaderboard ranks with `$rank` before applying `limit`. The trend uses `$densify` for gap-filling and a window function for the 7-day average.
- **Rounding:** half-up using integers or `Decimal`, never float `round()`.
- **Explain:** each explain branch calls the same query/pipeline builder as the real endpoint.

## How it works (flow charts)

### Overall request flow
```mermaid
flowchart TD
    C[Client] --> F[FastAPI route]
    F --> V{Pydantic validation}
    V -- invalid --> E422[422 response]
    V -- valid --> H[Endpoint handler]
    H --> R[Business rules R1-R5<br/>pure functions]
    H --> M[(MongoDB<br/>employees, attendance_logs)]
    R --> H
    M --> H
    H --> S[Serialise: no _id, epoch ms, defaults]
    S --> OK[JSON response]
    H -- missing resource --> E404[404]
    H -- duplicate / lost race --> E409[409]
```

### Punch-in and punch-out
```mermaid
flowchart TD
    A[POST /attendance/punch-in] --> B{Employee exists?}
    B -- no --> B1[404]
    B -- yes --> C[Truncate time to seconds<br/>compute IST attendance date R1<br/>compute late minutes R2]
    C --> D[insert_one into attendance_logs]
    D --> E{Unique index emp_code + date}
    E -- first request --> F[201 record created]
    E -- duplicate --> G[409 already punched in]

    P[POST /attendance/punch-out] --> Q{Employee exists?}
    Q -- no --> Q1[404]
    Q -- yes --> R[Find latest record with punch_in <= punched_at]
    R --> S{Found?}
    S -- no --> S1[404]
    S -- yes --> T{Already punched out?}
    T -- yes --> T1[409]
    T -- no --> U{punched_at after punch_in<br/>and within 24h?}
    U -- no --> U1[422]
    U -- yes --> V[Compute work hours R4, overtime R3, half day R5]
    V --> W[find_one_and_update where punch_out is null]
    W --> X{Matched?}
    X -- yes --> Y[200 updated record]
    X -- no, lost race --> T1
```

### Regularization (PATCH) with audit trail
```mermaid
flowchart TD
    A[PATCH /attendance/emp_code/date] --> B{Employee and record exist?}
    B -- no --> B1[404]
    B -- yes --> C[Apply requested status / punch times]
    C --> D{Rules ok?<br/>ABSENT/LEAVE clear punches<br/>presence needs punch_in<br/>punch_in on same date<br/>punch_out after, within 24h<br/>something changed}
    D -- no --> D1[422]
    D -- yes --> E[Recompute late, work hours, overtime, half day]
    E --> F[Build history entry<br/>only fields that really changed]
    F --> G[find_one_and_update with state check<br/>status, punches, history length<br/>set new values + push history]
    G --> H{Matched?}
    H -- yes --> I[200 updated record]
    H -- no, someone edited first --> J[Re-read and retry up to 8 times]
    J --> B
    J -- still conflicting --> K[409]
```

### Analytics pipelines
```mermaid
flowchart LR
    subgraph Monthly[Employee monthly]
        M1[match emp_code + month] --> M2[group: present, leave, late, overtime] --> M3[working days + pct in Python]
    end
    subgraph Summary[Department summary]
        S1[employees joined by month end] --> S2[lookup that employee's month of logs<br/>collapsed to one doc] --> S3[group by department<br/>zero-log employees still counted]
    end
    subgraph Board[Late leaderboard]
        L1[match month + late > 0] --> L2[group by employee] --> L3[lookup employee, drop unknown] --> L4[optional department filter] --> L5[setWindowFields rank<br/>1,2,2,4] --> L6[keep rank <= limit] --> L7[sort minutes desc, emp_code]
    end
    subgraph Trend[Department trend]
        T1[join dates stream] --> T3[unionWith]
        T2[daily log numbers stream] --> T3
        T3 --> T4[group by day] --> T5[densify: one row per day] --> T6[window: running headcount] --> T7[rate half-up] --> T8[window: 7-row moving average]
    end
```

### Explain endpoint
```mermaid
flowchart LR
    A[GET /admin/explain/name] --> B[Same query or pipeline builder<br/>as the real endpoint]
    B --> C[MongoDB explain, executionStats]
    C --> D[Raw plan as JSON<br/>IXSCAN visible]
```

## Indexes (created at startup)

| Collection | Index | Used for |
|---|---|---|
| employees | `emp_code` (unique) | identity, 409, sort, joins |
| employees | `department, emp_code` | filtered employee list |
| employees | `department, joined_on` | headcount / trend per department |
| employees | `joined_on` | headcount across departments |
| attendance_logs | `emp_code, date` (unique) | one record per day, per-employee queries |
| attendance_logs | `date desc, emp_code` | list order, month ranges |
| attendance_logs | `status, date desc, emp_code` | list filtered by status |
| attendance_logs | `emp_code, punch_in desc` | finding the open record at punch-out |

## Work completed
- **Part A (review):** every defect found in the starter code is listed in `REVIEW.md` with where it was, how to notice it, and the fix. All fixes are in `app/main.py`.
- **Part B (build):** every endpoint in the contract is implemented, including the three aggregation pipelines (department summary, late leaderboard, department trend), punch-out across midnight, and regularization with an audit trail.
- **Part C (prove and explain):** `GET /admin/explain/{endpoint}` covers all five endpoint names and builds each query exactly as the real endpoint does. The five design answers are in `DECISIONS.md`.
