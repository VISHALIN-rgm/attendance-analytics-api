"""
Employee Attendance & Analytics API (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file
  1. configuration / database
  2. time helpers and business rules R1 - R5 (pure functions)
  3. request models
  4. serialisation helpers
  5. endpoints: system, employees, attendance
  6. analytics: pipeline builders + endpoints
  7. explain endpoint
"""
import calendar
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # override=False: variables already in the environment win over .env

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration / database
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo hands back timezone-aware (UTC) datetimes instead of naive ones,
# so we never have to guess what a naive datetime means.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
WEEKDAYS_MONGO = [2, 3, 4, 5, 6]  # $dayOfWeek: 1 = Sunday ... 7 = Saturday

_indexes_ready = False


def ensure_indexes() -> None:
    """Create every index we rely on. Idempotent: re-creating an identical index is a no-op."""
    global _indexes_ready
    client.admin.command("ping")  # fail fast if MongoDB is down instead of timing out 8 times

    def make(coll, keys, **kw):
        try:
            coll.create_index(keys, **kw)
        except PyMongoError as exc:  # e.g. pre-existing duplicates block a unique index
            log.error("index %s on %s failed: %s", keys, coll.name, exc)
            if kw.pop("unique", None):  # still give the queries their index
                try:
                    coll.create_index(keys, **kw)
                except PyMongoError:
                    pass

    # employees
    make(employees, [("emp_code", ASCENDING)], unique=True)                      # identity, 409, sort
    make(employees, [("department", ASCENDING), ("emp_code", ASCENDING)])        # GET /employees?department
    make(employees, [("department", ASCENDING), ("joined_on", ASCENDING)])       # headcount / trend by dept
    make(employees, [("joined_on", ASCENDING)])                                  # headcount, all depts
    # attendance_logs
    make(logs, [("emp_code", ASCENDING), ("date", ASCENDING)], unique=True)      # 1 record/day, per-employee
    make(logs, [("date", DESCENDING), ("emp_code", ASCENDING)])                  # list order, month ranges
    make(logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)])  # status filter
    make(logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)])              # punch-out lookup
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # MongoDB not reachable yet: /health will say 503 and retry
        log.error("startup index creation failed: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time helpers and business rules
# --------------------------------------------------------------------------- #
def aware(dt: datetime) -> datetime:
    """Naive datetimes coming from MongoDB mean UTC."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> UTC datetime truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: datetime) -> int:
    dt = aware(dt)
    return calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond // 1000


def now_seconds() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_millis() -> datetime:
    """Current time at millisecond precision (what BSON can store)."""
    n = datetime.now(UTC)
    return n.replace(microsecond=(n.microsecond // 1000) * 1000)


def parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in
    belongs to the shift that started the previous day."""
    ist = aware(punch_in).astimezone(IST)
    d = ist.date()
    if is_overnight(shift_start, shift_end) and ist.time() < parse_hhmm(shift_end):
        d -= timedelta(days=1)
    return d.isoformat()


def shift_bounds(date_str: str, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """Start and end instants of the shift that belongs to attendance date `date_str` (R1)."""
    d = date.fromisoformat(date_str)
    start = datetime.combine(d, parse_hhmm(shift_start), tzinfo=IST)
    end_day = d + timedelta(days=1) if is_overnight(shift_start, shift_end) else d
    end = datetime.combine(end_day, parse_hhmm(shift_end), tzinfo=IST)
    return start, end


def compute_late_minutes(punch_in: datetime, date_str: str, emp: dict) -> int:
    """R2: late only when strictly more than 10:00 after shift start; then floor minutes since start."""
    start, _ = shift_bounds(date_str, emp["shift_start"], emp["shift_end"])
    seconds = int((aware(punch_in) - start).total_seconds())
    return seconds // 60 if seconds > 600 else 0


def compute_overtime(punch_out: datetime, date_str: str, emp: dict) -> int:
    """R3: floor minutes after shift end, counted only when at least 30."""
    _, end = shift_bounds(date_str, emp["shift_start"], emp["shift_end"])
    seconds = int((aware(punch_out) - end).total_seconds())
    minutes = seconds // 60  # floor, also for negatives
    return minutes if minutes >= 30 else 0


def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded half-up to 2 decimals, using exact decimal arithmetic."""
    seconds = int((aware(punch_out) - aware(punch_in)).total_seconds())
    return float(round_half_up(Decimal(seconds) / Decimal(3600), 2))


def derive_fields(emp: dict, date_str: str, status: str, punch_in: Optional[datetime],
                  punch_out: Optional[datetime]) -> dict:
    """All stored derived values for a record, per the table in DATA_MODEL.md."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    out["late_minutes"] = compute_late_minutes(punch_in, date_str, emp)
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(punch_out, date_str, emp)
        out["half_day"] = Decimal(str(wh)) < Decimal("4.50")  # R5: on the ROUNDED value
    return out


def month_bounds(month: str) -> tuple[str, str]:
    """'2026-07' -> ('2026-07-01', '2026-07-31'); 422 on an impossible month."""
    try:
        year, mon = int(month[:4]), int(month[5:7])
        first = date(year, mon, 1)
        last = date(year, mon, calendar.monthrange(year, mon)[1])
    except (ValueError, IndexError):
        raise HTTPException(422, "month must be YYYY-MM")
    return first.isoformat(), last.isoformat()


def count_weekdays(first: date, last: date) -> int:
    """Number of Mon-Fri days in [first, last] (R7)."""
    if last < first:
        return 0
    days = (last - first).days + 1
    full_weeks, extra = divmod(days, 7)
    count = full_weeks * 5
    for i in range(extra):
        if (first + timedelta(days=full_weeks * 7 + i)).weekday() < 5:
            count += 1
    return count


def parse_date(value: str, name: str = "date") -> str:
    """Strict YYYY-MM-DD; 422 otherwise."""
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise HTTPException(422, f"{name} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise HTTPException(422, f"{name} must be a valid YYYY-MM-DD date")


# --------------------------------------------------------------------------- #
# 3. Request models
# --------------------------------------------------------------------------- #
# Strict ints only: floats, strings and booleans are rejected; so are seconds-looking values.
EpochMs = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
MonthStr = Annotated[str, Query(pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")]
HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        if len(v) != 10 or v[4] != "-" or v[7] != "-":
            raise ValueError("joined_on must be YYYY-MM-DD")
        return date.fromisoformat(v).isoformat()

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


# `= None` defaults are not validated, so an explicit JSON null is rejected (422) while an omitted
# field means "not supplied".
class PunchInIn(BaseModel):
    emp_code: str
    punched_at: EpochMs = None  # type: ignore[assignment]
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: EpochMs = None  # type: ignore[assignment]


class RegularizeIn(BaseModel):
    status: Status = None  # type: ignore[assignment]
    punch_in: EpochMs = None  # type: ignore[assignment]
    punch_out: EpochMs = None  # type: ignore[assignment]
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# 4. Serialisation
# --------------------------------------------------------------------------- #
_INSTANT_KEYS = ("punch_in", "punch_out")


def _ms_or_none(dt: Optional[datetime]) -> Optional[int]:
    return None if dt is None else dt_to_ms(dt)


def serialize_history_entry(h: dict) -> dict:
    changes = {}
    for key, change in (h.get("changes") or {}).items():
        if key in _INSTANT_KEYS:
            changes[key] = {"from": _ms_or_none(change.get("from")), "to": _ms_or_none(change.get("to"))}
        else:
            changes[key] = {"from": change.get("from"), "to": change.get("to")}
    return {"at": dt_to_ms(h["at"]), "by": h.get("by"), "reason": h.get("reason"), "changes": changes}


def serialize_record(doc: dict) -> dict:
    """Mongo document -> API shape: no _id, epoch-ms instants, defaults for legacy gaps."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": _ms_or_none(doc.get("punch_in")),
        "punch_out": _ms_or_none(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [serialize_history_entry(h) for h in (doc.get("history") or [])],
    }


def serialize_employee(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": _ms_or_none(doc.get("created_at")),
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 5. Endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_millis()
    try:
        employees.insert_one(doc)  # the unique index on emp_code is the only duplicate guard
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict[str, Any] = {}
    if department is not None:
        q["department"] = department
    total = employees.count_documents(q)
    cursor = employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [serialize_employee(d) for d in cursor], "total": total,
            "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()
    d = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": d,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, d, emp),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        # No "find then insert": the unique (emp_code, date) index decides the race atomically.
        logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return serialize_record(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()

    rec = logs.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
                        sort=[("punch_in", DESCENDING)])
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")

    pin = aware(rec["punch_in"])
    if ts <= pin:
        raise HTTPException(422, "punched_at must be after punch_in")
    if ts - pin > timedelta(hours=24):
        raise HTTPException(422, "punched_at is more than 24 hours after punch_in")

    derived = derive_fields(emp, rec["date"], rec["status"], pin, ts)
    # Compare-and-set: only the request that still sees an open record wins.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return serialize_record(updated)


def build_attendance_query(emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str],
                           status: Optional[str], page: int, page_size: int):
    """Shared by GET /attendance and the explain endpoint so both run the same query."""
    if date_from is not None:
        date_from = parse_date(date_from, "date_from")
    if date_to is not None:
        date_to = parse_date(date_to, "date_to")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, "date_from must not be after date_to")
    q: dict[str, Any] = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    sort = [("date", DESCENDING), ("emp_code", ASCENDING)]
    return q, sort, (page - 1) * page_size, page_size


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    q, sort, skip, limit = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = logs.count_documents(q)
    docs = logs.find(q).sort(sort).skip(skip).limit(limit)
    return {"items": [serialize_record(d) for d in docs], "total": total,
            "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    date = parse_date(date)
    emp = get_employee_or_404(emp_code)

    for _ in range(8):  # optimistic concurrency: re-read and retry if someone else got in first
        rec = logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        result = _regularize_once(emp, rec, body)
        if result is not None:
            return serialize_record(result)
    raise HTTPException(409, "record is being modified concurrently, retry")


def _regularize_once(emp: dict, rec: dict, body: RegularizeIn) -> Optional[dict]:
    old_status = rec["status"]
    old_in, old_out = rec.get("punch_in"), rec.get("punch_out")
    old_in = aware(old_in) if old_in else None
    old_out = aware(old_out) if old_out else None

    new_status = body.status or old_status
    supplied_in = ms_to_dt(body.punch_in) if body.punch_in is not None else None
    supplied_out = ms_to_dt(body.punch_out) if body.punch_out is not None else None

    if new_status in ("ABSENT", "LEAVE"):
        if supplied_in is not None or supplied_out is not None:
            raise HTTPException(422, "ABSENT / LEAVE clear the punch times; do not supply them")
        new_in = new_out = None
    else:
        new_in = supplied_in or old_in
        new_out = supplied_out or old_out
        if new_in is None:
            raise HTTPException(422, "a presence status requires a punch_in")
        if supplied_in is not None and \
                attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != rec["date"]:
            raise HTTPException(422, "punch_in must stay on the record's attendance date")
        if new_out is not None and (supplied_in is not None or supplied_out is not None):
            if new_out <= new_in:
                raise HTTPException(422, "punch_out must be after punch_in")
            if new_out - new_in > timedelta(hours=24):
                raise HTTPException(422, "punch_out must be within 24 hours of punch_in")

    if new_status == old_status and new_in == old_in and new_out == old_out:
        raise HTTPException(422, "the request changes nothing")

    derived = derive_fields(emp, rec["date"], new_status, new_in, new_out)
    new_vals = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derived}
    old_vals = {
        "status": old_status, "punch_in": old_in, "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    changes = {k: {"from": old_vals[k], "to": new_vals[k]} for k in new_vals if new_vals[k] != old_vals[k]}
    entry = {"at": now_millis(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # The filter pins the exact state we computed from. If a concurrent PATCH (history length changes)
    # or punch-out (punch_out changes) got in first, nothing matches and the caller re-reads.
    flt = {
        "_id": rec["_id"],
        "status": old_status,
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(rec.get("history") or [])]},
    }
    return logs.find_one_and_update(
        flt, {"$set": new_vals, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER)


# --------------------------------------------------------------------------- #
# 6. Analytics
# --------------------------------------------------------------------------- #
# Weekday of a "YYYY-MM-DD" string. The string is parsed and read in UTC, so the weekday is that of
# the calendar date itself (no time zone can shift it).
_IS_WEEKDAY = {"$in": [{"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "onError": None}}},
                       WEEKDAYS_MONGO]}
_IS_PRESENT = {"$in": ["$status", PRESENCE]}
# Present value of one record in half-days: 2 for a full day, 1 for a half day.  Using integers keeps
# every later division exact.
_PRESENT_HALVES = {"$cond": [_IS_PRESENT, {"$cond": [{"$eq": ["$half_day", True]}, 1, 2]}, 0]}
_IS_LATE = {"$gt": ["$late_minutes", 0]}


def q2(value: Decimal) -> float:
    return float(round_half_up(value, 2))


# ---- employee monthly ------------------------------------------------------ #
def build_employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": None,
            # present_halves counts Mon-Fri presence records only (R7); full day = 2, half day = 1
            "present_halves": {"$sum": {"$cond": [_IS_WEEKDAY, _PRESENT_HALVES, 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [_IS_LATE, 1, 0]}},
            "total_late_minutes": {"$sum": {"$cond": [_IS_LATE, "$late_minutes", 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthStr):
    first, last = month_bounds(month)
    emp = get_employee_or_404(emp_code)
    agg = next(logs.aggregate(build_employee_monthly_pipeline(emp_code, month)), None) or {}

    # R7: Mon-Fri from joined_on (or the 1st) to month end
    start = max(date.fromisoformat(first), date.fromisoformat(emp["joined_on"]))
    working_days = count_weekdays(start, date.fromisoformat(last))
    present_days = Decimal(agg.get("present_halves", 0)) / 2
    pct = None
    if working_days > 0:
        pct = float(round_half_up(present_days * 100 / Decimal(working_days), 2))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(present_days),
        "leave_days": agg.get("leave_days", 0),
        "late_count": agg.get("late_count", 0),
        "total_late_minutes": agg.get("total_late_minutes", 0),
        "total_overtime_minutes": agg.get("total_overtime_minutes", 0),
        "attendance_pct": pct,
    }


# ---- department summary ---------------------------------------------------- #
def build_department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Starts from `employees` (so people with no logs are counted, R9), pulls each person's month of
    logs with an indexed $lookup that already collapses them to one small document, then rolls up
    by department."""
    first, last = month_bounds(month)
    match: dict[str, Any] = {"joined_on": {"$lte": last}}
    if department is not None:
        match["department"] = department
    has_hours = {"$and": [_IS_PRESENT, {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]}
    per_employee = [
        {"$match": {"date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": None,
            "present_halves": {"$sum": {"$cond": [_IS_WEEKDAY, _PRESENT_HALVES, 0]}},
            # decimal sum: no float drift when the mean lands exactly on a rounding boundary
            "hours_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$work_hours"}, 0]}},
            "hours_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
            "late_count": {"$sum": {"$cond": [_IS_LATE, 1, 0]}},
            "late_total": {"$sum": {"$cond": [_IS_LATE, "$late_minutes", 0]}},
            "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
        }},
    ]

    def m(field: str):
        return {"$ifNull": [f"$m.{field}", 0]}

    return [
        {"$match": match},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": per_employee, "as": "m"}},
        {"$set": {"m": {"$first": "$m"}}},  # employee with zero logs -> m is missing -> zeros via $ifNull
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_halves": {"$sum": m("present_halves")},
            "hours_sum": {"$sum": m("hours_sum")},
            "hours_n": {"$sum": m("hours_n")},
            "late_count": {"$sum": m("late_count")},
            "total_late_minutes": {"$sum": m("late_total")},
            "leave_count": {"$sum": m("leave_count")},
            "on_duty_count": {"$sum": m("on_duty_count")},
        }},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: MonthStr, department: Optional[str] = None):
    items = []
    for row in employees.aggregate(build_department_summary_pipeline(month, department)):
        n = row["hours_n"]
        avg = None
        if n:
            total = row["hours_sum"]
            total = total.to_decimal() if hasattr(total, "to_decimal") else Decimal(str(total))
            avg = q2(total / Decimal(n))
        items.append({
            "department": row["_id"],
            "headcount": row["headcount"],
            "present_days": float(Decimal(row["present_halves"]) / 2),
            "avg_work_hours": avg,
            "late_count": row["late_count"],
            "total_late_minutes": row["total_late_minutes"],
            "leave_count": row["leave_count"],
            "on_duty_count": row["on_duty_count"],
        })
    return {"month": month, "items": items}


# ---- late leaderboard ------------------------------------------------------ #
def build_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"},
                    "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},  # logs of unknown employees disappear here
    ]
    if department is not None:
        pipeline.append({"$match": {"e.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: 1, 2, 2, 4
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking, so ties at the cutoff stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$e.name",
                      "department": "$e.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipeline


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthStr, limit: int = Query(10, ge=1, le=50), department: Optional[str] = None):
    items = list(logs.aggregate(build_leaderboard_pipeline(month, limit, department)))
    return {"month": month, "items": items}


# ---- department trend ------------------------------------------------------ #
def validate_trend_range(from_: str, to: str) -> tuple[str, str]:
    from_, to = parse_date(from_, "from"), parse_date(to, "to")
    if to < from_:
        raise HTTPException(422, "to must not be before from")
    if (date.fromisoformat(to) - date.fromisoformat(from_)).days + 1 > 92:
        raise HTTPException(422, "range must not exceed 92 days")
    return from_, to


def build_trend_pipeline(department: str, from_: str, to: str) -> list:
    """Runs on `employees`. Two streams are merged and keyed by day:
         joins - how many employees of the department started on each day (before `from` -> `from`)
         logs  - presence / late numbers per day (indexed $lookup into attendance_logs)
       $densify then adds the days nobody touched, and two $setWindowFields stages turn the joins
       into a running headcount and compute the 7-row moving average.
    Rates are carried as integers in units of 1e-4 and rounded half-up with integer arithmetic, so
    there is no floating-point surprise on exact ties."""
    lo = datetime.combine(date.fromisoformat(from_), time(0), tzinfo=UTC)
    hi = datetime.combine(date.fromisoformat(to) + timedelta(days=1), time(0), tzinfo=UTC)  # exclusive

    logs_stream = [
        {"$match": {"department": department}},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": [{"$match": {"date": {"$gte": from_, "$lte": to}}},
                                  {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1,
                                                "late_minutes": 1}}],
                     "as": "l"}},
        {"$unwind": "$l"},
        {"$group": {"_id": "$l.date",
                    "present_halves": {"$sum": {"$cond": [
                        {"$in": ["$l.status", PRESENCE]},
                        {"$cond": [{"$eq": ["$l.half_day", True]}, 1, 2]},
                        0]}},
                    "late": {"$sum": {"$cond": [{"$gt": ["$l.late_minutes", 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "day": "$_id", "present_halves": 1, "late": 1}},
    ]
    return [
        {"$match": {"department": department, "joined_on": {"$lte": to}}},
        {"$group": {"_id": {"$cond": [{"$lt": ["$joined_on", from_]}, from_, "$joined_on"]},
                    "joins": {"$sum": 1}}},
        {"$project": {"_id": 0, "day": "$_id", "joins": 1}},
        {"$unionWith": {"coll": "employees", "pipeline": logs_stream}},
        {"$group": {"_id": "$day",
                    "joins": {"$sum": {"$ifNull": ["$joins", 0]}},
                    "present_halves": {"$sum": {"$ifNull": ["$present_halves", 0]}},
                    "late": {"$sum": {"$ifNull": ["$late", 0]}}}},
        {"$set": {"d": {"$dateFromString": {"dateString": "$_id"}}}},
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lo, hi]}}},
        {"$set": {"joins": {"$ifNull": ["$joins", 0]}, "present_halves": {"$ifNull": ["$present_halves", 0]},
                  "late": {"$ifNull": ["$late", 0]}}},
        {"$setWindowFields": {"sortBy": {"d": 1},
                              "output": {"headcount": {"$sum": "$joins",
                                                       "window": {"documents": ["unbounded", "current"]}}}}},
        {"$set": {"wd": {"$in": [{"$dayOfWeek": "$d"}, WEEKDAYS_MONGO]}}},
        {"$set": {
            # rate in 1e-4 units, half-up:  floor((10000 * halves + headcount) / (2 * headcount))
            "rate_u": {"$cond": [
                {"$and": ["$wd", {"$gt": ["$headcount", 0]}]},
                {"$floor": {"$divide": [{"$add": [{"$multiply": ["$present_halves", 10000]}, "$headcount"]},
                                        {"$multiply": [2, "$headcount"]}]}},
                None]},
        }},
        {"$set": {"has_rate": {"$cond": [{"$eq": ["$rate_u", None]}, 0, 1]}}},
        {"$setWindowFields": {"sortBy": {"d": 1},
                              "output": {"win_sum": {"$sum": "$rate_u", "window": {"documents": [-6, 0]}},
                                         "win_n": {"$sum": "$has_rate", "window": {"documents": [-6, 0]}}}}},
        {"$sort": {"d": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$d"}},
            "is_working_day": "$wd",
            "headcount": 1,
            "present_count": {"$divide": ["$present_halves", 2]},
            "late_count": "$late",
            "attendance_rate": {"$cond": [{"$eq": ["$rate_u", None]}, None, {"$divide": ["$rate_u", 10000]}]},
            # mean of the (rounded) rates in the window, half-up: floor((2 * sum + n) / (2 * n))
            "moving_avg_7d": {"$cond": [
                {"$eq": ["$win_n", 0]}, None,
                {"$divide": [{"$floor": {"$divide": [{"$add": [{"$multiply": [2, "$win_sum"]}, "$win_n"]},
                                                      {"$multiply": [2, "$win_n"]}]}}, 10000]}]},
        }},
    ]


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(alias="from"), to: str = Query()):
    from_, to = validate_trend_range(from_, to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = list(employees.aggregate(build_trend_pipeline(department, from_, to)))
    for it in items:
        it["headcount"] = int(it["headcount"])
        it["late_count"] = int(it["late_count"])
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. Explain
# --------------------------------------------------------------------------- #
ExplainEndpoint = Literal["attendance_list", "employee_monthly", "department_summary",
                          "late_leaderboard", "department_trend"]


def _need(value, name: str):
    if value is None:
        raise HTTPException(422, f"query parameter '{name}' is required for this endpoint")
    return value


def _json_safe(doc: Any) -> Any:
    return json.loads(json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS))


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1, le=10**9),
    page_size: int = Query(20, ge=1, le=100),
):
    # Each branch calls the very same builder the real endpoint uses.
    if endpoint == "attendance_list":
        q, sort, skip, lim = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        coll = logs.name
        command = {"find": coll, "filter": q, "sort": dict(sort), "skip": skip, "limit": lim}
    else:
        if endpoint == "employee_monthly":
            coll = logs.name
            pipeline = build_employee_monthly_pipeline(_need(emp_code, "emp_code"), _need(month, "month"))
        elif endpoint == "department_summary":
            coll = employees.name
            pipeline = build_department_summary_pipeline(_need(month, "month"), department)
        elif endpoint == "late_leaderboard":
            coll = logs.name
            pipeline = build_leaderboard_pipeline(_need(month, "month"), limit, department)
        else:  # department_trend
            coll = employees.name
            f, t = validate_trend_range(_need(from_, "from"), _need(to, "to"))
            pipeline = build_trend_pipeline(_need(department, "department"), f, t)
        command = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    try:
        result = db.command({"explain": command, "verbosity": "executionStats"})
    except PyMongoError as exc:
        raise HTTPException(503, f"explain failed: {exc}")
    return {"endpoint": endpoint, "collection": coll, "explain": _json_safe(result)}
