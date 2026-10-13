# DECISIONS.md

1. **Indexes.** `attendance_logs`: unique `(emp_code, date)` (one record per day, per-employee queries); `(date desc, emp_code)` for the list order and month ranges; `(status, date desc, emp_code)` for the status filter; `(emp_code, punch_in desc)` to find the open record at punch-out. `employees`: unique `emp_code`; `(department, emp_code)`, `(department, joined_on)` and `(joined_on)` for headcount. Rejected: an index on `late_minutes`; the leaderboard already narrows by date first, so it adds write cost for little gain.

2. **Punch-in race.** Both requests read the employee and build the same document, then call `insert_one`. The unique `(emp_code, date)` index lets exactly one insert succeed; the other raises `DuplicateKeyError` and I return 409. There is no check before the insert.

3. **Ties.** The pipeline uses `$setWindowFields` with `$rank` (1, 2, 2, 4), and only then keeps `rank <= limit`. So everyone tied at the cutoff stays in, and the response can be longer than `limit`. Rows are sorted by minutes, then `emp_code`.

4. **Headcount.** The summary starts from `employees`, not from logs. Each employee gets their month of logs through `$lookup`; no logs just means zeros, and the employee is still counted by the department `$group`. Employees who join after month end are filtered out first.

5. **At 100x.** I would stop doing the per-employee `$lookup` live and keep a small pre-aggregated daily-per-department collection, updated on punch and correction.
