# Table Bloat / Autovacuum Falling Behind

## Symptoms
- Query planner picking bad plans despite up-to-date indexes.
- `n_dead_tup` climbing steadily on hot tables.
- Autovacuum workers running constantly but never catching up, or disk usage
  growing faster than logical row count would suggest.

## Diagnosis
1. Find the worst offenders:
   ```sql
   SELECT relname, n_live_tup, n_dead_tup, last_autovacuum
   FROM pg_stat_user_tables
   ORDER BY n_dead_tup DESC
   LIMIT 10;
   ```
2. Check for long-running transactions holding back the vacuum horizon —
   autovacuum cannot reclaim rows still visible to an old snapshot:
   ```sql
   SELECT pid, xact_start, state, query
   FROM pg_stat_activity
   WHERE xact_start < now() - interval '10 minutes';
   ```
3. Check autovacuum settings aren't too conservative for the table's write
   rate (`autovacuum_vacuum_scale_factor`, `autovacuum_vacuum_cost_limit`).

## Remediation
1. If a long transaction is holding the horizon back, resolve it first (see
   the long-running-transactions runbook) — vacuuming won't help until it's
   gone.
2. Run a manual vacuum on the worst-affected table during a low-traffic
   window:
   ```sql
   VACUUM (ANALYZE, VERBOSE) <table_name>;
   ```
3. For tables with very high churn, set a more aggressive per-table
   autovacuum threshold instead of a one-off manual vacuum:
   ```sql
   ALTER TABLE <table_name> SET (autovacuum_vacuum_scale_factor = 0.05);
   ```

## Risk
Manual `VACUUM` is low risk (it does not take an exclusive lock) but consumes
I/O — avoid during peak traffic. `VACUUM FULL` rewrites the table and takes an
exclusive lock; only use it with an explicit maintenance window.
