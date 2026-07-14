# Connection Pool Exhaustion

## Symptoms
- `FATAL: remaining connection slots are reserved for non-replication superuser connections`
- `FATAL: too many connections for role "app_user"`
- Application errors: connection timeouts, `PoolTimeout` in app logs.

## Diagnosis
1. Check current connection count and limit:
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   SHOW max_connections;
   ```
2. Break down connections by state and application:
   ```sql
   SELECT application_name, state, count(*)
   FROM pg_stat_activity
   GROUP BY application_name, state
   ORDER BY count(*) DESC;
   ```
3. Look for a spike of `idle` or `idle in transaction` sessions — usually a leaking
   connection pool on the application side, not real load.

## Remediation
1. Terminate long-idle sessions that are holding slots without doing work:
   ```sql
   SELECT pg_terminate_backend(pid) FROM pg_stat_activity
   WHERE state = 'idle' AND state_change < now() - interval '10 minutes';
   ```
2. If the application pool is misconfigured, reduce its max pool size or add
   PgBouncer in transaction-pooling mode in front of Postgres.
3. Only as a last resort, raise `max_connections` — it increases memory
   pressure (each connection reserves `work_mem`-sized buffers) and does not
   fix a leaking pool.

## Risk
Terminating idle sessions is low risk. Raising `max_connections` requires a
restart and should be treated as medium risk (memory headroom check first).
