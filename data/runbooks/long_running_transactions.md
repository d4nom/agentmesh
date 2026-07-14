# Long-Running / Idle-in-Transaction Sessions

## Symptoms
- `idle in transaction` sessions sitting for minutes or hours in
  `pg_stat_activity`.
- Autovacuum unable to reclaim dead tuples (vacuum horizon stuck).
- Lock waits piling up behind a transaction that never commits.

## Diagnosis
1. List long-lived transactions:
   ```sql
   SELECT pid, usename, application_name, state, xact_start, query
   FROM pg_stat_activity
   WHERE xact_start < now() - interval '5 minutes'
   ORDER BY xact_start;
   ```
2. Check what it's blocking, if anything:
   ```sql
   SELECT blocked_locks.pid AS blocked_pid, blocking_locks.pid AS blocking_pid
   FROM pg_locks blocked_locks
   JOIN pg_locks blocking_locks
     ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.pid != blocked_locks.pid
   WHERE NOT blocked_locks.granted;
   ```

## Remediation
1. Identify the owning application/team from `application_name` before
   killing anything — it may be a legitimate batch job.
2. If it's a stuck client (common cause: app opened a transaction and never
   closed it, or a debugging session left open), terminate it:
   ```sql
   SELECT pg_terminate_backend(<pid>);
   ```
3. Set `idle_in_transaction_session_timeout` at the role or database level to
   prevent recurrence.

## Risk
Terminating a session that turns out to be an in-progress batch job or
migration can cause partial writes to roll back — always inspect `query` and
`application_name` first. Setting a timeout is low risk and recommended as a
standing guard rail.
