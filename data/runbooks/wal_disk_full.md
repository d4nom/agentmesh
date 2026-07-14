# WAL Growth / Disk Full

## Symptoms
- `PANIC: could not write to file "pg_wal/..." : No space left on device`
- Disk usage on the volume holding `pg_wal` climbing steadily with no
  corresponding growth in table data.
- Postgres refuses writes or crashes.

## Diagnosis
1. Confirm which mount is full:
   ```bash
   df -h $PGDATA/pg_wal
   ```
2. WAL only grows unbounded when it cannot be recycled. The most common cause
   is a replication slot that nothing is consuming:
   ```sql
   SELECT slot_name, active, wal_status, restart_lsn
   FROM pg_replication_slots;
   ```
3. Also check `archive_command` failures — if WAL archiving is failing,
   segments pile up until they can be archived:
   ```sql
   SELECT * FROM pg_stat_archiver;
   ```

## Remediation
1. Drop replication slots that are inactive and not coming back:
   ```sql
   SELECT pg_drop_replication_slot(slot_name)
   FROM pg_replication_slots WHERE active = false;
   ```
2. Fix the underlying archive/replication consumer, then let Postgres recycle
   WAL naturally — do not manually delete files in `pg_wal`.
3. If disk is already full and Postgres is down, free space on the volume
   (rotate other logs, expand the volume) before restarting — never delete
   WAL segments by hand, it can corrupt the cluster.

## Risk
Dropping an inactive replication slot is high risk if that slot is still
needed by a consumer that will reconnect later (e.g. a paused subscriber) —
confirm it is truly abandoned first.
