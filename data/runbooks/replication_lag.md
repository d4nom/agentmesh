# Replication Lag

## Symptoms
- Read replicas returning stale data.
- Monitoring alert on `replay_lag` or `pg_last_wal_replay_lsn()` falling
  behind the primary.
- `streaming replication` connection repeatedly dropping and reconnecting.

## Diagnosis
1. On the primary, check per-replica lag:
   ```sql
   SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
          replay_lag
   FROM pg_stat_replication;
   ```
2. On the replica, check how far behind it is and whether it is actively
   replaying:
   ```sql
   SELECT now() - pg_last_xact_replay_timestamp() AS replay_delay;
   ```
3. Common causes: network saturation between primary and replica, a long
   query on the replica blocking WAL replay (`hot_standby_feedback` conflicts),
   or the replica being under-provisioned for the write rate.

## Remediation
1. If a long-running query on the replica is blocking replay, either cancel
   it or increase `max_standby_streaming_delay` if staleness is acceptable.
2. Check network throughput and latency between primary and replica.
3. If the replica cannot keep up structurally (CPU/IO bound), scale it up or
   reduce the primary's write rate temporarily.

## Risk
Read-only investigation is low risk. Cancelling a replica query is low risk;
raising `max_standby_streaming_delay` trades staleness for stability and
should be reviewed with the team owning the read path.
