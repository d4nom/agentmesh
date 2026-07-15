# OS Update on a Patroni Cluster

## Scope
Applies to any `patroni_cluster` object undergoing an `update_os` subtask.

## Regulation
1. Updates are always rolling, node-by-node — never patch the whole cluster
   at once.
2. Before touching the current leader, perform a Patroni switchover to a
   healthy replica so the update never requires a failover under load:
   ```bash
   patronictl switchover --candidate <replica> --force
   ```
3. Update order: replicas first, current leader last (it will have become a
   replica after the switchover above).
4. After each node comes back up, confirm it has rejoined and caught up
   before moving to the next:
   ```sql
   SELECT client_addr, state, replay_lag FROM pg_stat_replication;
   ```
5. Do not proceed to the next node while any replica shows `state` other
   than `streaming` or `replay_lag` above a few seconds.

## Downtime classification
When followed as above, `is_downtime` must be `false` — the switchover
happens before the leader is touched, so there is always a writable primary.
Only mark `is_downtime: true` if the cluster has no healthy replica to
switch over to.

## Rollback
If a node fails to rejoin after patching, do not patch further nodes.
Restore the failed node from a base backup before continuing the rollout.
