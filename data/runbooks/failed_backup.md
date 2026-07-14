# Failed Backup / Archive Command Failure

## Symptoms
- `pg_basebackup` exits with an error mid-transfer.
- `archive_command` failing repeatedly, visible in `pg_stat_archiver` as a
  growing `failed_count` and a non-null `last_failed_wal`.
- Backup monitoring alert: no successful backup within the expected window.

## Diagnosis
1. Check archiver status:
   ```sql
   SELECT archived_count, failed_count, last_archived_wal, last_failed_wal,
          last_failed_time
   FROM pg_stat_archiver;
   ```
2. Check disk space and permissions on the backup target (local disk, S3
   mount, etc.) — the most common cause of archive failures.
3. Re-run `pg_basebackup` with verbose output to capture the exact failure:
   ```bash
   pg_basebackup -D /tmp/backup_test -Fp -Xs -P -v
   ```

## Remediation
1. If the target is full or unreachable, fix connectivity/space first, then
   let `archive_command` catch up — it retries automatically once it
   succeeds.
2. If WAL has been accumulating during the outage, verify `pg_wal` disk usage
   (see the WAL-growth runbook) before it becomes a second incident.
3. Once archiving is healthy again, take a fresh full backup — do not rely on
   a backup chain that spans the failure window until verified.

## Risk
Low risk to investigate. Do not delete unarchived WAL to free space — that
breaks point-in-time recovery for the affected window.
