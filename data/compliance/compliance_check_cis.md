# CIS Baseline Compliance Check

## Scope
Applies to `run_compliance_check` subtasks with `constraints: "CIS baseline"`.

## Checklist

### Authentication
- `password_encryption` is `scram-sha-256`, not `md5`.
- No `trust` entries in `pg_hba.conf` for non-local connections.
- Superuser accounts require a strong, rotated password.

### Logging
- `log_connections` and `log_disconnections` are `on`.
- `log_min_duration_statement` is set (flags slow queries for review).
- `log_line_prefix` includes at least `%m %u %d %h`.

### Network
- `listen_addresses` does not bind to `*` unless a firewall restricts
  inbound access.
- `ssl` is `on` and `ssl_min_protocol_version` is `TLSv1.2` or higher.
- No `pg_hba.conf` entries with a `0.0.0.0/0` CIDR.

## Report format
Produce a report listing each checklist item with `pass`/`fail` and, for
failures, the exact configuration value found. Attach the report to the
request record; do not just log a summary line.

## Remediation
Failures should be scheduled as follow-up `update_role_model` or config
subtasks — this check itself only observes and reports, it does not change
configuration.
