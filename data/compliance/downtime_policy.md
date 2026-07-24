# Downtime Policy

## When downtime is allowed
Downtime (`is_downtime: true`) is only acceptable when:
- The object has no healthy standby to switch over to, or
- The subtask itself is inherently disruptive (e.g. a major-version
  `update_dbms` that requires the cluster to be offline), and
- The request's `priority` is `high` or `critical`, or the maintenance
  window below has been explicitly scheduled.

For `low` and `medium` priority requests, prefer rescheduling over
accepting downtime.

## Maintenance windows
Standard maintenance window is 02:00–05:00 in the cluster's local time
zone. Any `is_downtime: true` subtask outside this window requires
explicit sign-off recorded on the request.

## SLA vs priority
- `critical` — SLA is a hard ceiling; if a plan's estimate exceeds it,
  the request must be re-scoped (drop non-essential subtasks) rather than
  silently run over.
- `high` — SLA overrun requires an explicit downtime_note explaining the
  delay; execution may proceed.
- `medium` / `low` — SLA is advisory; log the overrun and proceed.

## Verdict semantics
- `fits` — total estimated time is below 80% of `sla_minutes`.
- `at_risk` — estimate is at least 80% of the ceiling but does not exceed
  it; flag for monitoring during execution.
- `exceeds` — estimate is over `sla_minutes`. This must always be computed
  from the actual estimated minutes, never taken from a model's own
  self-reported verdict.
