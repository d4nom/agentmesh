# Certificate Renewal

## Scope
Applies to `renew_certificates` subtasks on any cluster object type.

## Order
Always renew standby nodes before the leader:
1. Renew the certificate file on each standby.
2. `reload` (not `restart`) each standby — a `reload` picks up new TLS
   material without dropping existing connections:
   ```sql
   SELECT pg_reload_conf();
   ```
3. Only once all standbys carry the new certificate, switch over so the
   current leader becomes a standby, then renew and reload it too.

## Reload vs restart
`reload` is sufficient for certificate changes (`ssl_cert_file`,
`ssl_key_file`) — a full `restart` is only required if `ssl_ca_file` or the
listen configuration itself changed. Prefer `reload` whenever possible to
avoid connection churn.

## Expiry checks
Before renewing, confirm the actual expiry window so renewal isn't wasted
effort or, worse, too late:
```bash
openssl x509 -enddate -noout -in server.crt
```
Renew any certificate expiring within 30 days.

## Rollback
Keep the previous certificate and key alongside the new ones until the
renewed chain has been verified end-to-end (client connections succeed on
every node). Only delete the old files after verification.
