# pg_replayfeed — Security Model (2.0)

## 1. Privilege model

`create_feed()` and `drop_feed()` are `SECURITY DEFINER`, but they do **not**
trust the definer's privileges. Before touching any table, they check the
**invoker's** privileges explicitly:

```sql
has_table_privilege(<invoker>, <source_table>, 'TRIGGER')
```

where `<invoker>` is the outer (session) user id, so `SET ROLE` is honored.
The check names the identity that was actually evaluated in the error detail.

Concretely:

- A caller with `USAGE` on the `replayfeed` schema and `EXECUTE` on the
  functions, but without `TRIGGER` on the source table, is **denied** —
  `EXECUTE` alone grants nothing.
- `drop_feed()` additionally requires `TRIGGER` on the source table.
- Revoking `TRIGGER` after a grant is honored immediately, including under
  `SET ROLE`.

This matters because the two-argument `has_table_privilege(table, privilege)`
form evaluates under the *definer* identity inside a `SECURITY DEFINER`
function — any caller with `EXECUTE` would inherit the definer's table rights.
The three-argument form with an explicit user closes that hole.
(Verified by `test/sql/security.sql`.)

`create_feed` also sets `REPLICA IDENTITY FULL` on the source table, which
requires the invoker to own the table or hold the relevant privilege — the
same `TRIGGER` check gates this.

## 2. Replication access

Streaming uses the PostgreSQL replication protocol:

- Slot creation (`pg_create_logical_replication_slot`) and
  `START_REPLICATION` require the `REPLICATION` attribute (or superuser).
  In practice this is a dedicated replication role, separate from the
  application role.
- The output plugin reads `replayfeed.feed` via SPI **as the walsender's
  user**. The feed catalog must be visible to the replication role
  (`USAGE` on the `replayfeed` schema, `SELECT` on `replayfeed.feed`).
- The optional `feed '<name>'` startup option is a **filter**, not
  authentication: any replication client that can start the slot can request
  any feed. Tenant isolation downstream is the consumer's job — documented,
  not hidden.

## 3. What the extension does not do

- **No arbitrary SQL.** Feed registration takes a table *name*, resolved and
  validated server-side; there is no eval of caller-supplied SQL fragments.
- **No privilege escalation path.** `SECURITY DEFINER` is used only to write
  the extension's own catalog table (`replayfeed.feed`); the invoker check
  above runs first and gates everything else.
- **No search_path games.** All internal SQL is schema-qualified
  (`replayfeed.…`); `test/sql/security.sql` verifies behavior under a
  hostile `search_path`.
- **No row bodies on the wire.** Envelopes are key-only, but `key` values
  may be sensitive in your domain; treat the replication stream like the
  source table for access control.

## 4. Threats explicitly out of scope

- A superuser (or anyone with `CREATEROLE`/catalog write) can bypass
  everything — this is standard PostgreSQL; the extension does not try to
  defend against the DBA.
- The Go e2e client is an *example* consumer: it opens a replication
  connection with whatever credentials you give it. Network security
  (TLS), secret storage, and delivery-endpoint authentication are the
  operator's job.
- WAL pinning by a malicious or negligent consumer is a resource-exhaustion
  vector: a client that never acknowledges pins WAL forever. Monitor slot
  lag and drop dead slots (`docs/semantics.md` §6).
