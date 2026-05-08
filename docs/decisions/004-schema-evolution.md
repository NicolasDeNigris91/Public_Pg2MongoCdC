# ADR-004: Schema evolution via dual-write windows, gated by a stamped `schemaVersion`

- **Status:** Accepted
- **Date:** 2026-05-07
- **Context pillar:** Forward Compatibility

## Context

Every doc the sink writes carries a `schemaVersion` field
([ADR-002](./002-lsn-gated-upserts.md)). The motivation in ADR-002 is
narrowly defensive: "if the transform rules change, we want to know
which version produced the doc." But that fact alone does not give us a
process for *evolving* the schema once readers are in production. This
ADR documents the process.

The CDC pipeline has three places a "schema" can change:

1. **Source schema (Postgres).** New column added to `users`. Existing
   columns renamed or dropped. Type changed.
2. **Transform rules** (`schema/transforms/<table>.yml`). Rename
   `full_name` → `fullName`. Embed a sub-doc. Drop a field.
3. **Target schema (Mongo).** A new index. A new computed field.

Reader applications (the systems that consume the Mongo collection
the sink writes) are the constraint. They read documents with
`schemaVersion=N` and assume the shape that version N guarantees.

## Decision

**Migrations advance `schemaVersion` by exactly one and run a dual-write
window where the sink writes both the old shape and the new shape until
every reader is on the new version.**

Concretely, a migration from `schemaVersion=N` to `N+1` follows this
shape:

1. **Bump `SCHEMA_VERSION`** in the sink's deployment env (`SCHEMA_VERSION=N+1`).
2. **Update `schema/transforms/<table>.yml`** to emit the union of old
   and new fields. Both shapes coexist on the same document.
3. **Roll the sink.** New writes carry `schemaVersion=N+1` AND both
   field sets. Old docs (still `schemaVersion=N`) are not rewritten —
   the LSN gate would reject a no-op rewrite anyway, and we don't need
   one: readers asking for the new shape walk by the new fields, which
   are absent on old docs.
4. **Backfill (optional).** If readers cannot tolerate "old shape only"
   on historical docs, run a one-shot backfill job: a Mongo aggregation
   that copies the old fields into the new field names on every doc
   where `schemaVersion < N+1`. Stamps `schemaVersion=N+1` on success.
5. **Remove the old fields from the transform rules** once every reader
   has been verified to be on the new shape.
6. **Roll the sink again.** From this point forward only the new shape
   is written. Backfill is now mandatory if it was skipped in step 4.

## Why dual-write, not "stop the world and migrate"

A CDC pipeline has no maintenance window. The whole point of the
project is zero-downtime migration. A "stop writes, migrate, resume"
posture defeats it. Dual-write costs one extra field per doc during
the window — usually a few bytes — and lets readers cut over
asynchronously.

## Why advance by exactly one

Skipping versions (`N → N+2`) makes rollback ambiguous. If
something breaks and you need to roll back the sink to write `N` again,
readers that have already advanced to `N+2` see a mix of two shapes
they don't know how to reconcile. By advancing exactly one step at a
time, every rollback is "go back to the previous shape", which both
sides know.

## Verification (operational checklist)

Before marking a schema migration complete:

- [ ] `db.<coll>.distinct("schemaVersion")` returns `[N+1]` (no docs left at N).
- [ ] No reader application has `schemaVersion: N` hard-coded in its query
      filter (grep across reader codebases).
- [ ] `migration_idempotent_skip_total` did not spike during the rollout
      (would indicate the LSN gate is rejecting the no-op rewrites
      from the backfill — fine, but a noise source for the alert).
- [ ] At least one full chaos run-all has been executed against the new
      shape — proves the new fields survive crash/recovery cycles.

## What this does NOT cover

- **Backwards-incompatible source schema changes** (DROP COLUMN with
  no rename). Debezium will emit events without that column;
  the transform rules need a fallback for missing fields. Out of scope
  for this ADR — handled at the rule level per table.
- **Cross-table schema migrations** (split one table into two). Each
  table follows its own dual-write window independently.

## Alternatives considered

- **Stop-the-world cutover.** Rejected — defeats the project's stated
  zero-downtime goal.
- **Untyped Mongo docs (no `schemaVersion`).** Rejected — readers
  cannot detect which shape they are about to parse, leading to
  defensive `if-field-exists` code spreading across every consumer.
- **Schema Registry-driven migration.** Worth doing eventually
  (`docker-compose.yml` already runs `cp-schema-registry`), but the
  current `JsonConverter` setup makes Avro a bigger lift than this
  ADR is willing to mandate. See README "Limitations" — switching is a
  Connect-config flip but is intentionally deferred.
