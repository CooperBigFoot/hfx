# Decision Records

Use this directory for short records of non-trivial specification and architecture decisions.

Recommended filename pattern:

- `YYYY-MM-DD-short-title.md`

Typical contents:

- decision statement
- rationale
- alternatives considered
- impact on spec, schemas, or validator behavior

## Status and supersession

Every decision record has a `**Status:**` field immediately below its title.
The supported statuses are:

- `Accepted`: the record is an active repository directive.
- `Superseded`: the record is retained as historical evidence and is not an
  active directive.

When a new record replaces an Accepted record, the new record names the old
record in its decision or consequences, and the old record changes only its
active header metadata to:

```text
**Status:** Superseded

**Superseded by:** [YYYY-MM-DD-new-record.md](YYYY-MM-DD-new-record.md)
```

Keep the old record's date, context, decision, failure narrative, rollback
evidence, and consequences unchanged. Supersession reconciles the active
status pointer; it does not rewrite dated evidence.
