# GRIT v0.3.0 manifest publication is authority-driven and in place

**Status:** Accepted

**Date:** 2026-08-07

## Context

The 2026-07-24 live fire and rollback remain the dated evidence for the
planetary COG reader failure. Subsequent release evidence is handled outside
this record. The reviewed authority package under
`hosting/grit-hfx-v0.3.0/` now pins the candidate manifest, the byte-identical
former manifest, and the recorded hosted-object identities needed to prepare
either action offline.

The COG observations establish Content-Length and multipart ETag only. Their
historical SHA-256 values remain labelled build records and are not live body
measurements.

## Decision

The active hosted address remains `grit/hfx-v0.3.0/`. Publication writes only
the authority package's candidate `manifest.json` to the existing
`grit/hfx-v0.3.0/manifest.json` key. It does not re-upload the planetary COGs
or attribution objects. The manifest is the only operation and therefore the
final operation.

Rollback writes only the authority package's `manifest.former.json` bytes to
that same manifest key. Publication and rollback are separate human actions,
each requiring its own explicit action flag and distinct typed confirmation.
Offline self-test and dry-run make zero AWS calls. Agents prepare and validate
the plans; a human controls every remote mutation.

This decision supersedes
`2026-07-24-grit-successor-prefix-frozen-v0-3-0.md` as an active directive.
That record's date, failure narrative, and rollback evidence remain historical
evidence under the repository's Superseded status.

## Rationale

The reviewed package gives publication and rollback fixed local byte sources
and keeps the remote operation set limited to the reader-visible manifest
switch. Retaining the failed live-fire account separately preserves the
evidence that future reader checks must falsify.

## Alternatives considered

- A successor dataset address was rejected for this declaration.
- Re-uploading byte-identical COGs or attribution objects was rejected because
  they are recorded prerequisites, not publication operations.
- Deriving either manifest from caller-supplied staging content was rejected
  because the reviewed authority package owns both byte sequences.

## Consequences

- The checked-in dry-run records one candidate-manifest operation and one
  byte-identical former-manifest rollback operation.
- Any authority-byte or recorded-prerequisite mismatch aborts before planning.
- The 2026-07-24 record is retained but no longer directs current operations.
- No new prefix, vector-contract change, or `format_version` change is part of
  this decision.
