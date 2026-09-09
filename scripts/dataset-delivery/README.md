# Private HFX dataset delivery

`dataset_delivery.py` promotes a preserved HFX dataset within one S3-compatible
bucket. It copies object parts in storage, verifies full destination SHA-256,
and copies `manifest.json` last. It does not compile, validate HFX semantics,
run an engine, create compute, change policies, abort uploads, or delete data.

## Native environment and tests

From the repository root:

```sh
uv sync --project scripts/dataset-delivery --frozen
uv run --project scripts/dataset-delivery python -m unittest discover -s scripts/dataset-delivery -p 'test_*.py' -v
```

The local protocol-shaped S3 test double exercises the actual composed Python
path. These tests do not establish real provider behavior. No dataset data or
credentials are needed for tests. The project keeps AWS dependencies separate
from compiler adapters and the Rust library.

## Preconditions

1. Independently review the source inventory, README and implementation.
2. Confirm exact source keys, current HEAD identities and retained full-byte
   SHA-256 evidence. `source-inventory.json` pins this selected artifact. The
   runtime refuses a changed object, even if its size still matches.
3. Choose a distinct stable dataset prefix below `hfx/`. A new journal requires
   that prefix to have no complete objects. Existing unrelated datasets and
   `hfx/README.txt` must remain unchanged.
4. Confirm private bucket and object access. This conservative implementation
   accepts only an absent bucket policy and owner-only ACLs. It does not change
   policy or permissions on existing objects. Both authenticated object access
   and explicit anonymous 403 denial must work. If a legitimate private policy
   exists, stop for reviewed changes rather than bypass the check.
5. In the default `provider-conditional` mode, prove provider-enforced conditional
   writes in a separately approved small probe. API parameter availability and
   HEAD-then-write checks are insufficient. The probe must prove
   `IfNoneMatch="*"` on both `PutObject` and `CompleteMultipartUpload`. A failed
   probe remains a failure. The separate, explicitly approved `exclusive-writer`
   mode below records a weaker operational guarantee. It is never an automatic
   fallback. Retain probe objects and upload IDs for later exact cleanup approval.

Hetzner's supported-actions documentation warns storage copy can fail due to
internal factors even in the same location. The tool propagates that failure.
There is no automatic full download/reupload or new compute fallback.

## Conditional-write evidence

Supply the actual reviewed probe record using `--conditional-writes-evidence`.
The tool validates its recorded outcomes and pins its canonical SHA in the
journal. Each invocation requires a probe from the last 24 hours. If a long
interruption requires a fresh probe, the journal retains both capability-record
hashes in its history; source, destination and artifact plan stay unchanged. This evidence is an operator-retained observation, not a provider
capability certificate or a guarantee against a later service behavior change.
Never create a passing record without executing and reviewing the probe.

Run the reviewed probe from the repository root only after explicit operational
authorization. It creates a random fresh prefix below
`scratch/dataset-delivery-probes/`. The directory below must be a new private
0700 directory, separate from the delivery journal:

```sh
uv run --project scripts/dataset-delivery python scripts/dataset-delivery/conditional_writes_probe.py \
  --source-inventory hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/source-inventory.json \
  --profile hetzner-pourpoint \
  --evidence-dir /absolute/private/conditional-write-probe \
  --execute
```

The generated `/absolute/private/conditional-write-probe/delivery.json` is the
conditional-write evidence input. The command name expresses its limited
purpose; the local filename is the shared atomic journal filename. Without
`--execute`, the command refuses before creating a client or making requests.
It never reuses an existing journal. Runtime defaults to 300 seconds; use
`--max-seconds` to choose a shorter positive bound. Each request has a 15-second
inactivity timeout, and processed probe reads are bounded to 4 KiB.

The receipt has schema `hfx-conditional-writes-probe-v1`, endpoint, region,
bucket, observed time, status, and two operation summaries (`PutObject` and
`CompleteMultipartUpload`). Each summary binds the exact key, condition,
original and attempted replacement SHA, initial HTTP 200, replacement HTTP 412,
original HEAD identity, and unchanged full readback SHA. The event list retains
sanitized actual API responses/statuses, source-free tiny readback hashes and
anonymous access observations. Authorization and arbitrary response headers or
provider exception text are excluded. The upload list retains both completed
and rejected-completion upload IDs. Probe bytes are fixed nonsensitive test
strings. Delivery checks the summaries against the actual write and readback
events; a hand-entered capability flag cannot satisfy the contract.

Use distinct keys. For each operation, first write a tiny known payload with
`IfNoneMatch="*"`, then attempt a different payload at the same key using the
same condition. Require HTTP 412 and verify the original full bytes remain.
For multipart completion, create two different upload IDs and use the condition
on each completion, rather than testing only upload initiation. Keep complete
sanitized response/status records and unfinished upload IDs alongside the
summary. A failed or ignored condition blocks promotion. No blind write retries
or retries without the condition are allowed. The implementation configures one
SDK attempt for every request and persists uncertain effects for inspection.

## Plan and execution commands

The example evidence path is a private durable operator directory outside build
outputs. Create it with mode 0700. It must not be a symlink. Do not place it in
`target`, `.venv`, `build`, or `dist`. Keep it after the process exits.

From the repository root, replace `/absolute/private/delivery-records` with the
approved evidence directory:

```sh
uv run --project scripts/dataset-delivery python scripts/dataset-delivery/dataset_delivery.py plan \
  --source-inventory hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/source-inventory.json \
  --destination-prefix hfx/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774 \
  --readme hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/README.md \
  --evidence-dir /absolute/private/delivery-records \
  --profile hetzner-pourpoint --max-seconds 3600 --max-read-bytes 1048576
```

`plan` performs read-only provider calls, including a full small-manifest read,
and writes the local plan journal. It neither reserves nor creates a bucket key.
Another writer could still appear; conditional completion prevents overwrite.

After root review, provider capability proof, and the operational go decision,
use the same arguments with action `deliver`, plus:

```text
--conditional-writes-evidence /absolute/private/conditional-write-probe.json
--max-seconds 86400
--max-read-bytes 142670000000
--request-timeout-seconds 30
```

The byte limit permits the six-object payload and small manifest/README reads.
The tool refuses a range before GET when its declared length exceeds the
remaining budget. The counter measures processed payload bytes; a malformed
response can overshoot by one bounded buffer, and SDK/TLS transport buffering
prevents a strict network-byte or billing cap. The limit does not authorize
cloud egress spending by itself. Confirm remaining
provider transfer quota/cost before a full run. Raise limits only through an
explicit operator decision. Exceeding a limit stops the invocation.

## Explicit exclusive-writer publication

`--publication-protection provider-conditional` remains the default. Its probe
validation and rejection of failed capability evidence remain unchanged.
`--publication-protection exclusive-writer` is a separate domain choice that
requires the operator's exact decision, a separate immutable authorization
binding, and the original failed probe. It does not reinterpret that probe as
successful. The failed record must demonstrate working conditional `PutObject`
creation/replacement rejection and ignored conditional multipart completion.

For the selected TDX-Hydro dataset, Nicolas Lazaro approved this weaker
single-writer approach on 2026-09-09 after the limitation was explained. The
original decision and its separate binding are retained byte-for-byte in the
repository's dataset hosting directory. The accepted vision is unchanged.
The binding pins both raw-file SHA-256 values and exactly the endpoint, region,
bucket, source prefix and destination prefix. Supplying a decision file is an
operator action: these records bind reviewed authority, rather than deriving
human permission from a JSON flag.

Add these arguments to the `deliver` command above only after the new method
has independent review and an operational go decision:

```text
--publication-protection exclusive-writer
--exclusive-writer-decision hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/exclusive-writer-decision.json
--exclusive-writer-authorization-binding hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/exclusive-writer-authorization-binding.json
--conditional-writes-evidence /absolute/private/original-failed-probe/delivery.json
```

The original failed probe must match raw SHA-256
`100047cf7498bc52afc897ee54200a323c2cb2f91ef36411633e6cd51aa929be`.
The root operator retains it unchanged. A normalized or edited copy fails the
binding. Both operator record files must also retain their exact original bytes.
Use only `--publication-protection exclusive-writer` with `plan`; decision inputs
belong to `deliver`. Planning remains read-only and does not acquire ownership.

Before the first dataset write, the tool requires the destination to have no
complete objects or multipart uploads. It conditionally creates a private
`owner.json` under `scratch/dataset-publication-ownership/<destination-hash>/`.
The hash binds endpoint, region, bucket and destination prefix. It excludes the
source prefix so alternate source plans cannot reserve the same destination
under another key. The ownership payload binds the source scope, immutable plan,
decision/binding/probe hashes and a unique operation token saved in the journal.
The exact final dataset inventory remains the six originals plus README.

The reservation uses the working `PutObject IfNoneMatch="*"` condition. It
coordinates cooperating invocations with distinct journals. The existing local
journal file lock prevents concurrent use of the same local journal. A foreign
reservation is refused. There is no automatic takeover, lease expiry, token
replacement, deletion, abort or cleanup. Do not copy an active journal to another
writer: the token identifies the recorded operation and is not a distributed
process lock. The operator must exclude all other writers to the selected
prefix, including external clients and replicated journals.

Reservation bytes and HEAD identity are checked through the existing bounded
full-SHA reader before each initiation, completion and README write, and before
acceptance. Source identities and the exact destination inventory are rechecked.
Active destination uploads must exactly equal the key/upload-ID pairs recorded
as copying in this journal. Foreign, missing or unexpected uploads refuse fresh
acquisition or resume. Existing completed payloads need their recorded identities
and full-SHA receipts. Unknown or uncertain reservation, part, completion or
README effects stop for inspection; they are never blindly repeated.

Immediately before each completion/write, the destination key must still be
absent. Source ETag/version guards and conditional README PUT remain enabled.
The completion request still carries `IfNoneMatch="*"` as best effort, with no
claim that the provider enforces it. A writer can appear between HEAD and
multipart completion. This TOCTOU race remains; the user-approved operational
single-writer exclusion is the safeguard. The reservation does not fence an
external writer or make multipart completion atomic.

The current receipt adds `publication_protection` with mode, the explicit weaker
guarantee, raw decision/probe/binding hashes, scope, operation token and reservation
key/identity/full-SHA evidence. Mode and authorization hashes become immutable
once recorded. Historical failed probe status remains visible in capability
history. Consumers still require current `status: verified-dataset` and all
existing complete payload checks; a refused or checking receipt cannot borrow
historical success. Retain the complete receipt with comparison outputs so its
protection disclosure survives normalization of dataset identity.

The ownership reservation and any incomplete artifacts stay in storage for the
later exact cleanup inventory and human approval. Existing HydroBASINS, sources,
probe objects and unrelated data remain untouched by cleanup operations because
this tool has no deletion command.

## Integrity, progress and interrupted execution

Source inventory and HEAD identities are checked on every invocation. Source
manifest bytes are fully verified before any destination mutation. Each
`UploadPartCopy` pins the source ETag. Multiple-part copies specify each exact
byte range; a one-part copy omits `CopySourceRange` and copies the whole source.
This includes the small attribution and manifest objects, for which S3 does not
permit `CopySourceRange`. A version ID is used when present. New objects use explicit private ACL. Multipart completion and
README PUT use destination `IfNoneMatch="*"` with no automatic fallback.
In exclusive-writer mode the multipart condition is documented best effort;
its lack of enforcement remains explicit in the protection receipt.

Multipart parts are at most 256 MiB by default; only request/response metadata
passes through the laptop. Upload IDs, exact part sizes and ETags are persisted
before moving to the next part. ETags provide conditional identity, not SHA
proof. The journal is local-writer locked and atomically replaced with fsync.
After an uncertain create, part, completion or PUT, the tool refuses automatic
repetition. Inspect preserved records and provider state; do not edit the journal
to manufacture success. Known completed parts can resume only when ListParts
exactly equals the recorded parts. No unknown upload ID is adopted.

### Explicit pending-part reconciliation

After independent review of an interrupted copy, `reconcile-part` can record one
observed pending part in the same journal-owned multipart upload. Ordinary
`deliver` still refuses pending intent. This command supports only the explicitly
approved `exclusive-writer` publication mode and requires all existing delivery
arguments, including the original probe, decision and authorization binding.

Stop all writers first, including any process with a replicated journal. Inspect
the raw journal and read-only provider ListParts response. Pin the raw journal
SHA-256 and the selected pending part ETag. Then replace action `deliver` with
`reconcile-part` and add:

```text
--object-path catchments.parquet
--expected-journal-sha256 <SHA-256-of-exact-reviewed-delivery.json-bytes>
--expected-part-etag '"<exact-reviewed-part-ETag>"'
--confirm-exclusive-writer
```

Use a small positive read budget sufficient for the source manifest and two
ownership-reservation checks. No dataset payload is read by this command.
The confirmation records the operator's current exclusion of all other writers;
it does not establish a provider fence or observe another client's liveness.

The command rechecks the exact plan, source inventory and HEAD identities,
private access, immutable authorization, reservation bytes and identity, completed
destination identities, and active upload ownership. Exactly one object may be
copying. It must have one pending intent for the next source range. All preceding
part numbers, sizes and ETags must match the journal exactly. Two complete
ListParts observations must agree and contain exactly those preceding parts plus
one pending part with the expected number, size and explicitly pinned ETag.
Missing, extra, foreign, changed or ambiguous state refuses. An uncertain create,
completion, reservation or README PUT remains outside this recovery path.

All provider calls are read-only with one SDK attempt. The command does not
copy, retry a write, create, complete, abort or delete anything. On success, one
atomic fsynced journal replacement stores the exact original journal text and its
SHA-256, original pending intent, observed parts, reservation verification,
operator confirmation and observation time in `part_recoveries`. The selected
part is recorded and its pending intent removed. The status is
`partial-unverified`. A refusal leaves the journal bytes unchanged, with its
reason reported on stderr. Retain that output separately for an audit.

The evidence explicitly states that the original copy request's success was
**not observed**. ListParts is an ownership/size/token observation. Its ETag is
not SHA-256 evidence. The original interruption remains preserved. A later,
separate `deliver` invocation rechecks all normal safeguards, resumes remaining
parts, and must verify the full completed object SHA-256 before acceptance or
manifest activation. The command does not authorize that subsequent invocation.
External-writer races and the operational exclusive-writer guarantee remain
unchanged.

After a completed object has a recorded HEAD identity, the tool reads its entire
payload as ordered 64 MiB HTTP ranges, through a 1 MiB buffer, into a sequential
SHA-256 calculation. Every GET pins `IfMatch` and version where present. Each
response must be 206 with exact `Content-Range`, length and ETag. Byte count,
full SHA, and final HEAD must match. A truncated or malformed range fails.
The tool never calls a range digest a whole-object checksum. Provider checksum
metadata is not used as a substitute; observed selected-object HEAD responses
contained no full-object SHA.

Verification restarts at the object boundary after interruption, without a full
local artifact. Finished verification resumes only with the same pinned plan,
HEAD identity, expected full SHA and byte count. The small README is reverified
each invocation. No hash state serialization or concatenation of range hashes
is attempted. There are no automatic read retries. Stream interruption preserves
the copied state so an operator can rerun unchanged arguments after inspection.
A same-size replacement with a different identity is refused.

Structured JSON progress emits copied-part and verified-range events. Both
commands run in the POSIX main thread. After the durable attempt checkpoint,
a real-time `ITIMER_REAL` alarm enforces the remaining total runtime across
client initialization, blocked SDK requests and stream reads. SIGINT and SIGTERM
raise an immediate dedicated interruption through those calls. The interruption
inherits `BaseException` so SDK exception retries cannot defer it. Previous
handlers are restored and the alarm is disabled before failure evidence is
saved. The tool refuses to replace an existing active real-time timer.

An SDK read timeout measures inactivity. The separate invocation alarm stops
continuous trickles or multipart keepalive responses even when that timeout
never fires. Localhost protocol tests cover slow bodies and delayed headers,
actual deadline expiration and actual SIGTERM for both CLI paths. These tests
establish local interruption behavior, independently of provider capability.
The tool preserves uncertain write intent and never automatically retries after
interruption. Durable checkpoint/failure-file writes happen outside the alarm
scope so an interrupted journal replacement cannot restore an accepted status.
The runtime bound controls storage work, rather than disk fsync completion time.

Runtime and byte limits are invocation-wide; they are not a system RAM or billing
cap. The process runs one copy or verification operation at a time. Source bytes
remain in storage throughout.

The five non-manifest originals and README must be verified before copying the
manifest. This controls dataset activation, not secrecy. Unverified payload
objects are still visible to authorized credentials. Before a new attempt,
a prior successful receipt is copied into `verification_history` and the current
status becomes `checking`, durably, before any preflight or deadline alarm.
The delivery CLI first identifies and locks a readable journal, then places
remaining argument conversion, source/config reads and runtime-bound validation
inside that attempt boundary. Invalid prerequisites therefore cannot leave a
previously accepted current receipt. Normal argparse syntax errors retain their
exit behavior while recording a sanitized failure; help remains informational
and never changes a receipt. An unidentifiable or unreadable journal cannot be
safely rewritten.
A failed attempt, including a contradicted preflight observation, sets the
current status to `refused` with a sanitized `last_failure`. Earlier failures
move to `failure_history` on a new attempt. The object and upload state stays
available for inspection and safe resume. An abrupt process loss can leave
`checking` or `partial-unverified`; neither is accepted by consumers.

If final manifest copy or readback fails, no current accepted delivery claim
is made. Historical success evidence is preserved but cannot override a current
refusal. The final `verified-dataset` receipt follows all six full SHA checks,
README verification, exact final inventory and repeated source/access checks.
It records the actual total bytes including README. It does not claim successful
consumer delineation, comparison, historical validator stdout, or cleanup.

## Retention and security

Keep `delivery.json`, original source inventory, conditional-write evidence,
source campaign records and the exact README in durable private locations. The
journal contains object identities and upload IDs, not credential values. Do not
use shell tracing, AWS debug output or credential dumps. No credential appears
in command arguments. The public GRIT consumer process must not inherit private
credentials.

There is no deletion command. Preserve source, incomplete destination and known
uploads after failure. Later cleanup requires successful comparison, exact
current inventories, protected retained keys, Nicolas Lazaro's explicit deletion
approval, identity rechecks, per-object results and post-cleanup verification.
Unrelated HydroBASINS and unknown-purpose objects are outside deletion authority.
