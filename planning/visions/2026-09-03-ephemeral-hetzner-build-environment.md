# Vision: Ephemeral Hetzner build environment
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/104
Status: completed

Historical outcome: Landed and closed on 2026-07-21. The reproducible fsn1 lifecycle, live NGA and S3 drill, and verified teardown were delivered.

## Goal / Why

Program #103 compiles pristine NGA TDX-Hydro into one planetary HFX v0.3.0
dataset, and its standing decision is that no wrangling happens on the local
machine. Nothing currently provides the place where that wrangling can happen.
This vision delivers the ephemeral, reproducible Hetzner build environment
(fsn1): committed scripts that provision a build VM with an attached volume,
bootstrap the full adapter toolchain on it, run builds that survive SSH
disconnects, and tear everything down to zero footprint after a campaign.
Success unblocks ticket #106 (single-basin compile) and, critically, proves the
NGA acquisition path from a German datacenter early — the one risk that would
trigger the documented GEOGLOWS fallback and a revision of
`docs/decisions/2026-07-21-tdx-hydro-pristine-nga-source.md`.

The central invariant: all TDX-Hydro "hfx-ication" runs on the VM, never on the
operator's machine. A "build campaign" (see `CONTEXT.md`) is one
provision → bootstrap → wrangle → deliver → teardown lifecycle; it is the unit
of cost accounting and teardown discipline, and nothing durable survives it on
the VM because every input is re-fetchable and every output re-creatable.

## Scope — In

1. Committed shell scripts in the hfx repo (suggested home: `scripts/hetzner/`)
   covering the full campaign lifecycle via the `hcloud` CLI against the
   existing `pourpoint` Cloud project:
   - `provision`: create the build server in fsn1 with an attached volume,
     SSH key auth, and the volume mounted; server type and volume size are
     parameters with documented defaults (server default CCX33 — 8 dedicated
     vCPU, 32 GB RAM; volume default modest, e.g. 100 GB, grow-only).
   - `bootstrap`: idempotent, pushed and executed over SSH (not cloud-init),
     streaming output to the operator's terminal. Installs the adapter
     toolchain: uv/Python geo stack (GDAL-capable), Rust toolchain, anonymous
     clone of the public hfx repo, `cargo build --release -p hfx-cli`, tmux,
     and S3-capable upload tooling.
   - `teardown`: destroys server and volume by default; `--keep-volume`
     deletes only the server, pausing a campaign at volume-only cost.
   - A launch helper wrapping `ssh … tmux` so a build is started from the
     workstation in one command.
2. Disconnect-safe execution convention: every build runs in a named tmux
   session on the VM with its log tee'd to a file on the attached volume;
   monitoring is reattach-or-tail from the workstation.
3. Credential scheme: the hcloud token never leaves the workstation
   (provision/teardown run locally, token sourced from the macOS Keychain per
   the pourpoint runbook pattern); the provision flow installs only the S3 key
   pair for `pourpoint-hfx` on the VM as a mode-600 env file, sourced from the
   operator's password manager, values never committed. This is the credential
   path ticket #108 inherits.
4. A lifecycle smoke workload proving the environment end to end:
   - download one real TDX-Hydro basin GeoPackage from earth-info.nga.mil to
     the volume (validates NGA reachability/throughput from fsn1);
   - write one test object to the `pourpoint-hfx` bucket using the VM's S3
     credentials;
   - no adapter compile — that is #106.
5. A short README beside the scripts: parameters and defaults, the campaign
   lifecycle, the cost envelope per campaign (server hourly rate, volume
   per-GB-month, traffic assumptions), the teardown discipline, and the
   credential scheme (names and locations, not values).

## Scope — Out (explicit non-goals)

- The TDX-Hydro adapter itself and any basin compile (#106).
- The planetary 62-basin build campaign and its staging strategy — all-at-once
  versus download-compile-delete streaming stays undecided here (#107).
- Dataset delivery to `hfx/` in the bucket beyond the single smoke test object
  (#108).
- Terraform or any infrastructure-as-code; the committed scripts are the
  reproducibility mechanism.
- Provisioning a new Hetzner account, project, token, or bucket — the
  `pourpoint` project, `pourpoint-bootstrap` token, and `pourpoint-hfx` bucket
  (fsn1) already exist per the pourpoint-web-app runbook.
- Any change to the pourpoint-web-app VM or repository.
- Rust CLI (`pce`/`hfx-cli`) feature changes.

## Constraints

- All infrastructure operations go through the `hcloud` CLI (v1.66.0 installed
  locally) against the existing `pourpoint` project; token retrieved from the
  macOS Keychain (`security find-generic-password -s hetzner-cloud-pourpoint
  -a pourpoint-bootstrap -w`), never displayed, never on the VM, never in the
  repo.
- VM and volume in fsn1, colocated with the `pourpoint-hfx` bucket so staging
  and upload stay intra-datacenter.
- The hfx repo is public: the VM clones anonymously; no deploy keys or GitHub
  credentials on the VM.
- No secret values in the repository — scripts and README reference names,
  paths, and retrieval commands only.
- Server type and volume size are script parameters, not hard-coded: sizing is
  learn-as-we-go, refined by #106 profiling before the #107 campaign. Hetzner
  volumes grow online but never shrink.
- Bootstrap must be idempotent: re-running after a failure converges instead
  of erroring.
- Teardown-by-default discipline: a campaign ends with zero Hetzner footprint;
  `--keep-volume` is the only sanctioned deviation.
- Interactive steps only a human can do (e.g. approving a Keychain access
  dialog) must be surfaced as guided operator steps, not assumed automatable.

## Acceptance criteria (vision-level "done")

1. One full lifecycle drill has been executed for real: provision → bootstrap
   → smoke workload → teardown, driven entirely from the workstation via the
   committed scripts.
2. During the drill, one real TDX-Hydro basin GeoPackage was downloaded from
   earth-info.nga.mil onto the VM's volume, and its observed feasibility
   (success, and rough throughput) is recorded; if the NGA endpoint proved
   unreachable or impractical from fsn1, that finding is recorded instead and
   flagged as triggering the GEOGLOWS fallback decision revision.
3. During the drill, a test object was written to the `pourpoint-hfx` bucket
   from the VM using the S3 credentials installed by the provision flow.
4. `hfx` (the CLI) built from source on the VM and `uv`-managed Python geo
   tooling import successfully there, proving the toolchain bootstrap.
5. A build launched through the tmux convention survived an SSH disconnect
   (demonstrated by detaching/killing the client and reattaching).
6. After teardown, `hcloud server list` and `hcloud volume list` show no
   build resources; the bucket retains only its smoke-test object (which is
   then removed).
7. The scripts and README are committed; a cold reader could run a campaign
   from them alone.
8. Re-running provision on an existing campaign and re-running bootstrap on a
   provisioned VM are both safe (idempotent or cleanly refused with a clear
   message).

## Decomposition hints

- Slice 1 (riskiest first): NGA reachability probe from fsn1 — even a manually
  provisioned throwaway VM curling one basin GeoPackage settles the
  fallback-triggering question before any script polish.
- Slice 2: provision + teardown pair with parameters, exercised as a cycle
  (create, verify, destroy, verify-zero) before bootstrap exists.
- Slice 3: idempotent bootstrap (toolchain, repo clone, hfx-cli build) run
  twice to prove convergence.
- Slice 4: credential injection + S3 smoke write; tmux launch helper +
  disconnect test.
- Slice 5: the full drill end to end, README, cost envelope documentation.
- Keep every script runnable independently; the drill is their composition.

## Open questions / risks

- NGA endpoint behavior from a Hetzner/German network is unverified: geo-
  blocking, throttling, or per-file friction at 62-basin scale would force the
  GEOGLOWS fallback (fabric identity change, source decision revision). The
  drill is designed to surface this first.
- The S3 key pair's current storage location must be confirmed by the operator
  (password manager per pourpoint runbook); the provision flow needs a
  documented, non-interactive retrieval path analogous to the Keychain token
  command.
- Real memory/CPU demands of a TDX basin compile are unknown until #106; the
  CCX33 default may need revision for #107 — deliberately deferred.
- Left to the implementer: OS image (Debian latest stable per pourpoint
  precedent is the natural default), S3 upload tool choice, log file naming,
  and which basin serves as the smoke download.
