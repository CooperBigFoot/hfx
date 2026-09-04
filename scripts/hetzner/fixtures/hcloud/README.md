# Recorded hcloud CLI output

These files are the JSON the installed Hetzner Cloud CLI emitted on 2026-09-04
for read-only describes in the `pourpoint` project. They are the fixtures for
`hcloud-identity.jq` and `verify-campaign-inputs.sh --check hcloud-json-shape`.

| File | Command | Provenance |
| --- | --- | --- |
| `server-describe-pourpoint-web-1.json` | `hcloud server describe pourpoint-web-1 -o json` | hcloud v1.66.0, 2026-09-04; server id, primary IP ids, IPv4 and IPv6 addresses, PTR name, and firewall id replaced by placeholders |
| `server-type-describe-ccx33.json` | `hcloud server-type describe ccx33 -o json` | hcloud v1.66.0, 2026-09-04, verbatim |
| `location-describe-fsn1.json` | `hcloud location describe fsn1 -o json` | hcloud v1.66.0, 2026-09-04, verbatim |
| `server-describe-legacy-datacenter.json` | none | derived from the first file by moving `location` under `datacenter`, the shape the runbook assumed before 2026-09-04 |

The server description carries `"datacenter": null` and the location at
`.location.name`. The legacy fixture exists so the tests prove that the
projection refuses the `.datacenter.location.name` shape instead of reading
null. None of these files contains a token, key, or credential; recapture them
with the same commands and redactions when the CLI is upgraded.
