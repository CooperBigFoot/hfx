# identity : (HcloudDescription, kind) -> Identity | error
#
# Projects one `hcloud <kind> describe -o json` document onto the exact
# identity record the campaign runbook stores and later matches at teardown.
# A projected field that is absent, null, or of the wrong type raises an
# error, so `jq -f` exits 5 and a strict-mode operator shell stops.
#
# The field paths below are the ones hcloud v1.66.0 emits. A server's
# location is `.location.name`; the `.datacenter` key that hcloud still emits
# is null, so a projection through `.datacenter.location.name` reads null.
# On 2026-09-04 that null consumed a whole approved lifecycle at the
# post-provision gate. This file is the single source for both the read-only
# preflight (proved against a standing server before provisioning) and the
# post-provision gate, so the two cannot diverge.
#
# Usage: jq --arg kind server|volume|server-type|location -f hcloud-identity.jq

# Walks a key path without raising, so the only error is the typed refusal.
def typed($path; $expected):
  (reduce $path[] as $key (.; if type == "object" then .[$key] else null end)) as $value
  | if ($value | type) == $expected then $value
    else error("hcloud \($kind) field \($path | join(".")) is \($value | type), expected \($expected)")
    end;

def server_identity:
  { id: typed(["id"]; "number"),
    name: typed(["name"]; "string"),
    server_type: typed(["server_type", "name"]; "string"),
    location: typed(["location", "name"]; "string"),
    volumes: typed(["volumes"]; "array") };

def volume_identity:
  { id: typed(["id"]; "number"),
    name: typed(["name"]; "string"),
    size: typed(["size"]; "number"),
    location: typed(["location", "name"]; "string"),
    server: typed(["server"]; "number") };

def server_type_identity:
  { name: typed(["name"]; "string"),
    cores: typed(["cores"]; "number"),
    cpu_type: typed(["cpu_type"]; "string"),
    architecture: typed(["architecture"]; "string"),
    locations: [typed(["locations"]; "array")[] | typed(["name"]; "string")] };

def location_identity:
  { name: typed(["name"]; "string"),
    network_zone: typed(["network_zone"]; "string") };

if type != "object" then error("hcloud \($kind) description is \(type), expected object")
elif $kind == "server" then server_identity
elif $kind == "volume" then volume_identity
elif $kind == "server-type" then server_type_identity
elif $kind == "location" then location_identity
else error("unknown hcloud identity kind \($kind)")
end
