# An unrecognised auxiliary costs its own entry, not the dataset

`AuxiliarySchemaId::parse` returns `AuxiliaryError::MalformedSchemaId` for any
`hfx.aux.*` name that is not currently blessed
(`crates/hfx/src/auxiliary.rs:77`), while a third-party reverse-DNS name
classifies as `ThirdParty` and is harmlessly ignored (line 81). HFX therefore
punishes a name from its own namespace harder than a name from a stranger's,
and it does so for declarations that are optional by construction. Readers
raise the error fatally — pourpoint maps it to `SessionError::AuxiliaryDeclParse`
and the dataset does not open — and `hfx-cli` reports it as a manifest field
error. The blast radius of introducing or retiring an auxiliary schema name is
the whole dataset, including its mandatory core, which no auxiliary is supposed
to be able to reach.

The worked example is a real global build, not a hypothetical. `merit-hfx-global`
was written on 2026-07-16 by adapter 0.2.0: 2,876,771 units, `format_version`
0.3.0, one `hfx.aux.snap.v2` declaration at `auxiliary[0]`, and 60
`hfx.aux.d8_raster.v1` declarations after it. `hfx.aux.d8_raster.v2` was blessed
on 2026-07-20, four days later. On 2026-08-01 `hfx-cli` reports each D8 entry as
a malformed schema id, and pourpoint 0.3.0 from PyPI refuses to open the dataset
at all with `DatasetError: auxiliary schema "hfx.aux.d8_raster.v1" is no longer
supported`. The catchment network, the graph, and a `hfx.aux.snap.v2`
declaration that is still current and still blessed are all unreachable. The
dataset was valid when it was written, nothing has touched it since, and the
adapter that produced it is not at fault — `adapters/merit-v2/build_adapter.py:700`
emits v2 today.

An unrecognised `hfx.aux.*` name therefore classifies as an unknown-but-tolerated
declaration, on the same footing as a third-party name: the dataset opens, and
the declaration is **retained** rather than dropped, so a reader that needed it
can report which schema it could not read. Retention is load-bearing, not a
convenience. Without it, tolerance is merely permissive: a renamed D8 schema
would produce a silently unrefined result, and "this dataset declares no D8
raster" would become indistinguishable from "this dataset declares one under a
schema I do not implement". Those are different facts and must stay different
answers. De-blessing keeps its meaning — a de-blessed schema is not read and
grants no capability — but it costs its own entry rather than the dataset.

Strict rejection was the rejected alternative, and its appeal is real: refusing
anything unrecognised sounds safer than tolerating it, and it is the behaviour
someone will want to restore. It buys nothing here. An optional declaration a
reader cannot use is not a threat to the artifacts a reader can use, and the
2026-07-24 hazard this discipline grew out of was a reader that accepted a
declaration and misread it — a case strictness at the namespace boundary never
touches.

The liability is that tolerance is one-way: once readers depend on unknown
auxiliaries being survivable, re-tightening breaks them. Two follow-up
obligations are created rather than settled here. The validator's severity for
an unblessed name is a separate axis from the parse classification and is not
decided by this record; what this record binds is that the classification alone
must not render a well-formed dataset unopenable. And `hfx-cli` currently exits
0 on `merit-hfx-global` while printing 29 such errors and no verdict line,
which is an independent defect noticed during this investigation and not
addressed by this decision.

Decided 2026-08-01 during discovery for pourpoint Effort ticket #100
(Program #42). Not yet implemented: `AuxiliarySchemaId::parse` still returns
`MalformedSchemaId` at the time of writing.
