# A basin verdict is reached from the source data, never from a failure message

When a processing basin is absent from a build's compiled coverage, it is assigned exactly
one basin verdict — source defect, adapter strictness, or transfer failure — and that verdict
is reached by inspecting the acquired source data, even when the preserved traceback names the
cause unambiguously. The cheaper alternative, adjudicating the three compile failures from the
tracebacks already held off-VM, was rejected: a traceback records what our adapter refused,
which is a fact about our contract and not about NGA's data, and only the data itself
distinguishes "two polygons genuinely cover different ground under one identifier" from "the
same shape written twice". This costs roughly 85 GB of re-acquisition against an endpoint that
serves no range requests, for basins that may be discarded immediately afterwards, and it is
worth it because a source defect report goes out under a named person to a national mapping
agency and cannot be retracted.
