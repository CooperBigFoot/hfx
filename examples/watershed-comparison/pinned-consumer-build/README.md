# Pinned consumer build evidence

Effort: https://github.com/CooperBigFoot/hfx/issues/108

This receipt records a local non-editable pourpoint wheel built from clean source
`7a43834a870d0f5d46ed6d23926205ee58c8fb93`. Native offline fixture tests passed:
48 passed, one skipped because the development wheel does not bundle GDAL data,
and seven network tests deselected. Logs and exact commands are retained here.
Apple shared-cache libraries are identified in linkage records; they cannot be
hashed as ordinary filesystem files. The receipt states that limitation.

Absolute paths identify the observed laptop environment. The wheel and target
outputs remain regenerable local artifacts outside Git. Reproduction must build
and hash its own wheel and record a new receipt. A matching package version does
not establish a matching build. These tests establish offline API behavior only;
they establish no Basel result, planetary memory bound, delivered object identity,
or GRIT/TDX real-world comparison.

The separate HFX worker integration test exercised a supervised isolated process
against the same exact wheel and tiny source fixture with normal default settings.
Its successful artifacts and resource trace remain in local comparison-evidence;
its command and exit result are recorded in native-worker-validation.txt.
