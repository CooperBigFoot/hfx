"""Scan HydroBASINS layers for attribute-join contract violations."""

from __future__ import annotations

import argparse
import json
import math
import numbers
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import pyogrio


REGIONS = ("af", "ar", "as", "au", "eu", "gr", "na", "sa", "si")
LEVELS = tuple(f"{level:02d}" for level in range(1, 13))
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class InputSpec:
    """Identify one required source layer."""

    layer_type: str
    region: str | None
    level: str
    layer: str


@dataclass
class LayerData:
    """Hold independently normalized projected columns for one layer."""

    feature_count: int | None
    columns: dict[str, list[int] | None]


Resolver = Callable[[Path, str], list[Path]]


def generate_input_specs() -> tuple[list[InputSpec], list[InputSpec]]:
    """Generate all required layer identities in fixed contract order."""
    basins = [
        InputSpec(
            layer_type="basin",
            region=region,
            level=level,
            layer=f"extract/hybas_{region}/hybas_{region}_lev{level}_v1c.shp",
        )
        for region in REGIONS
        for level in LEVELS
    ]
    pours = [
        InputSpec(
            layer_type="pour-point",
            region=None,
            level=level,
            layer=f"extract/pour/hybas_pour_lev{level}_v1.shp",
        )
        for level in LEVELS
    ]
    return basins, pours


def _input_record(spec: InputSpec, feature_count: int | None) -> dict[str, object]:
    record: dict[str, object] = {"level": spec.level, "layer": spec.layer}
    if spec.region is not None:
        record = {"region": spec.region, **record}
    record["feature_count"] = feature_count
    return record


def _checks_not_evaluated() -> list[dict[str, object]]:
    return [
        {
            "name": "adjacent-level-pfaf-parent",
            "status": "not-evaluated",
            "evaluated_pairs": 0,
            "evaluated_children": 0,
            "finding_count": 0,
        },
        {
            "name": "per-level-pour-point-coverage",
            "status": "not-evaluated",
            "evaluated_levels": 0,
            "evaluated_basins": 0,
            "finding_count": 0,
        },
        {
            "name": "global-hybas-id-collision-freedom",
            "status": "not-evaluated",
            "evaluated_layers": 0,
            "evaluated_basins": 0,
            "finding_count": 0,
        },
    ]


def _report_base(
    basins: list[InputSpec], pours: list[InputSpec], feature_counts: dict[InputSpec, int | None]
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "pass",
        "inventory_complete": True,
        "regions": list(REGIONS),
        "levels": list(LEVELS),
        "inputs": {
            "basins": [_input_record(spec, feature_counts.get(spec)) for spec in basins],
            "pour_points": [_input_record(spec, feature_counts.get(spec)) for spec in pours],
        },
        "checks": _checks_not_evaluated(),
        "findings": {"input_not_staged": [], "source_contract": []},
    }


def _exact_resolver(root: Path, layer: str) -> list[Path]:
    candidate = root / layer
    return [candidate] if candidate.is_file() else []


def _relative_identity(root: Path, match: Path) -> str:
    try:
        return match.relative_to(root).as_posix()
    except ValueError:
        return Path(match.name).as_posix()


def inventory_layers(
    root: Path,
    basins: list[InputSpec],
    pours: list[InputSpec],
    *,
    resolver: Resolver = _exact_resolver,
) -> tuple[dict[str, object], dict[InputSpec, Path] | None]:
    """Inventory every generated direct path before permitting attribute reads."""
    report = _report_base(basins, pours, {})
    findings: list[dict[str, object]] = []
    resolved: dict[InputSpec, Path] = {}
    for spec in [*basins, *pours]:
        matches = resolver(root, spec.layer)
        common = {
            "layer_type": spec.layer_type,
            "region": spec.region,
            "level": spec.level,
            "layer": spec.layer,
        }
        if not matches:
            findings.append({"kind": "missing-required-layer", **common})
        elif len(matches) > 1:
            relative_matches = sorted(_relative_identity(root, match) for match in matches)
            findings.append(
                {"kind": "ambiguous-required-layer", **common, "matches": relative_matches}
            )
        else:
            resolved[spec] = matches[0]

    if findings:
        report["status"] = "input-not-staged"
        report["inventory_complete"] = False
        report["findings"]["input_not_staged"] = findings  # type: ignore[index]
        return report, None
    return report, resolved


def _attribute_finding(
    spec: InputSpec, kind: str, column: str | None, detail: str
) -> dict[str, object]:
    return {
        "check": "attribute-input",
        "kind": kind,
        "layer_type": spec.layer_type,
        "region": spec.region,
        "level": spec.level,
        "layer": spec.layer,
        "column": column,
        "detail": detail,
    }


def _as_integral(value: object) -> tuple[int | None, str | None]:
    if pd.isna(value):
        return None, "column contains a null value"
    if isinstance(value, bool):
        return None, "column contains a non-numeric value"
    if isinstance(value, numbers.Integral):
        normalized = int(value)
    elif isinstance(value, numbers.Real):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None, "column contains a non-numeric value"
        if not numeric.is_integer():
            return None, "column contains a non-integral value"
        normalized = int(numeric)
    else:
        try:
            numeric_decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None, "column contains a non-numeric value"
        if not numeric_decimal.is_finite():
            return None, "column contains a non-numeric value"
        if numeric_decimal != numeric_decimal.to_integral_value():
            return None, "column contains a non-integral value"
        normalized = int(numeric_decimal)
    if normalized < INT64_MIN or normalized > INT64_MAX:
        return None, "column contains a value outside signed int64 range"
    if normalized <= 0:
        return None, "column contains a nonpositive value"
    return normalized, None


def _normalize_column(values: Iterable[object]) -> tuple[list[int] | None, str | None]:
    normalized: list[int] = []
    for value in values:
        item, error = _as_integral(value)
        if error is not None:
            return None, error
        normalized.append(item)  # type: ignore[arg-type]
    return normalized, None


def _read_attributes(
    resolved: dict[InputSpec, Path], specs: list[InputSpec]
) -> tuple[dict[InputSpec, LayerData], list[dict[str, object]]]:
    data: dict[InputSpec, LayerData] = {}
    findings: list[dict[str, object]] = []
    for spec in specs:
        required = ["HYBAS_ID", "PFAF_ID"] if spec.layer_type == "basin" else ["HYBAS_ID"]
        try:
            frame = pyogrio.read_dataframe(
                resolved[spec], columns=required, read_geometry=False
            )
        except Exception:
            findings.append(
                _attribute_finding(
                    spec, "attribute-read-error", None, "projected attributes could not be read"
                )
            )
            data[spec] = LayerData(None, {column: None for column in required})
            continue

        columns: dict[str, list[int] | None] = {}
        for column in required:
            if column not in frame.columns:
                findings.append(
                    _attribute_finding(
                        spec, "missing-required-column", column, "required column is absent"
                    )
                )
                columns[column] = None
                continue
            values, error = _normalize_column(frame[column].tolist())
            if (
                error is None
                and column == "PFAF_ID"
                and spec.level != "01"
                and values is not None
                and any(value < 10 for value in values)
            ):
                values = None
                error = "column contains a child code without a nonempty parent prefix"
            columns[column] = values
            if error is not None:
                findings.append(
                    _attribute_finding(spec, "invalid-attribute-value", column, error)
                )
        data[spec] = LayerData(len(frame), columns)
    return data, findings


def _relevant_attribute_count(
    findings: list[dict[str, object]], check: str
) -> int:
    count = 0
    for finding in findings:
        layer_type = finding["layer_type"]
        column = finding["column"]
        if check == "parent" and layer_type == "basin":
            count += 1
        elif check == "coverage" and column in (None, "HYBAS_ID"):
            count += 1
        elif check == "collision" and layer_type == "basin" and column in (None, "HYBAS_ID"):
            count += 1
    return count


def _parent_check(
    basin_specs: list[InputSpec], data: dict[InputSpec, LayerData], blocked: int
) -> tuple[dict[str, object], list[dict[str, object]]]:
    by_key = {(spec.region, spec.level): spec for spec in basin_specs}
    findings: list[dict[str, object]] = []
    evaluated_pairs = 0
    evaluated_children = 0
    for region in REGIONS:
        for child_number in range(2, 13):
            child_level = f"{child_number:02d}"
            parent_level = f"{child_number - 1:02d}"
            child = data[by_key[(region, child_level)]]
            parent = data[by_key[(region, parent_level)]]
            child_ids = child.columns["HYBAS_ID"]
            child_pfafs = child.columns["PFAF_ID"]
            parent_pfafs = parent.columns["PFAF_ID"]
            if child_ids is None or child_pfafs is None or parent_pfafs is None:
                continue
            evaluated_pairs += 1
            parent_counts: dict[int, int] = {}
            for value in parent_pfafs:
                parent_counts[value] = parent_counts.get(value, 0) + 1
            for child_hybas_id, child_pfaf_id in zip(child_ids, child_pfafs, strict=True):
                evaluated_children += 1
                parent_pfaf_id = int(str(child_pfaf_id)[:-1])
                match_count = parent_counts.get(parent_pfaf_id, 0)
                if match_count != 1:
                    findings.append(
                        {
                            "check": "adjacent-level-pfaf-parent",
                            "kind": "unresolved-parent" if match_count == 0 else "ambiguous-parent",
                            "region": region,
                            "child_level": child_level,
                            "parent_level": parent_level,
                            "child_hybas_id": child_hybas_id,
                            "child_pfaf_id": child_pfaf_id,
                            "parent_pfaf_id": parent_pfaf_id,
                            "match_count": match_count,
                        }
                    )
    findings.sort(
        key=lambda finding: (
            REGIONS.index(str(finding["region"])),
            int(str(finding["child_level"])),
            int(finding["child_hybas_id"]),
            int(finding["child_pfaf_id"]),
        )
    )
    return {
        "name": "adjacent-level-pfaf-parent",
        "status": "fail" if blocked or findings else "pass",
        "evaluated_pairs": evaluated_pairs,
        "evaluated_children": evaluated_children,
        "finding_count": blocked + len(findings),
    }, findings


def _coverage_check(
    basin_specs: list[InputSpec],
    pour_specs: list[InputSpec],
    data: dict[InputSpec, LayerData],
    blocked: int,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    basins_by_key = {(spec.region, spec.level): spec for spec in basin_specs}
    pours_by_level = {spec.level: spec for spec in pour_specs}
    findings: list[dict[str, object]] = []
    evaluated_levels = 0
    evaluated_basins = 0
    for level in LEVELS:
        regional = [data[basins_by_key[(region, level)]].columns["HYBAS_ID"] for region in REGIONS]
        pour_ids = data[pours_by_level[level]].columns["HYBAS_ID"]
        if pour_ids is None or any(values is None for values in regional):
            continue
        evaluated_levels += 1
        covered = set(pour_ids)
        for region, values in zip(REGIONS, regional, strict=True):
            assert values is not None
            evaluated_basins += len(values)
            for hybas_id in sorted(set(values)):
                if hybas_id not in covered:
                    findings.append(
                        {
                            "check": "per-level-pour-point-coverage",
                            "kind": "uncovered-basin-id",
                            "region": region,
                            "level": level,
                            "hybas_id": hybas_id,
                        }
                    )
    findings.sort(
        key=lambda finding: (
            REGIONS.index(str(finding["region"])),
            int(str(finding["level"])),
            int(finding["hybas_id"]),
        )
    )
    return {
        "name": "per-level-pour-point-coverage",
        "status": "fail" if blocked or findings else "pass",
        "evaluated_levels": evaluated_levels,
        "evaluated_basins": evaluated_basins,
        "finding_count": blocked + len(findings),
    }, findings


def _collision_check(
    basin_specs: list[InputSpec], data: dict[InputSpec, LayerData], blocked: int
) -> tuple[dict[str, object], list[dict[str, object]]]:
    occurrences: dict[int, list[dict[str, str]]] = {}
    evaluated_layers = 0
    evaluated_basins = 0
    for spec in basin_specs:
        values = data[spec].columns["HYBAS_ID"]
        if values is None:
            continue
        evaluated_layers += 1
        evaluated_basins += len(values)
        for hybas_id in values:
            occurrences.setdefault(hybas_id, []).append(
                {"region": spec.region or "", "level": spec.level, "layer": spec.layer}
            )
    findings = [
        {
            "check": "global-hybas-id-collision-freedom",
            "kind": "duplicate-hybas-id",
            "hybas_id": hybas_id,
            "occurrences": locations,
        }
        for hybas_id, locations in sorted(occurrences.items())
        if len(locations) > 1
    ]
    for finding in findings:
        finding["occurrences"].sort(
            key=lambda occurrence: (
                REGIONS.index(occurrence["region"]), int(occurrence["level"])
            )
        )
    return {
        "name": "global-hybas-id-collision-freedom",
        "status": "fail" if blocked or findings else "pass",
        "evaluated_layers": evaluated_layers,
        "evaluated_basins": evaluated_basins,
        "finding_count": blocked + len(findings),
    }, findings


def scan(source_root: Path) -> dict[str, object]:
    """Scan a fixed HydroBASINS source tree and return its deterministic report."""
    basins, pours = generate_input_specs()
    report, resolved = inventory_layers(source_root, basins, pours)
    if resolved is None:
        return report

    data, attribute_findings = _read_attributes(resolved, [*basins, *pours])
    feature_counts = {spec: layer.feature_count for spec, layer in data.items()}
    report = _report_base(basins, pours, feature_counts)
    parent_blocked = _relevant_attribute_count(attribute_findings, "parent")
    coverage_blocked = _relevant_attribute_count(attribute_findings, "coverage")
    collision_blocked = _relevant_attribute_count(attribute_findings, "collision")
    parent, parent_findings = _parent_check(basins, data, parent_blocked)
    coverage, coverage_findings = _coverage_check(basins, pours, data, coverage_blocked)
    collision, collision_findings = _collision_check(basins, data, collision_blocked)
    source_findings = [
        *attribute_findings,
        *parent_findings,
        *coverage_findings,
        *collision_findings,
    ]
    report["checks"] = [parent, coverage, collision]
    report["findings"]["source_contract"] = source_findings  # type: ignore[index]
    if source_findings:
        report["status"] = "source-contract-failed"
    return report


def serialize_report(report: dict[str, object]) -> str:
    """Serialize a report with the stable JSON wire contract."""
    return json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Run the scanner CLI and return its process exit status."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("~/data/hydrobasins-src/").expanduser(),
    )
    args = parser.parse_args(argv)
    report = scan(args.source_root)
    sys.stdout.write(serialize_report(report))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
