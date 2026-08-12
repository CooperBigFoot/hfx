"""adjudication : (PrePassDetermination, PrePassDetermination, RefusalTargets) -> AdapterCorrectionDetermination.

Validate the landed orientation-independent endpoint geometry and adjudicate the
two outstanding non-root traversal refusals without consulting source evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


INTEGRATION_REVISION = "81b0784b067eabe4322dcf01a1b6feb1098baf52"
ENDPOINT_TOLERANCE = 0.001
PROHIBITED_EVIDENCE_DIRECTORIES = (
    "tdx-m5-seven-acquire-evidence",
    "tdx-m5-planetary-evidence",
)
ROOT_KEYS = {
    "algorithm",
    "dscontarea_derivation",
    "geometry_normalization",
    "parameters",
    "population",
    "processing_basin_id",
    "reconciliation",
    "schema_version",
    "scope",
    "source_inputs",
    "two_current_endpoint_edges",
}
PAIRING_KEYS = {
    "current_endpoint_index",
    "distance_degrees",
    "successor_endpoint_index",
    "within_endpoint_tolerance",
}
RECORD_KEYS = {
    "DSLINKNO",
    "LINKNO",
    "candidate_pairing_distances",
    "class",
    "current_endpoint_separation_degrees",
    "successor_endpoint_separation_degrees",
    "successor_is_exact_production_degenerate",
    "successor_is_near_degenerate_under_non_root_limit",
}


class AdjudicationError(RuntimeError):
    """Raised when landed evidence or a pinned adjudication invariant disagrees."""


@dataclass(frozen=True)
class Pairing:
    """One current-by-successor endpoint pairing and its stored admission flag."""

    current_endpoint_index: int
    successor_endpoint_index: int
    distance_degrees: float
    within_endpoint_tolerance: bool


@dataclass(frozen=True)
class CorrectionRequired:
    """A singleton conditioned current endpoint is positive unconsumed proof."""

    candidate_current_endpoint_indexes: tuple[int, ...]
    selected_current_endpoint_index: int
    admitted_pairings: tuple[Pairing, ...]
    discarded_pairings: tuple[Pairing, ...]


@dataclass(frozen=True)
class NoCorrectionRequired:
    """The conditioned current endpoint set is empty or contains both indexes."""

    candidate_current_endpoint_indexes: tuple[int, ...]
    selected_current_endpoint_index: None
    admitted_pairings: tuple[Pairing, ...]
    discarded_pairings: tuple[Pairing, ...]


@dataclass(frozen=True)
class RefusalTarget:
    """An outstanding traversal refusal with an established successor orientation."""

    processing_basin_id: str
    linkno: int
    dslinkno: int
    successor_downstream_endpoint_index: int
    source: str


@dataclass(frozen=True)
class BasinSpec:
    """Pinned identity, population, provenance, and source values for one basin."""

    processing_basin_id: str
    filename: str
    git_blob: str
    total_reach_count: int
    connected_edge_count: int
    class_counts: dict[str, int]
    below_tolerance_reach_count: int
    record_count: int
    retained_pairing_count: int
    admitted_pairing_count: int
    discarded_pairing_count: int
    two_by_one_count: int
    exact_zero_record_count: int
    maximum_current_separation: float
    supported_degenerate_count: int
    source_inputs: list[dict[str, Any]]
    dscontarea: dict[str, Any]
    reconciliation: dict[str, Any]
    target: RefusalTarget


ALGORITHM = {
    "comparison_revision": "d5cb9239f1228a0c709bebf23cf2edbb3444972a",
    "imports_build_adapter": False,
    "mirrored_lines": "3710-3741",
    "mirrored_revision": "1385b56bccd4758aea0d04882eee6edadcefe05b",
    "prepass_predicates_unchanged": True,
    "reused_instrument_lines": "389-436",
    "reused_instrument_path": "adapters/tdx-hydro/reconstruct_2020003440_topology.py",
    "source_path": "adapters/tdx-hydro/build_adapter.py",
}
PARAMETERS = {
    "endpoint_tolerance": ENDPOINT_TOLERANCE,
    "non_root_reach_side_ambiguity_limit": 0.003,
    "non_root_reach_side_ambiguity_tolerance_multiplier": 3.0,
}
SCOPE = {
    "compiled_basin_output_retained": False,
    "continued_past_production_refusal": False,
    "measurement_only": True,
    "orientation_derived_or_assigned": False,
    "reverse_topological_traversal_performed": False,
    "source_order_condition_3871_evaluated": False,
    "source_order_condition_3871_omission": "The 1385b56:3871 source-order condition is deliberately omitted because it compares geometry against orientation assigned earlier in the reverse-topological traversal and recovered at 1385b56:3847-3862; enumerating it would measure the enumerator's continuation policy rather than the basin.",
}
OWN_AREA_METHOD = 'abs(float(Geod(ellps="WGS84").geometry_area_perimeter(post_clamp_geometry)[0])) stored as float64'


def _source(product: str, size: int, digest: str, layer: str, basin_id: str) -> dict[str, Any]:
    return {
        "bytes": size,
        "layer_name": layer,
        "path": f"tdx-m5-seven-acquire-evidence/salvage/downloads/{basin_id}-{product}.gpkg",
        "product": product,
        "sha256": digest,
    }


SPECS = {
    "2020003440": BasinSpec(
        "2020003440", "2020003440-prepass-determination.json",
        "23bcc711cb8f048d47b93cc0c577ac9ac390d0d8", 337012, 335296,
        {"NEAR_DEGENERATE_ADMITTED": 4219, "NON_COINCIDENT": 0, "REACH_SIDE_REFUSED": 0, "SINGLE_ADMISSIBLE": 331077},
        4368, 4219, 16864, 8509, 8355, 6, 4219, 0.0023297088607064573, 104,
        [_source("basins", 5397577728, "a4fd60ff2623631906cc356fb83310a4071bc9a8bd7f3749cada97d2dac7fcba", "basins", "2020003440"),
         _source("streamnet", 1688866816, "fa2676491f525fb769eff381ad165031c76ca6a8a73ee219573b336059a2d47e", "TDX_streamnet_2020003440_01", "2020003440")],
        {"checked_polygon_bearing_link_count": 336922, "geodesic_upstream_area_sum_m2": 746759097022242.5,
         "dscontarea_sum_raw": 784573229018319.8, "selected_relative_error": 0.06596786072740056,
         "unit_decisiveness_ratio": 15926492.790983995, "signed_aggregate_relative_divergence": 0.05063765831158121,
         "absolute_aggregate_relative_divergence": 0.06596786072740056, "max_absolute_relative_divergence": 0.31386539918434125},
        {"inconsistency": None, "measured_near_degenerate_admitted_count": 4219,
         "measured_non_coincident_count": 0, "measured_reach_side_refused_count": 0,
         "prior_estimate": None, "prohibited_comparisons": [4309, 4368], "status": "measured_without_sourced_prior"},
        RefusalTarget("2020003440", 817894, 819270, 1,
                      "adapters/tdx-hydro/2020003440-topology-determination.json:measurement.refusal"),
    ),
    "2020071190": BasinSpec(
        "2020071190", "2020071190-prepass-determination.json",
        "bcf0c1cf71479dddbbc9ccd4163c3ac6f2e74eb9", 664189, 659418,
        {"NEAR_DEGENERATE_ADMITTED": 10832, "NON_COINCIDENT": 0, "REACH_SIDE_REFUSED": 0, "SINGLE_ADMISSIBLE": 648586},
        11445, 10832, 43294, 22059, 21235, 17, 10832, 0.0012570787221126288, 256,
        [_source("basins", 13388906496, "c646fd9ab70a655f038bcaa0f898e972675ab731027c48bac1e70aca18b3bf4f", "basins", "2020071190"),
         _source("streamnet", 3697729536, "9aaa47aeae3ab8a9b9e564c1d0b7cda20401cc1958a671ae75cec63ef50bdc6c", "TDX_streamnet_2020071190_01", "2020071190")],
        {"checked_polygon_bearing_link_count": 663991, "geodesic_upstream_area_sum_m2": 676932258569593.1,
         "dscontarea_sum_raw": 706270664409287.5, "selected_relative_error": 0.07046090978259306,
         "unit_decisiveness_ratio": 14807348.388910966, "signed_aggregate_relative_divergence": 0.04334023895048012,
         "absolute_aggregate_relative_divergence": 0.07046090978259306, "max_absolute_relative_divergence": 0.19756492885233165},
        {"inconsistency": "two-current-endpoint count expected 11030, measured 10832",
         "measured_near_degenerate_admitted_count": 10832, "measured_non_coincident_count": 0,
         "measured_reach_side_refused_count": 0,
         "prior_estimate": {"endpoint_separation_strictly_below_tolerance_reach_count": 11445,
                            "near_degenerate_admitted_plus_reach_side_refused_count": 11030,
                            "source": "milestone-7/step-2/plan.md:238-239, repeated at line 257"},
         "status": "corrected"},
        RefusalTarget("2020071190", 1307547, 1308923, 1,
                      "milestone-7/step-2/plan.md quotation reproduced verbatim in m7-s4 plan"),
    ),
}


def _require_equal(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AdjudicationError(f"{label} is {actual!r}; expected {expected!r}")


def validate_prepass_path(path: Path, expected_filename: str | None = None) -> Path:
    """Resolve an explicit input and reject evidence-tree paths before opening it."""
    if not path.is_absolute():
        raise AdjudicationError(f"pre-pass determination path is not absolute: {path}")
    resolved = path.resolve()
    repository_root = Path(__file__).resolve().parents[2]
    for directory_name in PROHIBITED_EVIDENCE_DIRECTORIES:
        prohibited_root = (repository_root / directory_name).resolve()
        if resolved == prohibited_root or prohibited_root in resolved.parents:
            raise AdjudicationError(
                f"pre-pass determination path is inside a prohibited evidence root: {resolved}"
            )
    if not resolved.is_file():
        raise AdjudicationError(f"pre-pass determination path is not a regular file: {resolved}")
    if expected_filename is not None and resolved.name != expected_filename:
        raise AdjudicationError(
            f"pre-pass determination filename is {resolved.name!r}; expected {expected_filename!r}"
        )
    return resolved


def _pairing_from_object(value: dict[str, Any]) -> Pairing:
    _require_equal("pairing keys", set(value), PAIRING_KEYS)
    current = value["current_endpoint_index"]
    successor = value["successor_endpoint_index"]
    distance = value["distance_degrees"]
    stored = value["within_endpoint_tolerance"]
    if type(current) is not int or current not in (0, 1):
        raise AdjudicationError(f"current endpoint index is invalid: {current!r}")
    if type(successor) is not int or successor not in (0, 1):
        raise AdjudicationError(f"successor endpoint index is invalid: {successor!r}")
    if type(distance) not in (int, float) or not math.isfinite(distance) or distance < 0:
        raise AdjudicationError(f"pairing ({current}, {successor}) distance is invalid: {distance!r}")
    if type(stored) is not bool:
        raise AdjudicationError(f"pairing ({current}, {successor}) tolerance flag is not boolean")
    expected = distance <= ENDPOINT_TOLERANCE
    if stored != expected:
        raise AdjudicationError(
            f"pairing ({current}, {successor}) distance {distance} has within_endpoint_tolerance "
            f"{str(stored).lower()}; expected {str(expected).lower()}"
        )
    return Pairing(current, successor, float(distance), stored)


def select_orientation(
    pairings: Sequence[Pairing | dict[str, Any]], successor_downstream_endpoint_index: int
) -> CorrectionRequired | NoCorrectionRequired:
    """Apply the fixed relation-only selector to one outstanding refusal."""
    if successor_downstream_endpoint_index not in (0, 1):
        raise AdjudicationError(
            f"unexpected successor downstream endpoint index {successor_downstream_endpoint_index}"
        )
    normalized = []
    for value in pairings:
        if isinstance(value, Pairing):
            value = {
                "current_endpoint_index": value.current_endpoint_index,
                "successor_endpoint_index": value.successor_endpoint_index,
                "distance_degrees": value.distance_degrees,
                "within_endpoint_tolerance": value.within_endpoint_tolerance,
            }
        normalized.append(_pairing_from_object(value))
    normalized.sort(key=lambda item: (item.current_endpoint_index, item.successor_endpoint_index))
    keys = [(item.current_endpoint_index, item.successor_endpoint_index) for item in normalized]
    if len(keys) != len(set(keys)):
        raise AdjudicationError("candidate pairings contain a duplicate endpoint pair")
    admitted = tuple(item for item in normalized if item.within_endpoint_tolerance)
    discarded = tuple(item for item in normalized if not item.within_endpoint_tolerance)
    successor_upstream_endpoint_index = 1 - successor_downstream_endpoint_index
    candidates = tuple(sorted({item.current_endpoint_index for item in admitted if item.successor_endpoint_index == successor_upstream_endpoint_index}))
    if len(candidates) == 1:
        return CorrectionRequired(candidates, candidates[0], admitted, discarded)
    if len(candidates) in (0, 2):
        return NoCorrectionRequired(candidates, None, admitted, discarded)
    raise AdjudicationError(f"candidate current endpoint indexes are invalid: {candidates!r}")


def _validate_fixed_objects(data: dict[str, Any], spec: BasinSpec) -> None:
    _require_equal(f"{spec.processing_basin_id} root keys", set(data), ROOT_KEYS)
    _require_equal(f"{spec.processing_basin_id} schema version", data["schema_version"], 1)
    _require_equal(f"{spec.processing_basin_id} processing basin ID", data["processing_basin_id"], spec.processing_basin_id)
    _require_equal(f"{spec.processing_basin_id} algorithm", data["algorithm"], ALGORITHM)
    _require_equal(f"{spec.processing_basin_id} parameters", data["parameters"], PARAMETERS)
    _require_equal(f"{spec.processing_basin_id} source inputs", data["source_inputs"], spec.source_inputs)
    _require_equal(f"{spec.processing_basin_id} reconciliation", data["reconciliation"], spec.reconciliation)
    _require_equal(f"{spec.processing_basin_id} scope", data["scope"], SCOPE)
    expected_population = {
        "class_count_sum": spec.connected_edge_count,
        "class_count_sum_equals_connected_edge_count": True,
        "class_counts": spec.class_counts,
        "connected_edge_count": spec.connected_edge_count,
        "endpoint_separation_strictly_below_tolerance_reach_count": spec.below_tolerance_reach_count,
        "total_reach_count": spec.total_reach_count,
    }
    _require_equal(f"{spec.processing_basin_id} population", data["population"], expected_population)
    geometry = data["geometry_normalization"]
    _require_equal(f"{spec.processing_basin_id} geometry keys", set(geometry), {"basins_clamp", "coordinate_domain_tolerance_degrees", "start_equals_end", "streamnet_clamp"})
    _require_equal(f"{spec.processing_basin_id} coordinate-domain tolerance", geometry["coordinate_domain_tolerance_degrees"], 0.00011111111111111112)
    for name in ("basins_clamp", "streamnet_clamp"):
        _require_equal(f"{spec.processing_basin_id} {name}", geometry[name], {"altered_native_ids": [], "altered_vertex_count": 0})
    start_equals_end = geometry["start_equals_end"]
    _require_equal(f"{spec.processing_basin_id} start-equals-end keys", set(start_equals_end), {"reach_count", "supported_two_coordinate_count", "supported_two_coordinate_native_linknos", "unsupported_more_than_two_coordinate_count", "unsupported_more_than_two_coordinate_native_linknos"})
    _require_equal(f"{spec.processing_basin_id} supported degenerate reach count", start_equals_end["reach_count"], spec.supported_degenerate_count)
    _require_equal(f"{spec.processing_basin_id} supported two-coordinate count", start_equals_end["supported_two_coordinate_count"], spec.supported_degenerate_count)
    _require_equal(f"{spec.processing_basin_id} supported ID count", len(start_equals_end["supported_two_coordinate_native_linknos"]), spec.supported_degenerate_count)
    _require_equal(f"{spec.processing_basin_id} unsupported degenerate count", start_equals_end["unsupported_more_than_two_coordinate_count"], 0)
    _require_equal(f"{spec.processing_basin_id} unsupported degenerate IDs", start_equals_end["unsupported_more_than_two_coordinate_native_linknos"], [])
    dscontarea = data["dscontarea_derivation"]
    expected_dscontarea_keys = {
        "absolute_aggregate_relative_divergence", "checked_polygon_bearing_link_count",
        "divergence_is_production_comparable", "dscontarea_sum_raw", "fabric_divergence_sanity_ceiling",
        "geodesic_upstream_area_sum_m2", "km2_relative_error", "m2_relative_error",
        "max_absolute_relative_divergence", "own_area_method", "selected_relative_error",
        "signed_aggregate_relative_divergence", "source_unit", "unit_decisiveness_min_ratio",
        "unit_decisiveness_ratio", "upstream_accumulation",
    }
    _require_equal(f"{spec.processing_basin_id} DSContArea keys", set(dscontarea), expected_dscontarea_keys)
    for key, expected in spec.dscontarea.items():
        _require_equal(f"{spec.processing_basin_id} DSContArea {key}", dscontarea[key], expected)
    _require_equal(f"{spec.processing_basin_id} own-area method", dscontarea["own_area_method"], OWN_AREA_METHOD)
    _require_equal(f"{spec.processing_basin_id} accumulation", dscontarea["upstream_accumulation"], "math.fsum in production topology order")
    _require_equal(f"{spec.processing_basin_id} source unit", dscontarea["source_unit"], "m2")
    _require_equal(f"{spec.processing_basin_id} decisiveness minimum", dscontarea["unit_decisiveness_min_ratio"], 1000.0)
    _require_equal(f"{spec.processing_basin_id} sanity ceiling", dscontarea["fabric_divergence_sanity_ceiling"], 1.0)
    _require_equal(f"{spec.processing_basin_id} production comparability", dscontarea["divergence_is_production_comparable"], True)
    _require_equal(f"{spec.processing_basin_id} m2 relative error", dscontarea["m2_relative_error"], spec.dscontarea["selected_relative_error"])


def validate_determination(data: dict[str, Any], spec: BasinSpec) -> dict[str, Any]:
    """Validate every retained record and return measurements plus the unique target."""
    _validate_fixed_objects(data, spec)
    records = data["two_current_endpoint_edges"]
    _require_equal(f"{spec.processing_basin_id} record count", len(records), spec.record_count)
    prior_identity: tuple[int, int] | None = None
    target_records: list[tuple[dict[str, Any], list[Pairing]]] = []
    retained = admitted = two_by_one = exact_zero_records = 0
    maximum_separation = 0.0
    for record in records:
        _require_equal(f"{spec.processing_basin_id} record keys", set(record), RECORD_KEYS)
        identity = (record["LINKNO"], record["DSLINKNO"])
        if type(identity[0]) is not int or type(identity[1]) is not int:
            raise AdjudicationError(f"{spec.processing_basin_id} record identity is not integral: {identity!r}")
        if prior_identity is not None and identity <= prior_identity:
            raise AdjudicationError(f"{spec.processing_basin_id} records are not in stable unique identity order at {identity!r}")
        prior_identity = identity
        _require_equal(f"{spec.processing_basin_id} record {identity} class", record["class"], "NEAR_DEGENERATE_ADMITTED")
        for field in ("current_endpoint_separation_degrees", "successor_endpoint_separation_degrees"):
            value = record[field]
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise AdjudicationError(f"{spec.processing_basin_id} record {identity} has invalid {field}: {value!r}")
        if type(record["successor_is_near_degenerate_under_non_root_limit"]) is not bool:
            raise AdjudicationError(
                f"{spec.processing_basin_id} record {identity} successor near-degenerate flag is not boolean"
            )
        if type(record["successor_is_exact_production_degenerate"]) is not bool:
            raise AdjudicationError(f"{spec.processing_basin_id} record {identity} exact-degenerate flag is not boolean")
        pairings = [_pairing_from_object(value) for value in record["candidate_pairing_distances"]]
        expected_pairs = [(0, 0), (1, 0)] if record["successor_is_exact_production_degenerate"] else [(0, 0), (0, 1), (1, 0), (1, 1)]
        actual_pairs = [(value.current_endpoint_index, value.successor_endpoint_index) for value in pairings]
        _require_equal(f"{spec.processing_basin_id} record {identity} complete cross product", actual_pairs, expected_pairs)
        if len(pairings) == 2:
            two_by_one += 1
        retained += len(pairings)
        admitted_pairings = [value for value in pairings if value.within_endpoint_tolerance]
        admitted += len(admitted_pairings)
        projection = sorted({value.current_endpoint_index for value in admitted_pairings})
        _require_equal(f"{spec.processing_basin_id} record {identity} admitted current projection", projection, [0, 1])
        if sum(value.distance_degrees == 0.0 for value in pairings) == 1:
            exact_zero_records += 1
        maximum_separation = max(maximum_separation, record["current_endpoint_separation_degrees"])
        if identity == (spec.target.linkno, spec.target.dslinkno):
            target_records.append((record, pairings))
    _require_equal(f"{spec.processing_basin_id} retained pairing count", retained, spec.retained_pairing_count)
    _require_equal(f"{spec.processing_basin_id} admitted pairing count", admitted, spec.admitted_pairing_count)
    _require_equal(f"{spec.processing_basin_id} discarded pairing count", retained - admitted, spec.discarded_pairing_count)
    _require_equal(f"{spec.processing_basin_id} two-by-one count", two_by_one, spec.two_by_one_count)
    _require_equal(f"{spec.processing_basin_id} unique exact-zero record count", exact_zero_records, spec.exact_zero_record_count)
    _require_equal(f"{spec.processing_basin_id} maximum current endpoint separation", maximum_separation, spec.maximum_current_separation)
    _require_equal(f"{spec.processing_basin_id} target record count", len(target_records), 1)
    return {"record": target_records[0][0], "pairings": target_records[0][1]}


def _pairing_artifact(pairing: Pairing) -> dict[str, Any]:
    return {
        "current_endpoint_index": pairing.current_endpoint_index,
        "successor_endpoint_index": pairing.successor_endpoint_index,
        "distance_degrees": pairing.distance_degrees,
    }


def assemble_artifact(validated: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Apply the selector to validated targets and assemble the union-shaped result."""
    basin_determinations = []
    selected_basin_ids = []
    for basin_id in sorted(SPECS):
        spec = SPECS[basin_id]
        selection = select_orientation(validated[basin_id]["pairings"], spec.target.successor_downstream_endpoint_index)
        correction_required = isinstance(selection, CorrectionRequired)
        if correction_required:
            selected_basin_ids.append(basin_id)
        basin_determinations.append({
            "processing_basin_id": basin_id,
            "outstanding_refusal": {
                "LINKNO": spec.target.linkno,
                "DSLINKNO": spec.target.dslinkno,
                "source": spec.target.source,
                "successor_downstream_endpoint_index": spec.target.successor_downstream_endpoint_index,
                "successor_upstream_endpoint_index": 1 - spec.target.successor_downstream_endpoint_index,
            },
            "admitted_pairings": [_pairing_artifact(value) for value in selection.admitted_pairings],
            "discarded_pairings": [_pairing_artifact(value) for value in selection.discarded_pairings],
            "candidate_current_endpoint_indexes": list(selection.candidate_current_endpoint_indexes),
            "outcome": "correction_required" if correction_required else "no_correction_required",
            "selected_current_endpoint_index": selection.selected_current_endpoint_index,
        })
    correction_required = bool(selected_basin_ids)
    required_correction = None
    if correction_required:
        required_correction = {
            "target_path": "adapters/tdx-hydro/build_adapter.py",
            "target_function": "_build_compact_topology",
            "target_branch": "ambiguous current reach with an already oriented non-root successor",
            "replace_source_order_lines_at_integration_ref": "3864-3889",
            "algorithm": "filter admitted matches by successor_upstream_endpoint_index, collect sorted distinct current endpoint indexes, assign the sole index, and refuse zero or two indexes",
            "distance_ordering_within_tolerance": False,
            "per_basin_special_case": False,
            "implementation_node": "new runtime node after m7-s4",
        }
    return {
        "schema_version": 1,
        "runtime_node_id": "m7-s4",
        "integration_revision": INTEGRATION_REVISION,
        "inputs": [{"processing_basin_id": basin_id,
                    "path": f"adapters/tdx-hydro/{SPECS[basin_id].filename}",
                    "git_blob": SPECS[basin_id].git_blob} for basin_id in sorted(SPECS)],
        "criterion": {
            "endpoint_tolerance": ENDPOINT_TOLERANCE,
            "admitted_pairing_predicate": "distance_degrees <= endpoint_tolerance",
            "successor_upstream_endpoint_derivation": "1 - established_successor_downstream_endpoint_index",
            "candidate_current_endpoint_derivation": "sorted distinct current_endpoint_index values among admitted pairings whose successor_endpoint_index equals the established successor upstream endpoint index",
            "correction_required_predicate": "at least one outstanding traversal refusal has exactly one candidate current endpoint and the existing adapter refuses instead of consuming it",
            "no_correction_required_predicate": "both outstanding traversal refusals have zero or two candidate current endpoints",
            "within_tolerance_distance_ordering_allowed": False,
            "discarded_pairings_allowed": False,
        },
        "population_checks": [{
            "processing_basin_id": basin_id,
            "total_reach_count": SPECS[basin_id].total_reach_count,
            "connected_edge_count": SPECS[basin_id].connected_edge_count,
            "class_counts": SPECS[basin_id].class_counts,
            "two_current_endpoint_record_count": SPECS[basin_id].record_count,
            "retained_pairing_count": SPECS[basin_id].retained_pairing_count,
            "admitted_pairing_count": SPECS[basin_id].admitted_pairing_count,
            "discarded_pairing_count": SPECS[basin_id].discarded_pairing_count,
            "unique_exact_zero_pairing_record_count": SPECS[basin_id].exact_zero_record_count,
            "unique_exact_zero_pairing_used_as_evidence": False,
            "maximum_current_endpoint_separation_degrees": SPECS[basin_id].maximum_current_separation,
        } for basin_id in sorted(SPECS)],
        "basin_determinations": basin_determinations,
        "decision": {
            "outcome": "correction_required" if correction_required else "no_correction_required",
            "selected_processing_basin_ids": selected_basin_ids,
            "required_correction": required_correction,
            "milestone_disposition": "follow_up_runtime_node_required" if correction_required else "milestone_7_closed",
        },
        "scope": {
            "decision_only": True, "adapter_behavior_changed": False, "verdicts_changed": False,
            "bands_changed": False, "guards_changed": False, "compiled_basin_output_retained": False,
            "evidence_trees_read": False,
        },
    }


def serialize_artifact(value: dict[str, Any]) -> str:
    """Serialize an adjudication deterministically with one trailing newline."""
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def adjudicate(paths: dict[str, Path]) -> dict[str, Any]:
    """Validate the two explicit landed files and return their determination.

    # Errors

    Raises `AdjudicationError` when an input path or any pinned artifact value
    disagrees with the fixed decision contract.
    """
    resolved_paths = {}
    for basin_id, spec in SPECS.items():
        resolved_paths[basin_id] = validate_prepass_path(paths[basin_id], spec.filename)
    if resolved_paths["2020003440"] == resolved_paths["2020071190"]:
        raise AdjudicationError("pre-pass determination paths must be distinct")
    validated = {}
    for basin_id, spec in SPECS.items():
        with resolved_paths[basin_id].open("r", encoding="utf-8") as source:
            data = json.load(source)
        if not isinstance(data, dict):
            raise AdjudicationError(f"{basin_id} pre-pass determination root is not an object")
        validated[basin_id] = validate_determination(data, spec)
    return assemble_artifact(validated)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--2020003440-prepass", type=Path, required=True)
    parser.add_argument("--2020071190-prepass", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the strict two-input adjudication CLI and write JSON only to stdout."""
    arguments = _parser().parse_args(argv)
    try:
        artifact = adjudicate({
            "2020003440": getattr(arguments, "2020003440_prepass"),
            "2020071190": getattr(arguments, "2020071190_prepass"),
        })
    except (AdjudicationError, json.JSONDecodeError, OSError) as error:
        print(error, file=sys.stderr)
        return 1
    sys.stdout.write(serialize_artifact(artifact))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
