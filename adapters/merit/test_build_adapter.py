"""Regression tests for the MERIT HFX adapter."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pyarrow.ipc as pa_ipc

from build_adapter import stage_7_write_graph


class Stage7WriteGraphTests(unittest.TestCase):
    def _read_graph(self, out_dir: Path) -> dict[int, list[int]]:
        with pa_ipc.open_file(out_dir / "graph.arrow") as reader:
            table = reader.read_all()
        return {
            int(row["id"]): [int(v) for v in row["upstream_ids"]]
            for row in table.to_pylist()
        }

    def test_treats_self_loops_as_terminal(self) -> None:
        rivers = gpd.GeoDataFrame(
            {
                "COMID": [1, 2, 3, 4],
                "NextDownID": [0, 2, 1, 99],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            stage_7_write_graph([1, 2, 3, 4], rivers, out_dir)

            self.assertEqual(
                self._read_graph(out_dir),
                {
                    1: [3],
                    2: [],
                    3: [],
                    4: [],
                },
            )

    def test_still_rejects_real_cycles(self) -> None:
        rivers = gpd.GeoDataFrame(
            {
                "COMID": [1, 2],
                "NextDownID": [2, 1],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "cycle detected"):
                stage_7_write_graph([1, 2], rivers, Path(tmp))


if __name__ == "__main__":
    unittest.main()
