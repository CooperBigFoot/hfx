"""Native orientation input guards match corrected full compilation."""
from pathlib import Path
import tempfile
import unittest

import numpy as np
from shapely.geometry import LineString

import build_adapter
import regenerate_outlets
from test_regenerate_outlets import compile_reference, write_streamnet


class NativeStreamReaderGuardTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(__file__).resolve().parents[2] / ".test-tmp"
        scratch.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch, prefix="native-clamp-")
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_clamp_degeneracy_change_refuses_full_build_reader_orient_and_regeneration(self):
        rows = [dict(LINKNO=i, DSLINKNO=downstream, DSContArea=area, geometry=LineString(coordinates))
                for i, downstream, area, coordinates in (
                    (1, -1, .3077, [(180.00008, 0), (180.00009, 0)]),
                    (2, 3, .1, [(0, 0), (.01, 0)]),
                    (3, -1, .2, [(.01, 0), (.02, 0)]))]
        message = "degenerate reach classification changed during coordinate normalization"
        with self.assertRaisesRegex(ValueError, message):
            compile_reference(self.root, source_rows=rows, polygon_ids=(1,))
        source = self.root / "streamnet.gpkg"
        with self.assertRaisesRegex(ValueError, message):
            build_adapter._read_streamnet_topology_columns(source)
        basin, header = next(iter(build_adapter.load_header_crosswalk().items()))
        report = build_adapter.orient_topology(self.root / "basins.gpkg", source, processing_basin_id=basin)
        self.assertEqual(report["outcome"], "refused")
        with self.assertRaisesRegex(ValueError, message):
            regenerate_outlets.derive_native_outlets(np.array([1], dtype="int64"), source, header,
                reference_up_area_km2=np.array([.3077], dtype="float32"))

    def test_healthy_clamp_and_native_degenerate_reaches_remain_admitted(self):
        source = self.root / "streamnet.gpkg"
        write_streamnet(source, [
            dict(LINKNO=1, DSLINKNO=-1, DSContArea=1., geometry=LineString([(180.00008, 0), (179.99, 0)])),
            dict(LINKNO=2, DSLINKNO=-1, DSContArea=1., geometry=LineString([(0, 0), (0, 0)])),
        ])
        columns = build_adapter._read_streamnet_topology_columns(source)
        np.testing.assert_array_equal(columns.degenerate, [False, True])
        self.assertEqual(columns.endpoints[0, 0, 0], 180.)


if __name__ == "__main__":
    unittest.main()
