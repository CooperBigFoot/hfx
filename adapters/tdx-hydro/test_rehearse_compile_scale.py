"""Regression tests for bounded-memory TDX-Hydro compilation.

The campaign VM cannot exceed 32 GB. The Hetzner account's dedicated-vCPU
quota is 8 (a ccx33: 8 cores / 32 GB) and the console states the account is
too new to request a limit increase, so the 64 GB ccx43 is unavailable
indefinitely. Every shared server type caps at 32 GB. The planetary target is
all 62 processing basins, so compile MUST fit under 32 GB for basins at least
as large as the largest observed.

MEASURED EVIDENCE FROM PAID RUNS (real NGA data, ccx33, 32 GB, no swap):
- basin 1020000010: input 8,859,344,896 bytes -> kernel OOM. Killed at
  anon-rss 31,671,528 kB, total-vm 36,500,432 kB.
- basin 7020000010: input 7,584,165,888 bytes -> compiled successfully.
- basin 9020000010: input 7,506,964,480 bytes -> compiled successfully.
So roughly 7.5 GB of input fits and 8.9 GB does not; the margin today is razor
thin and peak memory appears to scale steeply with input size.
"""

from __future__ import annotations

import math
import subprocess
import sys
import unittest

import rehearse_compile_scale


class CompileScaleRehearsalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.result = rehearse_compile_scale.run_rehearsal(
            basin_rows=rehearse_compile_scale.CI_BASIN_ROWS,
            streamnet_rows=rehearse_compile_scale.CI_STREAMNET_ROWS,
            rss_ceiling_bytes=rehearse_compile_scale.CI_RSS_CEILING_BYTES,
            scratch_ceiling_bytes=rehearse_compile_scale.CI_SCRATCH_CEILING_BYTES,
            child_timeout_seconds=rehearse_compile_scale.CHILD_TIMEOUT_SECONDS,
        )

    def test_real_build_stays_below_fixed_rss_ceiling(self) -> None:
        self.assertLessEqual(
            self.result["parent_observed_tree_peak_rss_bytes"],
            rehearse_compile_scale.CI_RSS_CEILING_BYTES,
        )

    def test_success_report_has_exact_authored_keys(self) -> None:
        self.assertEqual(
            set(self.result),
            {
                "adapter_high_water_rss_bytes",
                "adapter_observed_peak_rss_bytes",
                "basins_coordinate_count",
                "basins_input_bytes",
                "basins_rows",
                "child_timeout_seconds",
                "largest_allocating_phase",
                "largest_phase_allocation_delta_bytes",
                "parent_observed_tree_peak_rss_bytes",
                "rss_ceiling_bytes",
                "scratch_ceiling_bytes",
                "scratch_high_water_bytes",
                "streamnet_coordinate_count",
                "streamnet_input_bytes",
                "streamnet_rows",
                "wall_time_seconds",
            },
        )
        self.assertTrue(all(math.isfinite(float(value)) for value in self.result.values() if isinstance(value, float)))

    def test_reported_high_water_tracks_parent_observation(self) -> None:
        self.assertGreaterEqual(
            self.result["adapter_high_water_rss_bytes"],
            self.result["parent_observed_tree_peak_rss_bytes"] - 67_108_864,
        )

    def test_one_byte_ceiling_has_stable_failure_contract(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                rehearse_compile_scale.__file__,
                "--basin-rows",
                "8",
                "--streamnet-rows",
                "8",
                "--rss-ceiling-bytes",
                "1",
                "--child-timeout-seconds",
                "30",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(completed.stdout, "")
        self.assertRegex(completed.stderr, r"^compile rehearsal failed: RSS ceiling exceeded:")


if __name__ == "__main__":
    unittest.main()
