"""Regression tests for the MERIT v2 cross-basin preflight."""

from __future__ import annotations

import unittest

from verify_cross_basin import DEFAULT_SELECTED_PFAF_CODES, parse_pfaf_codes


class PfafSelectionTests(unittest.TestCase):
    def test_default_selection_is_sixty_and_excludes_pfaf_35(self) -> None:
        self.assertEqual(len(DEFAULT_SELECTED_PFAF_CODES), 60)
        self.assertNotIn(35, DEFAULT_SELECTED_PFAF_CODES)

    def test_rejects_excluded_pfaf_35(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "excluded Pfaf"):
            parse_pfaf_codes("35")

    def test_parses_explicit_codes(self) -> None:
        self.assertEqual(parse_pfaf_codes("27,42,91"), [27, 42, 91])


if __name__ == "__main__":
    unittest.main()
