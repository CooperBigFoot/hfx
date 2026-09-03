from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.verify_visions import verify


REPOSITORY = "CooperBigFoot/hfx"


def vision(program: int = 103, effort: int = 104, repository: str = REPOSITORY) -> str:
    return (
        "# Vision: Test\n"
        f"Program: https://github.com/{repository}/issues/{program}\n"
        f"Effort: https://github.com/{repository}/issues/{effort}\n"
    )


class VerifyVisionsTests(unittest.TestCase):
    def verify_files(self, files: dict[str, str], repository: str = REPOSITORY) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "planning" / "visions"
            root.mkdir(parents=True)
            for relative, content in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            return verify(root, repository)

    def test_accepts_valid_vision(self) -> None:
        self.assertEqual(self.verify_files({"2026-09-03-test.md": vision()}), [])

    def test_rejects_foreign_repository(self) -> None:
        errors = self.verify_files({"2026-09-03-test.md": vision(repository="attacker/fake")})
        self.assertTrue(any("must belong" in error for error in errors))

    def test_rejects_same_program_and_effort_issue(self) -> None:
        errors = self.verify_files({"2026-09-03-test.md": vision(effort=103)})
        self.assertTrue(any("different issues" in error for error in errors))

    def test_rejects_malformed_repository_argument(self) -> None:
        errors = self.verify_files({"2026-09-03-test.md": vision()}, repository="bad?/repo#")
        self.assertTrue(any("malformed canonical repository" in error for error in errors))

    def test_rejects_duplicate_effort(self) -> None:
        errors = self.verify_files(
            {
                "2026-09-03-one.md": vision(),
                "2026-09-03-two.md": vision(),
            }
        )
        self.assertTrue(any("already linked" in error for error in errors))

    def test_rejects_metadata_after_body_text(self) -> None:
        content = vision().replace("# Vision: Test\n", "# Vision: Test\nBody first.\n")
        errors = self.verify_files({"2026-09-03-test.md": content})
        self.assertTrue(any("immediately follow" in error for error in errors))

    def test_rejects_nested_entries(self) -> None:
        errors = self.verify_files({"nested/2026-09-03-test.md": vision()})
        self.assertTrue(any("nested directories" in error for error in errors))

    def test_rejects_noncanonical_filename(self) -> None:
        errors = self.verify_files({"Test Vision.md": vision()})
        self.assertTrue(any("filename must use" in error for error in errors))

    def test_current_repository_visions_pass(self) -> None:
        root = Path(__file__).resolve().parents[1] / "planning" / "visions"
        self.assertEqual(verify(root, REPOSITORY), [])


if __name__ == "__main__":
    unittest.main()
