.PHONY: docs docs-serve

DOCS_DEPS := mkdocs-material>=9,<10

docs:
	uv run --no-project --with '$(DOCS_DEPS)' mkdocs build --strict

docs-serve:
	uv run --no-project --with '$(DOCS_DEPS)' mkdocs serve
