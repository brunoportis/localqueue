"""Contract checks for the standalone HTTP contact-import example."""

from __future__ import annotations

import ast
from pathlib import Path

EXAMPLE = Path(__file__).parents[1] / "examples" / "contact_import.py"


def test_contact_import_example_is_a_parseable_standalone_program():
    """Keep the documented production recipe importable after installing httpx."""
    source = EXAMPLE.read_text(encoding="utf-8")

    ast.parse(source, filename=str(EXAMPLE))

    assert '@event(identity=("import_id", "cnpj"))' in source
    assert "raise Reject(" in source
    assert 'category="validation"' in source
    assert 'raise Retry("rate limited", after=retry_after)' in source
    assert "await bus.execute(contacts, timeout=None)" in source
