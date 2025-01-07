from io import StringIO
import sys

from kart.diagnostics import print_diagnostics

def test_print_diagnostics(monkeypatch):
	stderr = StringIO()
	monkeypatch.setattr(sys, "stderr", stderr)
	print_diagnostics()
	assert "DIAGNOSTICS" in stderr.getvalue()
