"""Smoke tests – ensure the package imports and CLI entrypoints are reachable."""

import importlib


def test_package_importable():
    mod = importlib.import_module("qsr_audit")
    assert mod.__version__ == "0.1.0"


def test_subpackages_importable():
    for sub in ["ingest", "normalize", "validate", "reconcile", "reporting", "strategy"]:
        mod = importlib.import_module(f"qsr_audit.{sub}")
        assert mod is not None


def test_settings_instantiate():
    from qsr_audit.config import Settings

    s = Settings()
    assert s.log_level == "INFO"


def test_cli_app_exists():
    from qsr_audit.cli import app

    assert app is not None
