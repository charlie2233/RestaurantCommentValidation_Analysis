"""Makefile workflow regressions."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_target_commands(target_name: str) -> list[str]:
    makefile = REPO_ROOT / "Makefile"
    lines = makefile.read_text(encoding="utf-8").splitlines()
    target_header = f"{target_name}:"
    start = lines.index(target_header) + 1
    commands: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith(("\t", " ")):
            break
        if line.startswith("\t"):
            commands.append(line.strip())
    return commands


def test_run_full_audit_includes_release_gate_before_reports() -> None:
    commands = _make_target_commands("run-full-audit")

    assert "qsr-audit gate-gold" in commands
    assert "qsr-audit preflight-release" in commands
    assert commands.index("qsr-audit gate-gold") < commands.index("qsr-audit preflight-release")
    assert commands.index("qsr-audit preflight-release") < commands.index(
        "qsr-audit report --output reports/"
    )


def test_show_targets_aliases_help() -> None:
    commands = _make_target_commands("show-targets")

    assert commands == ["$(MAKE) help"]


def test_smoke_cli_target_runs_help_only_cli_regressions() -> None:
    commands = _make_target_commands("smoke-cli")

    assert commands == ["pytest tests/test_cli_help.py"]
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_quick_target_runs_cli_smoke_and_hygiene_checks() -> None:
    commands = _make_target_commands("quick")

    assert commands == ["$(MAKE) smoke-cli", "$(MAKE) check-hygiene"]
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_doctor_target_prints_safe_diagnostics_only() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("doctor")]

    assert "python --version" in commands
    assert "qsr-audit --help" in commands
    assert "git branch --show-current" in commands
    assert "git rev-parse --short HEAD" in commands
    assert "git status --short --branch" in commands
    qsr_commands = [command for command in commands if command.startswith("qsr-audit ")]
    assert qsr_commands == ["qsr-audit --help"]


def test_version_target_prints_package_version_and_git_commit() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("version")]
    recipe = "\n".join(commands)

    assert "from importlib.metadata import version" in recipe
    assert "version('qsr-audit')" in recipe
    assert "git rev-parse --short HEAD" in commands
    assert all(not command.startswith("qsr-audit ") for command in commands)


def test_ci_status_target_uses_github_cli_with_soft_missing_gh_exit() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("ci-status")]
    recipe = "\n".join(commands)

    assert "command -v gh" in recipe
    assert "GitHub CLI (gh) is not installed" in recipe
    assert "else" in recipe
    assert "refs/remotes/origin/HEAD" in recipe
    assert "gh run list" in recipe
    assert '--branch "$$branch"' in recipe
    assert all("qsr-audit " not in command for command in commands)


def test_clean_build_target_removes_only_packaging_outputs() -> None:
    commands = _make_target_commands("clean-build")
    recipe = "\n".join(commands)

    assert "rm -rf dist build" in commands
    assert "-name '*.egg-info'" in recipe
    assert "./.venv" in recipe
    assert "./.git" in recipe
    assert "data/" not in recipe
    assert "reports" not in recipe
    assert "strategy" not in recipe


def test_list_clean_targets_prints_cleanup_scope_notes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-clean-targets")
    ]
    recipe = "\n".join(commands)

    assert "qsr-audit cleanup targets" in recipe
    assert "make clean-build" in recipe
    assert "make clean-test" in recipe
    assert "make clean-caches" in recipe
    assert "make clean-generated" in recipe
    assert "make clean-all-local" in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_pipeline_targets_prints_pipeline_scope_notes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-pipeline-targets")
    ]
    recipe = "\n".join(commands)

    assert "Pipeline shortcut targets" in recipe
    assert "make run-ingest" in recipe
    assert "make run-validate" in recipe
    assert "make run-syntheticness" in recipe
    assert "make run-reconcile" in recipe
    assert "make run-report" in recipe
    assert "make run-full-audit" in recipe
    assert "make demo-bundle" in recipe
    assert "make list-report-targets" not in recipe
    assert "qsr-audit " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_workflow_targets_prints_command_list_index_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-workflow-targets")
    ]
    recipe = "\n".join(commands)

    assert "Workflow target lists" in recipe
    assert "make list-diagnostic-targets" in recipe
    assert "make list-verification-targets" in recipe
    assert "make list-pipeline-targets" in recipe
    assert "make list-data-targets" in recipe
    assert "make list-reference-targets" in recipe
    assert "make list-governance-targets" in recipe
    assert "make list-release-targets" in recipe
    assert "make list-history-targets" in recipe
    assert "make list-forecasting-targets" in recipe
    assert "make list-rag-targets" in recipe
    assert "make list-report-targets" in recipe
    assert "make list-strategy-targets" in recipe
    assert "make list-demo-targets" in recipe
    assert "make list-clean-targets" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_diagnostic_targets_prints_non_mutating_scope_notes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-diagnostic-targets")
    ]
    recipe = "\n".join(commands)

    assert "Diagnostic and discovery targets" in recipe
    assert "make help" in recipe
    assert "make show-targets" in recipe
    assert "make list-workflow-targets" in recipe
    assert "make list-pipeline-targets" in recipe
    assert "make list-data-targets" in recipe
    assert "make list-reference-targets" in recipe
    assert "make list-governance-targets" in recipe
    assert "make list-release-targets" in recipe
    assert "make list-history-targets" in recipe
    assert "make list-forecasting-targets" in recipe
    assert "make list-rag-targets" in recipe
    assert "make list-report-targets" in recipe
    assert "make list-strategy-targets" in recipe
    assert "make list-demo-targets" in recipe
    assert "make list-clean-targets" in recipe
    assert "make doctor" in recipe
    assert "make version" in recipe
    assert "make ci-status" in recipe
    assert "pytest" not in recipe
    assert "rm -rf" not in recipe
    assert "qsr-audit " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_data_targets_prints_data_layer_scopes_only() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("list-data-targets")]
    recipe = "\n".join(commands)

    assert "Data-layer target scopes" in recipe
    assert "make run-ingest" in recipe
    assert "data/raw/source_workbook.xlsx" in recipe
    assert "data/bronze/" in recipe
    assert "data/silver/" in recipe
    assert "make run-validate" in recipe
    assert "data/gold/ validation flags" in recipe
    assert "make run-syntheticness" in recipe
    assert "syntheticness diagnostics" in recipe
    assert "make run-reconcile" in recipe
    assert "data/reference/" in recipe
    assert "data/gold/ reconciled metrics" in recipe
    assert "make run-full-audit" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_reference_targets_prints_reference_artifact_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-reference-targets")
    ]
    recipe = "\n".join(commands)

    assert "Reference and reconciliation target scopes" in recipe
    assert "audit-reference CLI" in recipe
    assert "data/reference/" in recipe
    assert "data/gold/reference_coverage.parquet" in recipe
    assert "reports/reference/reference_coverage.md" in recipe
    assert "make run-reconcile" in recipe
    assert "data/gold/reconciled_core_metrics.parquet" in recipe
    assert "reconcile-qsr50 CLI" in recipe
    assert "QSR50 coverage and deltas" in recipe
    assert "reconcile-primary-source CLI" in recipe
    assert "Primary-source coverage, deltas, and candidates" in recipe
    assert "reports/reconciliation/" in recipe
    assert "coverage/gap artifacts" in recipe
    assert "unresolved reference-gap reports" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_governance_targets_prints_release_gate_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-governance-targets")
    ]
    recipe = "\n".join(commands)

    assert "Governance and release target scopes" in recipe
    assert "make run-full-audit" in recipe
    assert "Gold gate" in recipe
    assert "release preflight" in recipe
    assert "gate-gold CLI" in recipe
    assert "data/gold/gold_publish_decisions.parquet" in recipe
    assert "publishable/blocked KPI outputs" in recipe
    assert "preflight-release CLI" in recipe
    assert "artifacts/release/preflight_summary.json" in recipe
    assert "manifest/audit logs" in recipe
    assert "artifacts/manifests/" in recipe
    assert "artifacts/audit_logs/" in recipe
    assert "publishability reports" in recipe
    assert "reports/audit/gold_publish_scorecard.md" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_release_targets_prints_preflight_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-release-targets")
    ]
    recipe = "\n".join(commands)

    assert "Release and preflight target scopes" in recipe
    assert "make run-full-audit" in recipe
    assert "gate-gold" in recipe
    assert "preflight-release" in recipe
    assert "required Gold inputs" in recipe
    assert "data/gold/gold_publish_decisions.parquet" in recipe
    assert "data/gold/publishable_kpis.parquet" in recipe
    assert "data/gold/blocked_kpis.parquet" in recipe
    assert "required manifests" in recipe
    assert "validate-workbook" in recipe
    assert "run-syntheticness" in recipe
    assert "reconcile" in recipe
    assert "artifacts/manifests/<command>/latest.json" in recipe
    assert "preflight outputs" in recipe
    assert "artifacts/release/preflight_summary.json" in recipe
    assert "artifacts/release/preflight_summary.md" in recipe
    assert "release boundary" in recipe
    assert "advisory/blocked rows stay out of publishable exports" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_history_targets_prints_snapshot_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-history-targets")
    ]
    recipe = "\n".join(commands)

    assert "Monthly Gold history target scopes" in recipe
    assert "snapshot-gold CLI" in recipe
    assert "data/gold/history/as_of_date=YYYY-MM-DD/" in recipe
    assert "preconditions" in recipe
    assert "Evidence cycle closed or deferred" in recipe
    assert "reconcile" in recipe
    assert "gate-gold" in recipe
    assert "preflight-release" in recipe
    assert "archived files" in recipe
    assert "forecast_snapshot.parquet" in recipe
    assert "gold_publish_decisions.parquet" in recipe
    assert "publishable_kpis.parquet" in recipe
    assert "manifest.json" in recipe
    assert "snapshot index" in recipe
    assert "data/gold/history/snapshot_manifest.parquet" in recipe
    assert "records each as_of_date" in recipe
    assert "forecast boundary" in recipe
    assert "Repeated publishable Gold snapshots only" in recipe
    assert "no raw workbook, advisory, or blocked facts" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_forecasting_targets_prints_experiment_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-forecasting-targets")
    ]
    recipe = "\n".join(commands)

    assert "Forecasting experiment target scopes" in recipe
    assert "snapshot-gold CLI" in recipe
    assert "data/gold/history/as_of_date=YYYY-MM-DD/" in recipe
    assert "snapshot manifest" in recipe
    assert "data/gold/history/snapshot_manifest.parquet" in recipe
    assert "build-forecast-panel CLI" in recipe
    assert "artifacts/forecasting/<metric>/panel.parquet" in recipe
    assert "forecast-baseline CLI" in recipe
    assert "artifacts/forecasting/<metric>/" in recipe
    assert "forecasting boundary" in recipe
    assert "reports/" in recipe
    assert "strategy/" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_rag_targets_prints_retrieval_only_scopes_only() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("list-rag-targets")]
    recipe = "\n".join(commands)

    assert "Retrieval-only RAG target scopes" in recipe
    assert "build-rag-corpus CLI" in recipe
    assert "artifacts/rag/corpus/" in recipe
    assert "init/seed/bootstrap CLI" in recipe
    assert "data/rag_benchmarks/<pack>/" in recipe
    assert "validate/reviewer CLI" in recipe
    assert "artifacts/rag/benchmarks/validation/" in recipe
    assert "adjudication CLI" in recipe
    assert "artifacts/rag/benchmarks/adjudication/" in recipe
    assert "eval/triage CLI" in recipe
    assert "artifacts/rag/benchmarks/" in recipe
    assert "search/inspect CLI" in recipe
    assert "no answer generation" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_report_targets_prints_output_locations_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-report-targets")
    ]
    recipe = "\n".join(commands)

    assert "Report and demo artifact targets" in recipe
    assert "make run-report" in recipe
    assert "reports/" in recipe
    assert "strategy/" in recipe
    assert "make run-full-audit" in recipe
    assert "data/gold/" in recipe
    assert "artifacts/release/" in recipe
    assert "make demo-bundle" in recipe
    assert "artifacts/demo_bundle/" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_strategy_targets_prints_interpretation_scopes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-strategy-targets")
    ]
    recipe = "\n".join(commands)

    assert "Strategy interpretation target scopes" in recipe
    assert "make run-report" in recipe
    assert "Gold report path writes strategy interpretation outputs" in recipe
    assert "strategy playbook" in recipe
    assert "reports/strategy/strategy_playbook.md" in recipe
    assert "reports/strategy/recommendations.json" in recipe
    assert "machine outputs" in recipe
    assert "strategy/recommendations.parquet" in recipe
    assert "strategy/recommendations.json" in recipe
    assert "Gold validated, reconciled, and publish-gated artifacts only" in recipe
    assert "do not redefine metrics" in recipe
    assert "blocked/advisory facts" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_demo_targets_prints_demo_scopes_only() -> None:
    commands = [command.removeprefix("@") for command in _make_target_commands("list-demo-targets")]
    recipe = "\n".join(commands)

    assert "Five-brand demo target scopes" in recipe
    assert "demo-happy-path CLI" in recipe
    assert "Raw workbook plus data/reference/qsr50_reference.csv" in recipe
    assert "fixed five-brand demo outputs" in recipe
    assert "Starbucks" in recipe
    assert "Taco Bell" in recipe
    assert "Raising Cane's" in recipe
    assert "Dutch Bros" in recipe
    assert "Shake Shack" in recipe
    assert "reports/demo/index.html" in recipe
    assert "reports/validation/core_scorecard.html" in recipe
    assert "reports/reconciliation/brand_deltas.csv" in recipe
    assert "reports/summary/top_risks.md" in recipe
    assert "data/gold/demo_gold.parquet" in recipe
    assert "data/gold/demo_syntheticness.parquet" in recipe
    assert "package-demo CLI" in recipe
    assert "make demo-bundle" in recipe
    assert "artifacts/demo_bundle/" in recipe
    assert "qsr-audit " not in recipe
    assert "pytest" not in recipe
    assert "pre-commit" not in recipe
    assert "rm -rf" not in recipe
    assert "find " not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_list_verification_targets_prints_check_scope_notes_only() -> None:
    commands = [
        command.removeprefix("@") for command in _make_target_commands("list-verification-targets")
    ]
    recipe = "\n".join(commands)

    assert "Verification and check targets" in recipe
    assert "make lint" in recipe
    assert "make test" in recipe
    assert "make smoke-cli" in recipe
    assert "make quick" in recipe
    assert "make verify" in recipe
    assert "make check-hygiene" in recipe
    assert "make build-package" in recipe
    assert "pre-commit run" not in recipe
    assert "pytest " not in recipe
    assert "python scripts/check_repo_hygiene.py" not in recipe
    assert "python -m build" not in recipe
    assert all("$(MAKE)" not in command for command in commands)


def test_clean_test_target_removes_only_test_and_coverage_outputs() -> None:
    commands = _make_target_commands("clean-test")
    recipe = "\n".join(commands)

    assert commands == ["rm -rf .pytest_cache .coverage coverage.xml htmlcov"]
    assert "dist" not in recipe
    assert "build" not in recipe
    assert "egg-info" not in recipe
    assert "data/" not in recipe
    assert "reports" not in recipe
    assert "strategy" not in recipe


def test_clean_caches_composes_narrow_cleanup_targets() -> None:
    commands = _make_target_commands("clean-caches")

    assert commands == [
        "$(MAKE) clean-test",
        "$(MAKE) clean-build",
        "find . -type d -name '__pycache__' -prune -exec rm -rf {} +",
        "rm -rf .ruff_cache",
    ]


def test_clean_all_local_composes_generated_and_cache_cleanup() -> None:
    commands = _make_target_commands("clean-all-local")

    assert commands == ["$(MAKE) clean-generated", "$(MAKE) clean-caches"]
