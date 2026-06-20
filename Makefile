.PHONY: help show-targets setup lint test smoke-cli quick doctor version ci-status list-workflow-targets list-diagnostic-targets list-verification-targets verify check-hygiene build-package list-clean-targets clean-generated clean-build clean-test clean-caches clean-all-local list-pipeline-targets list-data-targets list-reference-targets list-governance-targets list-forecasting-targets list-rag-targets list-report-targets list-strategy-targets list-demo-targets run-ingest run-validate run-syntheticness run-reconcile run-report run-full-audit demo-bundle

help:
	@printf "qsr-audit developer commands\n"
	@printf "\n"
	@printf "Setup and verification:\n"
	@printf "  make show-targets       Alias for make help\n"
	@printf "  make setup              Install editable dev dependencies and pre-commit hooks\n"
	@printf "  make lint               Run pre-commit hooks on all files\n"
	@printf "  make test               Run the pytest suite\n"
	@printf "  make smoke-cli          Run fast CLI help smoke checks\n"
	@printf "  make quick              Run CLI smoke checks and repository hygiene\n"
	@printf "  make doctor             Print safe local diagnostics without modifying files\n"
	@printf "  make version            Print installed package version and git commit\n"
	@printf "  make ci-status          Print latest GitHub Actions status for origin/main\n"
	@printf "  make list-workflow-targets    Print workflow command-list index\n"
	@printf "  make list-diagnostic-targets  Print diagnostic/discovery commands and scope notes\n"
	@printf "  make list-verification-targets  Print verification/check commands and scope notes\n"
	@printf "  make verify             Run hooks, coverage tests, repo hygiene, and package build\n"
	@printf "  make check-hygiene      Run repository artifact hygiene checks\n"
	@printf "  make build-package      Build the Python package\n"
	@printf "\n"
	@printf "Pipeline shortcuts:\n"
	@printf "  make list-pipeline-targets    Print pipeline commands and scope notes\n"
	@printf "  make list-data-targets        Print data-layer commands and Bronze/Silver/Gold scopes\n"
	@printf "  make list-reference-targets   Print reference/reconciliation commands and artifact locations\n"
	@printf "  make list-governance-targets  Print Gold/release governance commands and artifacts\n"
	@printf "  make list-forecasting-targets Print forecasting snapshot/baseline commands and artifacts\n"
	@printf "  make list-rag-targets         Print retrieval-only RAG commands and artifact locations\n"
	@printf "  make list-report-targets      Print report/demo artifact commands and output locations\n"
	@printf "  make list-strategy-targets    Print strategy interpretation commands and output locations\n"
	@printf "  make list-demo-targets        Print five-brand demo commands, inputs, and artifacts\n"
	@printf "  make run-ingest         Ingest the default workbook path into Bronze/Silver\n"
	@printf "  make run-validate       Validate the default Silver input\n"
	@printf "  make run-syntheticness  Run syntheticness diagnostics on core metrics\n"
	@printf "  make run-reconcile      Reconcile Silver metrics against manual references\n"
	@printf "  make run-report         Generate reports from Gold outputs\n"
	@printf "  make run-full-audit     Run the standard audit path through release preflight\n"
	@printf "  make demo-bundle        Package the demo showcase bundle\n"
	@printf "\n"
	@printf "Cleanup:\n"
	@printf "  make list-clean-targets       Print cleanup commands and scope notes\n"
	@printf "  make clean-generated    Remove ignored generated data/report artifacts\n"
	@printf "  make clean-build        Remove package build outputs only\n"
	@printf "  make clean-test         Remove pytest and coverage artifacts only\n"
	@printf "  make clean-caches       Remove Python/test/build caches\n"
	@printf "  make clean-all-local    Remove generated local artifacts and caches\n"

show-targets:
	$(MAKE) help

setup:
	pip install -e ".[dev]"
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

smoke-cli:
	pytest tests/test_cli_help.py

quick:
	$(MAKE) smoke-cli
	$(MAKE) check-hygiene

doctor:
	@printf "Python:\n"
	@python --version
	@printf "\nqsr-audit CLI help:\n"
	@qsr-audit --help
	@printf "\nGit branch:\n"
	@git branch --show-current
	@printf "\nGit commit:\n"
	@git rev-parse --short HEAD
	@printf "\nGit status:\n"
	@git status --short --branch

version:
	@python -c "from importlib.metadata import version; print('qsr-audit package:', version('qsr-audit'))"
	@printf "git commit: "
	@git rev-parse --short HEAD

ci-status:
	@if ! command -v gh >/dev/null 2>&1; then \
		printf "GitHub CLI (gh) is not installed. Install gh to check CI status from Make.\n"; \
	else \
		branch=$$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##'); \
		if [ -z "$$branch" ]; then branch=main; fi; \
		printf "Latest GitHub Actions run for origin/%s:\n" "$$branch"; \
		gh run list --branch "$$branch" --limit 1; \
	fi

list-workflow-targets:
	@printf "Workflow target lists\n"
	@printf "\n"
	@printf "  make list-diagnostic-targets    Diagnostic and discovery commands\n"
	@printf "  make list-verification-targets  Verification and check commands\n"
	@printf "  make list-pipeline-targets      Pipeline shortcut commands\n"
	@printf "  make list-data-targets          Data-layer command scopes\n"
	@printf "  make list-reference-targets     Reference/reconciliation command scopes\n"
	@printf "  make list-governance-targets    Gold/release governance command scopes\n"
	@printf "  make list-forecasting-targets   Forecasting experiment command scopes\n"
	@printf "  make list-rag-targets           Retrieval-only RAG command scopes\n"
	@printf "  make list-report-targets        Report/demo artifact commands\n"
	@printf "  make list-strategy-targets      Strategy interpretation command scopes\n"
	@printf "  make list-demo-targets          Five-brand demo command scopes\n"
	@printf "  make list-clean-targets         Cleanup commands\n"

list-diagnostic-targets:
	@printf "Diagnostic and discovery targets\n"
	@printf "\n"
	@printf "  make help               Full Make command index\n"
	@printf "  make show-targets       Alias for make help\n"
	@printf "  make list-workflow-targets  Workflow command-list index\n"
	@printf "  make list-verification-targets  Verification/check commands and scope notes\n"
	@printf "  make list-pipeline-targets    Pipeline shortcut commands and scope notes\n"
	@printf "  make list-data-targets        Data-layer commands and Bronze/Silver/Gold scopes\n"
	@printf "  make list-reference-targets   Reference validation, reconciliation, and coverage scopes\n"
	@printf "  make list-governance-targets  Gold gate, release preflight, and lineage scopes\n"
	@printf "  make list-forecasting-targets Forecast snapshots, panels, baselines, and artifacts\n"
	@printf "  make list-rag-targets         Retrieval-only RAG commands and artifact locations\n"
	@printf "  make list-report-targets      Report/demo artifact commands and output locations\n"
	@printf "  make list-strategy-targets    Strategy interpretation commands and output locations\n"
	@printf "  make list-demo-targets        Five-brand demo commands, inputs, and artifacts\n"
	@printf "  make list-clean-targets       Cleanup commands and scope notes\n"
	@printf "  make doctor             Python, CLI help, git branch/SHA, and repo status\n"
	@printf "  make version            Installed package version and git commit\n"
	@printf "  make ci-status          Latest GitHub Actions status when gh is installed\n"

list-verification-targets:
	@printf "Verification and check targets\n"
	@printf "\n"
	@printf "  make lint               Pre-commit hooks across the repository\n"
	@printf "  make test               Full test suite without coverage gate\n"
	@printf "  make smoke-cli          Fast CLI help smoke checks\n"
	@printf "  make quick              CLI smoke checks plus repository hygiene\n"
	@printf "  make verify             Full local gate: hooks, coverage, hygiene, package build\n"
	@printf "  make check-hygiene      Repository artifact hygiene checks\n"
	@printf "  make build-package      Source distribution and wheel build\n"

verify:
	pre-commit run --all-files
	pytest --cov=src --cov-report=term-missing --cov-fail-under=85
	python scripts/check_repo_hygiene.py
	python -m build

check-hygiene:
	python scripts/check_repo_hygiene.py

build-package:
	python -m build

list-clean-targets:
	@printf "qsr-audit cleanup targets\n"
	@printf "\n"
	@printf "  make clean-build        Packaging outputs only: dist/, build/, *.egg-info\n"
	@printf "  make clean-test         Test outputs only: .pytest_cache/, .coverage, coverage.xml, htmlcov/\n"
	@printf "  make clean-caches       Test/build outputs plus Python bytecode and .ruff_cache\n"
	@printf "  make clean-generated    Generated local data/report artifacts under data/, reports/, strategy/\n"
	@printf "  make clean-all-local    Broad local reset: clean-generated plus clean-caches\n"

clean-generated:
	find data/raw -mindepth 1 ! -name '.gitkeep' -delete
	find data/bronze -mindepth 1 ! -name '.gitkeep' -delete
	find data/silver -mindepth 1 ! -name '.gitkeep' -delete
	find data/gold -mindepth 1 ! -name '.gitkeep' -delete
	find reports -mindepth 1 ! -name '.gitkeep' -delete
	find strategy -mindepth 1 ! -name '.gitkeep' -delete

clean-build:
	rm -rf dist build
	find . -path './.venv' -prune -o -path './.git' -prune -o -name '*.egg-info' -type d -prune -exec rm -rf {} +

clean-test:
	rm -rf .pytest_cache .coverage coverage.xml htmlcov

clean-caches:
	$(MAKE) clean-test
	$(MAKE) clean-build
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	rm -rf .ruff_cache

clean-all-local:
	$(MAKE) clean-generated
	$(MAKE) clean-caches

list-pipeline-targets:
	@printf "Pipeline shortcut targets\n"
	@printf "\n"
	@printf "  make run-ingest         Workbook ingest into Bronze/Silver\n"
	@printf "  make run-validate       Silver workbook validation with AUV tolerance\n"
	@printf "  make run-syntheticness  Syntheticness diagnostics on core metrics\n"
	@printf "  make run-reconcile      Manual-reference reconciliation into Gold artifacts\n"
	@printf "  make run-report         Report generation from Gold outputs\n"
	@printf "  make run-full-audit     Full standard audit path through release preflight\n"
	@printf "  make demo-bundle        Package the five-brand demo showcase bundle\n"

list-data-targets:
	@printf "Data-layer target scopes\n"
	@printf "\n"
	@printf "  make run-ingest         data/raw/source_workbook.xlsx -> data/bronze/ and data/silver/\n"
	@printf "  make run-validate       data/silver/ -> data/gold/ validation flags\n"
	@printf "  make run-syntheticness  data/silver/core_brand_metrics.parquet -> syntheticness diagnostics\n"
	@printf "  make run-reconcile      data/silver/ + data/reference/ -> data/gold/ reconciled metrics\n"
	@printf "  make run-full-audit     Raw workbook -> Bronze, Silver, Gold, and release/report artifacts\n"

list-reference-targets:
	@printf "Reference and reconciliation target scopes\n"
	@printf "\n"
	@printf "  audit-reference CLI            data/reference/ -> data/gold/reference_coverage.parquet and reports/reference/reference_coverage.md\n"
	@printf "  make run-reconcile             data/silver/ + data/reference/ -> data/gold/reconciled_core_metrics.parquet\n"
	@printf "  reconcile-qsr50 CLI            QSR50 coverage and deltas under reports/reconciliation/ and data/gold/\n"
	@printf "  reconcile-primary-source CLI   Primary-source coverage, deltas, and candidates under reports/reconciliation/ and data/gold/\n"
	@printf "  coverage/gap artifacts         reference coverage plus unresolved reference-gap reports for analyst review\n"

list-governance-targets:
	@printf "Governance and release target scopes\n"
	@printf "\n"
	@printf "  make run-full-audit     Includes Gold gate and release preflight before reports\n"
	@printf "  gate-gold CLI           Writes data/gold/gold_publish_decisions.parquet plus publishable/blocked KPI outputs\n"
	@printf "  preflight-release CLI   Writes artifacts/release/preflight_summary.json and .md readiness checks\n"
	@printf "  manifest/audit logs     Lineage records live under artifacts/manifests/ and artifacts/audit_logs/\n"
	@printf "  publishability reports  Gold scorecards live under reports/audit/gold_publish_scorecard.md and .json\n"

list-forecasting-targets:
	@printf "Forecasting experiment target scopes\n"
	@printf "\n"
	@printf "  snapshot-gold CLI       Closed Gold state -> data/gold/history/as_of_date=YYYY-MM-DD/\n"
	@printf "  snapshot manifest       Forecast-ready history index at data/gold/history/snapshot_manifest.parquet\n"
	@printf "  build-forecast-panel CLI Gold history -> artifacts/forecasting/<metric>/panel.parquet\n"
	@printf "  forecast-baseline CLI   Baseline metrics, splits, and summary under artifacts/forecasting/<metric>/\n"
	@printf "  forecasting boundary    Experimental outputs stay out of reports/ and strategy/\n"

list-rag-targets:
	@printf "Retrieval-only RAG target scopes\n"
	@printf "\n"
	@printf "  build-rag-corpus CLI    Vetted Gold/provenance artifacts -> artifacts/rag/corpus/\n"
	@printf "  init/seed/bootstrap CLI Benchmark packs and suggestions under data/rag_benchmarks/<pack>/\n"
	@printf "  validate/reviewer CLI   Validation reports under artifacts/rag/benchmarks/validation/\n"
	@printf "  adjudication CLI        Reviewer conflicts under artifacts/rag/benchmarks/adjudication/\n"
	@printf "  eval/triage CLI         Metrics, per-query rows, and failure triage under artifacts/rag/benchmarks/\n"
	@printf "  search/inspect CLI      Retrieved chunks, metadata, and diagnostics only; no answer generation\n"

list-report-targets:
	@printf "Report and demo artifact targets\n"
	@printf "\n"
	@printf "  make run-report         Writes audit reports under reports/ and strategy/\n"
	@printf "  make run-full-audit     Produces data/gold/, artifacts/release/, reports/, and strategy/\n"
	@printf "  make demo-bundle        Writes shareable demo bundle under artifacts/demo_bundle/\n"

list-strategy-targets:
	@printf "Strategy interpretation target scopes\n"
	@printf "\n"
	@printf "  make run-report         Gold report path writes strategy interpretation outputs\n"
	@printf "  strategy playbook       reports/strategy/strategy_playbook.md plus reports/strategy/recommendations.json\n"
	@printf "  machine outputs         strategy/recommendations.parquet and strategy/recommendations.json\n"
	@printf "  source boundary         Consumes Gold validated, reconciled, and publish-gated artifacts only\n"
	@printf "  interpretation boundary Strategy outputs do not redefine metrics or promote blocked/advisory facts\n"

list-demo-targets:
	@printf "Five-brand demo target scopes\n"
	@printf "\n"
	@printf "  demo-happy-path CLI     Raw workbook plus data/reference/qsr50_reference.csv -> fixed five-brand demo outputs\n"
	@printf "  demo input scope        Starbucks, Taco Bell, Raising Cane's, Dutch Bros, and Shake Shack only\n"
	@printf "  scorecard artifacts     reports/demo/index.html, reports/validation/core_scorecard.html, reports/reconciliation/brand_deltas.csv\n"
	@printf "  summary/gold artifacts  reports/summary/top_risks.md, data/gold/demo_gold.parquet, data/gold/demo_syntheticness.parquet\n"
	@printf "  package-demo CLI        make demo-bundle packages the same demo artifacts under artifacts/demo_bundle/\n"

run-ingest:
	qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx

run-validate:
	qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05

run-syntheticness:
	qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet

run-reconcile:
	qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/

run-report:
	qsr-audit report --output reports/

run-full-audit:
	qsr-audit ingest-workbook --input data/raw/source_workbook.xlsx
	qsr-audit validate-workbook --input data/silver --tolerance-auv 0.05
	qsr-audit run-syntheticness --input data/silver/core_brand_metrics.parquet
	qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
	qsr-audit gate-gold
	qsr-audit preflight-release
	qsr-audit report --output reports/

demo-bundle:
	qsr-audit package-demo
