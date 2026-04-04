"""Validation core for the normalized QSR workbook."""

from qsr_audit.validate.invariants import (
    InvariantBundle,
    check_brand_alignment,
    check_core_brand_uniqueness,
    check_implied_auv,
    check_monotonic_ranges,
    check_rank_uniqueness,
    evaluate_invariants,
)
from qsr_audit.validate.models import (
    ValidationArtifacts,
    ValidationCategory,
    ValidationFinding,
    ValidationRun,
    ValidationSeverity,
)
from qsr_audit.validate.reporting import (
    render_validation_results_json,
    render_validation_summary,
    summarize_findings,
    write_validation_outputs,
)
from qsr_audit.validate.schemas import SchemaBundle, build_schema_bundle, validate_schema
from qsr_audit.validate.syntheticness import (
    SyntheticnessArtifacts,
    SyntheticnessRun,
    analyze_syntheticness_signals,
    load_core_metrics_for_syntheticness,
    run_syntheticness,
    write_syntheticness_outputs,
)
from qsr_audit.validate.syntheticness_reporting import (
    SyntheticnessReport,
    SyntheticnessSignal,
    build_syntheticness_report,
    render_syntheticness_report,
    write_syntheticness_report,
)
from qsr_audit.validate.workbook import ValidationTables, load_validation_tables, validate_workbook

__all__ = [
    "InvariantBundle",
    "SchemaBundle",
    "SyntheticnessArtifacts",
    "SyntheticnessReport",
    "SyntheticnessRun",
    "SyntheticnessSignal",
    "ValidationArtifacts",
    "ValidationCategory",
    "ValidationFinding",
    "ValidationRun",
    "ValidationSeverity",
    "ValidationTables",
    "build_schema_bundle",
    "build_syntheticness_report",
    "check_brand_alignment",
    "check_core_brand_uniqueness",
    "check_implied_auv",
    "check_monotonic_ranges",
    "check_rank_uniqueness",
    "evaluate_invariants",
    "load_core_metrics_for_syntheticness",
    "load_validation_tables",
    "render_syntheticness_report",
    "render_validation_results_json",
    "render_validation_summary",
    "run_syntheticness",
    "summarize_findings",
    "analyze_syntheticness_signals",
    "validate_schema",
    "validate_workbook",
    "write_syntheticness_outputs",
    "write_syntheticness_report",
    "write_validation_outputs",
]
