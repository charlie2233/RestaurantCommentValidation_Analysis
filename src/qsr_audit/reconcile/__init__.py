"""Gold-layer reconciliation modules."""

from qsr_audit.reconcile.entity_resolution import (
    BrandResolution,
    canonical_brand_dictionary,
    normalize_brand_key,
    resolve_brand_name,
    resolve_brand_series,
)
from qsr_audit.reconcile.pipeline import (
    REFERENCE_TEMPLATE_FILES,
    ReconciliationArtifacts,
    ReconciliationRun,
    ReferenceCoverageArtifacts,
    ReferenceCoverageRun,
    audit_reference_coverage,
    build_reconciled_core_metrics,
    build_reference_coverage,
    load_reference_catalog,
    reconcile_core_metrics,
    render_reconciliation_summary,
    render_reference_coverage_summary,
    standardize_reference_frame,
    write_reconciliation_outputs,
    write_reference_coverage_outputs,
)
from qsr_audit.reconcile.provenance import (
    ProvenanceRecord,
    ProvenanceRegistry,
    build_provenance_record,
    provenance_frame,
    provenance_records,
)
from qsr_audit.reconcile.reconciliation import (
    FieldComparison,
    compare_numeric_field,
    compare_rank_field,
    grade_numeric_credibility,
    overall_reconciliation_grade,
    select_best_reference_row,
)
from qsr_audit.reconcile.reference_audit import (
    validate_reference_file as validate_reference_frame,
)

__all__ = [
    "BrandResolution",
    "FieldComparison",
    "ProvenanceRecord",
    "ProvenanceRegistry",
    "REFERENCE_TEMPLATE_FILES",
    "ReferenceCoverageArtifacts",
    "ReferenceCoverageRun",
    "ReconciliationArtifacts",
    "ReconciliationRun",
    "audit_reference_coverage",
    "build_provenance_record",
    "build_reconciled_core_metrics",
    "build_reference_coverage",
    "canonical_brand_dictionary",
    "compare_numeric_field",
    "compare_rank_field",
    "grade_numeric_credibility",
    "load_reference_catalog",
    "normalize_brand_key",
    "overall_reconciliation_grade",
    "provenance_frame",
    "provenance_records",
    "reconcile_core_metrics",
    "render_reconciliation_summary",
    "render_reference_coverage_summary",
    "resolve_brand_name",
    "resolve_brand_series",
    "select_best_reference_row",
    "standardize_reference_frame",
    "validate_reference_frame",
    "write_reference_coverage_outputs",
    "write_reconciliation_outputs",
]
