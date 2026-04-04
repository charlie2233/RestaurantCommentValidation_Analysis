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
    build_reconciled_core_metrics,
    load_reference_catalog,
    reconcile_core_metrics,
    render_reconciliation_summary,
    standardize_reference_frame,
    write_reconciliation_outputs,
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

__all__ = [
    "BrandResolution",
    "FieldComparison",
    "ProvenanceRecord",
    "ProvenanceRegistry",
    "REFERENCE_TEMPLATE_FILES",
    "ReconciliationArtifacts",
    "ReconciliationRun",
    "build_provenance_record",
    "build_reconciled_core_metrics",
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
    "resolve_brand_name",
    "resolve_brand_series",
    "select_best_reference_row",
    "standardize_reference_frame",
    "write_reconciliation_outputs",
]
