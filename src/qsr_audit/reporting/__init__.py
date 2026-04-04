"""Analyst-facing report generation."""

from qsr_audit.reporting.generator import ReportArtifacts, write_reports
from qsr_audit.reporting.scorecards import (
    BrandScorecard,
    GlobalScorecard,
    ReportBundle,
    ReportInputs,
    build_report_bundle,
    load_report_inputs,
    slugify_brand_name,
)

__all__ = [
    "BrandScorecard",
    "GlobalScorecard",
    "ReportArtifacts",
    "ReportBundle",
    "ReportInputs",
    "build_report_bundle",
    "load_report_inputs",
    "slugify_brand_name",
    "write_reports",
]
