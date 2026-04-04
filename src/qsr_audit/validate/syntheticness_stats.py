"""Statistical syntheticness signals for normalized numeric fields."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Sequence

import numpy as np
import pandas as pd
from scipy.stats import chisquare

from qsr_audit.validate.syntheticness_reporting import SyntheticnessSignal

FIRST_DIGIT_MIN_SAMPLE = 40
FIRST_TWO_DIGIT_MIN_SAMPLE = 100
HEAPING_MIN_SAMPLE = 15
BENFORD_MIN_ORDER_SPAN = 10.0
NICE_NUMBER_BASELINE = 0.12


def extract_first_digits(values: Iterable[object]) -> list[int]:
    """Extract the first significant digit for each positive numeric value."""

    digits: list[int] = []
    for value in values:
        digit = _extract_significant_digits(value, count=1)
        if digit is not None:
            digits.append(digit)
    return digits


def extract_first_two_digits(values: Iterable[object]) -> list[int]:
    """Extract the first two significant digits for each positive numeric value."""

    digits: list[int] = []
    for value in values:
        digit = _extract_significant_digits(value, count=2)
        if digit is not None:
            digits.append(digit)
    return digits


def extract_end_digits(values: Iterable[object]) -> list[int]:
    """Extract the final digit of each rounded integer value."""

    digits: list[int] = []
    for value in values:
        coerced = _coerce_numeric(value)
        if coerced is None:
            continue
        digits.append(int(abs(round(coerced))) % 10)
    return digits


def count_digits(values: Iterable[int], *, domain: Sequence[int]) -> dict[int, int]:
    """Count integer digits over a fixed domain."""

    counts = Counter(int(value) for value in values)
    return {digit: int(counts.get(digit, 0)) for digit in domain}


def analyze_first_digit_benford(
    values: Iterable[object],
    *,
    field_name: str,
    title: str | None = None,
    caveat: str | None = None,
    dataset: str = "core_brand_metrics",
) -> SyntheticnessSignal:
    """Compare first-digit frequencies against Benford expectations."""

    digits = extract_first_digits(values)
    return _analyze_benford(
        digits=digits,
        domain=list(range(1, 10)),
        expected=_expected_first_digit_benford(),
        signal_type="benford_first_digit",
        title=title or f"First-digit Benford check for {field_name}",
        field_name=field_name,
        min_sample_size=FIRST_DIGIT_MIN_SAMPLE,
        caveat=caveat,
        dataset=dataset,
    )


def analyze_first_two_digit_benford(
    values: Iterable[object],
    *,
    field_name: str,
    title: str | None = None,
    caveat: str | None = None,
    dataset: str = "core_brand_metrics",
) -> SyntheticnessSignal:
    """Compare first-two-digit frequencies against Benford expectations."""

    digits = extract_first_two_digits(values)
    return _analyze_benford(
        digits=digits,
        domain=list(range(10, 100)),
        expected=_expected_first_two_digit_benford(),
        signal_type="benford_first_two_digits",
        title=title or f"First-two-digit Benford check for {field_name}",
        field_name=field_name,
        min_sample_size=FIRST_TWO_DIGIT_MIN_SAMPLE,
        caveat=caveat,
        dataset=dataset,
    )


def analyze_end_digit_heaping(
    values: Iterable[object],
    *,
    field_name: str,
    title: str | None = None,
    caveat: str | None = None,
    dataset: str = "core_brand_metrics",
) -> SyntheticnessSignal:
    """Check whether final digits over-cluster on 0 or 5."""

    digits = extract_end_digits(values)
    counts = count_digits(digits, domain=range(10))
    sample_size = len(digits)
    if sample_size < HEAPING_MIN_SAMPLE:
        return SyntheticnessSignal(
            signal_type="end_digit_heaping",
            title=title or f"End-digit heaping check for {field_name}",
            plain_english=(
                f"Skipped end-digit heaping analysis for `{field_name}` because only "
                f"{sample_size} usable values were available."
            ),
            strength="unknown",
            dataset=dataset,
            field_name=field_name,
            method="chi-square + 0/5 share",
            sample_size=sample_size,
            caveat=_merge_caveats("Small samples can make end-digit tests unstable.", caveat),
            details={"digit_counts": counts},
        )

    observed = np.array([counts[digit] for digit in range(10)], dtype=float)
    expected = np.full(10, sample_size / 10, dtype=float)
    chi2, p_value = chisquare(f_obs=observed, f_exp=expected)
    share_zero_or_five = (counts[0] + counts[5]) / sample_size
    z_score = _proportion_z_score(
        proportion=share_zero_or_five,
        baseline=0.2,
        sample_size=sample_size,
    )
    strength = _strength_from_heaping(share_zero_or_five=share_zero_or_five, z_score=z_score)

    plain_english = (
        f"`{field_name}` ends in 0 or 5 for {share_zero_or_five:.0%} of rounded values. "
        "That is more rounded than a flat end-digit pattern would suggest."
        if strength != "unknown"
        else f"`{field_name}` does not show a strong 0/5 end-digit pile-up."
    )

    return SyntheticnessSignal(
        signal_type="end_digit_heaping",
        title=title or f"End-digit heaping check for {field_name}",
        plain_english=plain_english,
        strength=strength,
        dataset=dataset,
        field_name=field_name,
        method="chi-square + 0/5 share",
        sample_size=sample_size,
        score=float(share_zero_or_five),
        benchmark=0.2,
        p_value=float(p_value),
        z_score=float(z_score),
        threshold=0.35,
        observed=f"{share_zero_or_five:.0%} end in 0 or 5",
        expected="About 20% would end in 0 or 5 under a flat end-digit pattern.",
        interpretation=(
            "This is a weak-to-moderate rounding signal. Business reporting conventions can "
            "create the same pattern without fabrication."
        ),
        caveat=_merge_caveats(
            "Rounded operational metrics often cluster on 0 or 5 because analysts or source systems prefer tidy integers.",
            caveat,
        ),
        details={"digit_counts": counts, "chi2": float(chi2)},
    )


def analyze_nice_number_spikes(
    values: Iterable[object],
    *,
    field_name: str,
    title: str | None = None,
    caveat: str | None = None,
    dataset: str = "core_brand_metrics",
) -> SyntheticnessSignal:
    """Check whether values over-cluster on multiples of 10 or 25."""

    integers = _rounded_integers(values)
    sample_size = len(integers)
    if sample_size < HEAPING_MIN_SAMPLE:
        return SyntheticnessSignal(
            signal_type="nice_number_spike",
            title=title or f"Nice-number spike check for {field_name}",
            plain_english=(
                f"Skipped nice-number analysis for `{field_name}` because only {sample_size} "
                "usable rounded values were available."
            ),
            strength="unknown",
            dataset=dataset,
            field_name=field_name,
            method="share of multiples of 10 or 25",
            sample_size=sample_size,
            caveat=_merge_caveats(
                "Small samples can make rounded-number spikes hard to interpret.", caveat
            ),
        )

    nice_count = sum(1 for value in integers if value % 10 == 0 or value % 25 == 0)
    share = nice_count / sample_size
    z_score = _proportion_z_score(
        proportion=share,
        baseline=NICE_NUMBER_BASELINE,
        sample_size=sample_size,
    )
    if z_score >= 3.0:
        strength = "moderate"
    elif z_score >= 2.0:
        strength = "weak"
    else:
        strength = "unknown"

    return SyntheticnessSignal(
        signal_type="nice_number_spike",
        title=title or f"Nice-number spike check for {field_name}",
        plain_english=(
            f"{share:.0%} of rounded `{field_name}` values land on multiples of 10 or 25."
            if strength != "unknown"
            else f"`{field_name}` does not show a strong spike in tidy multiples of 10 or 25."
        ),
        strength=strength,
        dataset=dataset,
        field_name=field_name,
        method="share of multiples of 10 or 25",
        sample_size=sample_size,
        score=float(share),
        benchmark=NICE_NUMBER_BASELINE,
        z_score=float(z_score),
        threshold=0.30,
        observed=f"{nice_count}/{sample_size} rounded values",
        expected="Around 12% under a flat last-two-digit pattern.",
        interpretation=(
            "Clusters on tidy anchors can reflect hand-rounding or reporting conventions, but they "
            "are not proof that numbers were fabricated."
        ),
        caveat=_merge_caveats(
            "Bounded, estimated, or target-based business metrics often prefer tidy anchors such as 10, 25, 50, or 100.",
            caveat,
        ),
        details={"nice_count": nice_count},
    )


def analyze_correlation_sanity(
    frame: pd.DataFrame,
    *,
    title: str = "Correlation sanity summary",
    dataset: str = "core_brand_metrics",
) -> list[SyntheticnessSignal]:
    """Summarize the strongest correlations across major numeric fields."""

    numeric = frame.select_dtypes(include=["number"]).dropna(axis=1, how="all")
    numeric = numeric.loc[:, numeric.nunique(dropna=True) > 1]
    sample_size = len(numeric)
    if numeric.shape[1] < 2 or sample_size < 5:
        return [
            SyntheticnessSignal(
                signal_type="correlation_sanity",
                title=title,
                plain_english="Skipped correlation sanity checks because too few numeric fields were available.",
                strength="unknown",
                dataset=dataset,
                sample_size=sample_size,
                caveat="Correlation summaries need several numeric fields with enough variation.",
            )
        ]

    corr = numeric.corr(numeric_only=True)
    pairs: list[tuple[str, str, float]] = []
    columns = list(corr.columns)
    for left_index, left in enumerate(columns):
        for right in columns[left_index + 1 :]:
            pairs.append((left, right, float(corr.loc[left, right])))
    pairs.sort(key=lambda item: abs(item[2]), reverse=True)
    top_pairs = pairs[:3]

    signals = [
        SyntheticnessSignal(
            signal_type="correlation_sanity",
            title=title,
            plain_english=(
                "Major numeric fields show the following strongest correlations: "
                + ", ".join(
                    f"{left} vs {right} ({corr_value:.2f})" for left, right, corr_value in top_pairs
                )
            ),
            strength="unknown",
            dataset=dataset,
            method="pearson correlation",
            sample_size=sample_size,
            details={"top_pairs": top_pairs},
            caveat=(
                "High correlation can be entirely expected when two fields describe the same economic concept or share a formula."
            ),
        )
    ]

    for left, right, corr_value in top_pairs:
        strength = "weak" if abs(corr_value) >= 0.97 else "unknown"
        signals.append(
            SyntheticnessSignal(
                signal_type="correlation_pair",
                title=f"Correlation check for {left} and {right}",
                plain_english=(
                    f"`{left}` and `{right}` move together with correlation {corr_value:.2f}."
                ),
                strength=strength,
                dataset=dataset,
                field_name=f"{left}::{right}",
                method="pearson correlation",
                sample_size=sample_size,
                score=float(corr_value),
                benchmark=0.0,
                observed=f"{corr_value:.2f}",
                expected="No single expected benchmark; this is contextual.",
                interpretation=(
                    "Very high correlation is only notable if the metrics are not formulaically or economically linked."
                ),
                caveat=(
                    "Correlation is descriptive, not diagnostic. Strong relationships can arise naturally from shared drivers."
                ),
            )
        )

    return signals


def _analyze_benford(
    *,
    digits: list[int],
    domain: list[int],
    expected: list[float],
    signal_type: str,
    title: str,
    field_name: str,
    min_sample_size: int,
    caveat: str | None,
    dataset: str,
) -> SyntheticnessSignal:
    sample_size = len(digits)
    if sample_size < min_sample_size:
        return SyntheticnessSignal(
            signal_type=signal_type,
            title=title,
            plain_english=(
                f"Skipped {signal_type.replace('_', ' ')} for `{field_name}` because "
                f"{sample_size} usable values is too small for a stable read."
            ),
            strength="unknown",
            dataset=dataset,
            field_name=field_name,
            method="Benford chi-square + MAD",
            sample_size=sample_size,
            caveat=_merge_caveats(
                "Benford-style tests become noisy on small samples and bounded business metrics.",
                caveat,
            ),
        )

    observed_counts = np.array([digits.count(value) for value in domain], dtype=float)
    observed_share = observed_counts / sample_size
    mad = float(np.mean(np.abs(observed_share - np.array(expected, dtype=float))))
    chi2, p_value = chisquare(f_obs=observed_counts, f_exp=np.array(expected) * sample_size)

    if p_value < 0.01 and mad >= 0.02:
        strength = "moderate"
    elif p_value < 0.05 or mad >= 0.015:
        strength = "weak"
    else:
        strength = "unknown"

    plain_english = (
        f"`{field_name}` departs from the reference Benford pattern with MAD {mad:.3f}."
        if strength != "unknown"
        else f"`{field_name}` does not show a strong Benford departure in this sample."
    )

    return SyntheticnessSignal(
        signal_type=signal_type,
        title=title,
        plain_english=plain_english,
        strength=strength,
        dataset=dataset,
        field_name=field_name,
        method="Benford chi-square + MAD",
        sample_size=sample_size,
        score=mad,
        benchmark=float(np.mean(expected)),
        p_value=float(p_value),
        threshold=0.015,
        observed=str(count_digits(digits, domain=domain)),
        expected=str({key: round(value, 4) for key, value in zip(domain, expected, strict=True)}),
        interpretation=(
            "Benford divergence is a weak contextual signal here. Treat it as a prompt to inspect provenance, not as evidence of fabrication."
        ),
        caveat=_merge_caveats(
            "Benford assumes naturally occurring, multi-scale numbers; bounded or curated operational metrics can violate that assumption.",
            caveat,
        ),
        details={
            "mad": mad,
            "digit_counts": count_digits(digits, domain=domain),
            "chi2": float(chi2),
        },
    )


def _expected_first_digit_benford() -> list[float]:
    return [math.log10(1 + 1 / digit) for digit in range(1, 10)]


def _expected_first_two_digit_benford() -> list[float]:
    return [math.log10(1 + 1 / digits) for digits in range(10, 100)]


def _extract_significant_digits(value: object, *, count: int) -> int | None:
    number = _coerce_numeric(value)
    if number is None or number <= 0:
        return None

    lower = 10 ** (count - 1)
    upper = lower * 10
    scaled = abs(number)
    while scaled < lower:
        scaled *= 10
    while scaled >= upper:
        scaled /= 10
    return int(scaled)


def _coerce_numeric(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _rounded_integers(values: Iterable[object]) -> list[int]:
    integers: list[int] = []
    for value in values:
        coerced = _coerce_numeric(value)
        if coerced is None:
            continue
        integers.append(int(round(coerced)))
    return integers


def _strength_from_heaping(*, share_zero_or_five: float, z_score: float) -> str:
    if share_zero_or_five >= 0.5 and z_score >= 3.0:
        return "moderate"
    if share_zero_or_five >= 0.35 and z_score >= 2.0:
        return "weak"
    return "unknown"


def _proportion_z_score(*, proportion: float, baseline: float, sample_size: int) -> float:
    variance = baseline * (1 - baseline) / sample_size
    if variance <= 0:
        return 0.0
    return float((proportion - baseline) / math.sqrt(variance))


def _merge_caveats(base: str, extra: str | None) -> str:
    if extra:
        return f"{base} {extra}"
    return base


__all__ = [
    "BENFORD_MIN_ORDER_SPAN",
    "FIRST_DIGIT_MIN_SAMPLE",
    "FIRST_TWO_DIGIT_MIN_SAMPLE",
    "HEAPING_MIN_SAMPLE",
    "NICE_NUMBER_BASELINE",
    "analyze_correlation_sanity",
    "analyze_end_digit_heaping",
    "analyze_first_digit_benford",
    "analyze_first_two_digit_benford",
    "analyze_nice_number_spikes",
    "count_digits",
    "extract_end_digits",
    "extract_first_digits",
    "extract_first_two_digits",
]
