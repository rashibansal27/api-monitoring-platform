"""
Geographic behaviour tracker for client-level anomaly signals.

Computes Shannon entropy over a client's geographic distribution.
A sudden increase in geo_entropy signals that a merchant is transacting
from unusual or new locations — a potential fraud or account-takeover signal.

Shannon entropy: H = -sum(p_i * log2(p_i))
  Low entropy  → concentrated in few known locations (normal)
  High entropy → spread across many countries (suspicious)
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def compute_geo_entropy(geo_codes: list[str]) -> float:
    """
    Compute Shannon entropy (base-2) over a list of geo codes.

    Args:
        geo_codes: List of country or city codes (e.g. ["IN", "IN", "SG", "IN"])

    Returns:
        Entropy in bits. 0.0 for empty or single-location list.

    Examples:
        ["IN", "IN", "IN"]              → 0.0   (all same)
        ["IN", "SG"]                    → 1.0   (50/50 split)
        ["IN", "SG", "AE", "US"]        → 2.0   (perfectly uniform across 4)
    """
    if not geo_codes:
        return 0.0

    counts = Counter(geo_codes)
    total = len(geo_codes)
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return round(entropy, 4)


@dataclass
class GeoProfile:
    """Snapshot of a merchant's geographic distribution in one window."""
    merchant_id: str
    unique_countries: list[str]
    unique_cities: list[str]
    country_distribution: dict[str, int]    # country_code → request count
    city_distribution: dict[str, int]
    geo_entropy: float
    is_new_country_seen: bool               # True if a country not in baseline appeared
    new_countries: list[str]                # Which new countries


def build_geo_profile(
    merchant_id: str,
    country_codes: list[str],
    city_codes: list[str],
    known_countries: set[str] | None = None,
) -> GeoProfile:
    """
    Build a GeoProfile from a list of country/city codes observed in a window.

    Args:
        merchant_id: Merchant identifier.
        country_codes: List of country codes from events in this window.
        city_codes: List of city names from events in this window.
        known_countries: Set of country codes seen in the merchant's history.
                         If None, new-country detection is skipped.
    """
    country_dist = dict(Counter(country_codes))
    city_dist = dict(Counter(city_codes))

    new_countries: list[str] = []
    if known_countries is not None:
        new_countries = [c for c in country_dist if c not in known_countries]

    return GeoProfile(
        merchant_id=merchant_id,
        unique_countries=list(country_dist.keys()),
        unique_cities=list(city_dist.keys()),
        country_distribution=country_dist,
        city_distribution=city_dist,
        geo_entropy=compute_geo_entropy(country_codes),
        is_new_country_seen=bool(new_countries),
        new_countries=new_countries,
    )


def detect_geo_deviation(
    current_entropy: float,
    baseline_entropy: float,
    baseline_std: float,
    spike_threshold: float = 2.0,
) -> tuple[bool, float]:
    """
    Detect whether the current geo entropy is anomalously high.

    Args:
        current_entropy: Entropy measured in the current window.
        baseline_entropy: Historical mean entropy for this merchant.
        baseline_std: Historical std of entropy.
        spike_threshold: Number of std deviations above baseline to flag.

    Returns:
        (is_anomaly, deviation_score)
        deviation_score is the Z-score equivalent.
    """
    if baseline_std < 0.01:
        # Baseline is very stable — any deviation is notable
        deviation = abs(current_entropy - baseline_entropy)
        return deviation > 0.5, deviation

    z = (current_entropy - baseline_entropy) / baseline_std
    return z > spike_threshold, round(z, 4)
