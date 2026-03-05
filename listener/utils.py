"""
Utility helpers for naming, tagging, and metric extraction.
"""

import re
from datetime import datetime
from typing import Any, Dict, Optional


def sanitize_query_name(name: Optional[str]) -> str:
    """Normalize a query name to a safe, consistent identifier."""
    if not name:
        return "unnamed_stream"
    return re.sub(r"[^a-zA-Z0-9_\-.]", "_", name.strip()).lower()


def build_tags(
    domain: str = "",
    owner: str = "",
    criticality: str = "medium",
) -> Dict[str, str]:
    """Build a standard tag dict for metric records."""
    return {
        "domain": domain,
        "owner": owner,
        "criticality": criticality,
    }


def safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely navigate nested dicts: safe_get(d, 'a', 'b') → d['a']['b']."""
    current = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
    return current


def compute_latency_ms(
    processing_time: Optional[datetime],
    event_time_max: Optional[datetime],
) -> Optional[int]:
    """Compute latency = processing_time - event_time_max in ms."""
    if processing_time is None or event_time_max is None:
        return None
    delta = processing_time - event_time_max
    return int(delta.total_seconds() * 1000)
