"""
Technical error rule tables.

These are pure data structures — no logic here.
The TechnicalErrorClassifier consumes them in order of precedence.

Rule evaluation order:
  1. HTTP status code ranges     (fastest, most reliable)
  2. Response-time timeout check (latency > threshold)
  3. Error-message regex patterns (text from gateway / infra layer)
"""

import re
from dataclasses import dataclass, field

from common.enums import Severity, TechnicalErrorCategory

# ---------------------------------------------------------------------------
# 1. HTTP status code → (category, severity)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StatusCodeRule:
    status_code: int
    category: TechnicalErrorCategory
    severity: Severity
    description: str


STATUS_CODE_RULES: list[StatusCodeRule] = [
    # ── Gateway / proxy errors ──────────────────────────────────────────────
    StatusCodeRule(502, TechnicalErrorCategory.UPSTREAM_FAILURE,  Severity.HIGH,     "Bad Gateway"),
    StatusCodeRule(503, TechnicalErrorCategory.INFRASTRUCTURE,    Severity.HIGH,     "Service Unavailable"),
    StatusCodeRule(504, TechnicalErrorCategory.TIMEOUT,           Severity.HIGH,     "Gateway Timeout"),
    StatusCodeRule(524, TechnicalErrorCategory.TIMEOUT,           Severity.HIGH,     "CDN Origin Timeout"),

    # ── Server errors ───────────────────────────────────────────────────────
    StatusCodeRule(500, TechnicalErrorCategory.HTTP_5XX,          Severity.HIGH,     "Internal Server Error"),
    StatusCodeRule(501, TechnicalErrorCategory.HTTP_5XX,          Severity.MEDIUM,   "Not Implemented"),
    StatusCodeRule(505, TechnicalErrorCategory.HTTP_5XX,          Severity.MEDIUM,   "HTTP Version Not Supported"),
    StatusCodeRule(507, TechnicalErrorCategory.HTTP_5XX,          Severity.CRITICAL, "Insufficient Storage (DB full?)"),
    StatusCodeRule(508, TechnicalErrorCategory.HTTP_5XX,          Severity.HIGH,     "Loop Detected"),

    # ── Client-side technical ────────────────────────────────────────────────
    StatusCodeRule(408, TechnicalErrorCategory.TIMEOUT,           Severity.MEDIUM,   "Request Timeout"),
    StatusCodeRule(429, TechnicalErrorCategory.INFRASTRUCTURE,    Severity.MEDIUM,   "Too Many Requests / Rate Limited"),
    StatusCodeRule(499, TechnicalErrorCategory.TIMEOUT,           Severity.LOW,      "Client Closed Request"),
]

# Fast lookup: status_code → rule
STATUS_CODE_RULE_MAP: dict[int, StatusCodeRule] = {r.status_code: r for r in STATUS_CODE_RULES}

# Range-based fallback: 5xx not in the explicit map
def classify_by_status_range(status_code: int) -> tuple[TechnicalErrorCategory, Severity] | None:
    """Returns (category, severity) for any 5xx not in STATUS_CODE_RULE_MAP."""
    if 500 <= status_code <= 599:
        return TechnicalErrorCategory.HTTP_5XX, Severity.HIGH
    return None


# ---------------------------------------------------------------------------
# 2. Timeout detection via response time
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LatencyTimeoutRule:
    """
    If response_time_ms exceeds threshold AND the event is not already
    classified as a technical error, flag it as a timeout.
    """
    threshold_ms: float
    severity: Severity
    description: str


# Ordered: first match wins
LATENCY_TIMEOUT_RULES: list[LatencyTimeoutRule] = [
    LatencyTimeoutRule(30_000, Severity.CRITICAL, "Response > 30s — hard timeout"),
    LatencyTimeoutRule(10_000, Severity.HIGH,     "Response > 10s — likely timeout"),
    LatencyTimeoutRule( 5_000, Severity.MEDIUM,   "Response > 5s  — slow, possible timeout"),
]


# ---------------------------------------------------------------------------
# 3. Error-message pattern rules
# ---------------------------------------------------------------------------

@dataclass
class ErrorMessageRule:
    pattern: re.Pattern
    category: TechnicalErrorCategory
    severity: Severity
    description: str


def _rule(pattern: str, category: TechnicalErrorCategory, severity: Severity, desc: str) -> ErrorMessageRule:
    return ErrorMessageRule(
        pattern=re.compile(pattern, re.IGNORECASE),
        category=category,
        severity=severity,
        description=desc,
    )


ERROR_MESSAGE_RULES: list[ErrorMessageRule] = [
    # ── Database ─────────────────────────────────────────────────────────────
    _rule(r"(db|database|sql|postgres|mysql|oracle).*(error|fail|down|connect|timeout)",
          TechnicalErrorCategory.DATABASE,      Severity.CRITICAL, "Database error"),
    _rule(r"(deadlock|lock.wait.timeout|too many connections|max_connections)",
          TechnicalErrorCategory.DATABASE,      Severity.HIGH,     "Database contention"),
    _rule(r"(connection pool exhausted|pool timeout)",
          TechnicalErrorCategory.DATABASE,      Severity.HIGH,     "DB connection pool exhausted"),

    # ── Network / connectivity ───────────────────────────────────────────────
    _rule(r"(connection refused|econnrefused|econnreset)",
          TechnicalErrorCategory.CONNECTION_REFUSED, Severity.HIGH, "Connection refused"),
    _rule(r"(network.*unreachable|no route to host|host.*down)",
          TechnicalErrorCategory.CONNECTION_REFUSED, Severity.HIGH, "Network unreachable"),
    _rule(r"(read timeout|write timeout|socket timeout|connect timeout|timed.?out)",
          TechnicalErrorCategory.TIMEOUT,        Severity.HIGH,    "Socket/network timeout"),

    # ── Circuit breaker ──────────────────────────────────────────────────────
    _rule(r"(circuit.?breaker|circuit.*open|fallback.*triggered|hystrix|resilience4j)",
          TechnicalErrorCategory.CIRCUIT_BREAKER, Severity.HIGH,   "Circuit breaker open"),

    # ── Infrastructure ───────────────────────────────────────────────────────
    _rule(r"(out of memory|oom|java.*heap|gc overhead|memory.*exceeded)",
          TechnicalErrorCategory.INFRASTRUCTURE, Severity.CRITICAL, "OOM / memory pressure"),
    _rule(r"(disk.*full|no space left|storage.*exhausted)",
          TechnicalErrorCategory.INFRASTRUCTURE, Severity.CRITICAL, "Disk full"),
    _rule(r"(service.*unavailable|upstream.*unavailable|downstream.*unavailable)",
          TechnicalErrorCategory.INFRASTRUCTURE, Severity.HIGH,     "Dependency unavailable"),
    _rule(r"(ssl.*error|tls.*handshake|certificate.*expired|cert.*invalid)",
          TechnicalErrorCategory.INFRASTRUCTURE, Severity.HIGH,     "TLS/SSL error"),

    # ── Upstream / dependency ────────────────────────────────────────────────
    _rule(r"(upstream.*error|upstream.*timeout|backend.*error|dependency.*fail)",
          TechnicalErrorCategory.UPSTREAM_FAILURE, Severity.HIGH,  "Upstream dependency failure"),
    _rule(r"(rpc.*error|grpc.*unavailable|grpc.*deadline)",
          TechnicalErrorCategory.UPSTREAM_FAILURE, Severity.HIGH,  "RPC/gRPC failure"),
]


# ---------------------------------------------------------------------------
# 4. HTTP status codes that are definitively NOT technical errors
#    (used as a fast-exit guard before pattern matching)
# ---------------------------------------------------------------------------
NON_TECHNICAL_STATUS_CODES: frozenset[int] = frozenset({
    200, 201, 202, 204,     # Success
    301, 302, 304,          # Redirect / not modified
    400, 401, 403, 404,     # Client errors (business / auth, not infra)
    409, 410, 422,          # Conflict / unprocessable — usually business
})
