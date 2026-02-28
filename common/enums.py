from enum import Enum


class DataSource(str, Enum):
    """Origin of an ingested log event."""
    ELASTICSEARCH = "elasticsearch"
    PROMETHEUS = "prometheus"
    KAFKA = "kafka"
    MOCK = "mock"


class ErrorType(str, Enum):
    """Top-level error classification."""
    NONE = "none"                   # Successful transaction
    TECHNICAL = "technical"         # Infra, network, timeout, HTTP 5xx
    BUSINESS = "business"           # Domain error — often inside HTTP 200


class TechnicalErrorCategory(str, Enum):
    """Sub-categories of technical errors."""
    TIMEOUT = "timeout"
    HTTP_5XX = "http_5xx"
    CONNECTION_REFUSED = "connection_refused"
    DATABASE = "database"
    INFRASTRUCTURE = "infrastructure"
    UPSTREAM_FAILURE = "upstream_failure"
    CIRCUIT_BREAKER = "circuit_breaker"
    UNKNOWN = "unknown"


class BusinessErrorCategory(str, Enum):
    """
    Sub-categories of business errors.
    These typically arrive as HTTP 200 with a domain error code in the response body.
    """
    INSUFFICIENT_FUNDS = "insufficient_funds"
    OTP_FAILURE = "otp_failure"
    AML_REJECTION = "aml_rejection"
    LIMIT_EXCEEDED = "limit_exceeded"
    INVALID_ACCOUNT = "invalid_account"
    INVALID_VPA = "invalid_vpa"               # UPI virtual payment address
    ACCOUNT_FROZEN = "account_frozen"
    BENEFICIARY_BANK_DOWN = "beneficiary_bank_down"
    DUPLICATE_TRANSACTION = "duplicate_transaction"
    TRANSACTION_NOT_PERMITTED = "transaction_not_permitted"
    INVALID_IFSC = "invalid_ifsc"
    DEBIT_FREEZE = "debit_freeze"
    CREDIT_FREEZE = "credit_freeze"
    RISK_DECLINED = "risk_declined"
    EXPIRED_TOKEN = "expired_token"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    """Alert and anomaly severity levels."""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MetricName(str, Enum):
    """Named metrics tracked in the time-series store."""
    TPS = "tps"
    SUCCESS_RATE = "success_rate"
    TECHNICAL_ERROR_RATE = "technical_error_rate"
    BUSINESS_ERROR_RATE = "business_error_rate"
    P50_LATENCY_MS = "p50_latency_ms"
    P95_LATENCY_MS = "p95_latency_ms"
    P99_LATENCY_MS = "p99_latency_ms"
    AVG_LATENCY_MS = "avg_latency_ms"
    TXN_VOLUME = "txn_volume"
    AVG_TXN_AMOUNT = "avg_txn_amount"
    FAILURE_RATIO = "failure_ratio"


class AnomalyType(str, Enum):
    """Types of anomalies the detection engine can raise."""
    ERROR_RATE_SPIKE = "error_rate_spike"
    TECHNICAL_ERROR_SPIKE = "technical_error_spike"
    BUSINESS_ERROR_SPIKE = "business_error_spike"
    TPS_SPIKE = "tps_spike"
    TPS_DROP = "tps_drop"
    LATENCY_INCREASE = "latency_increase"
    CLIENT_BURST = "client_burst"
    CLIENT_SILENCE = "client_silence"
    AMOUNT_ANOMALY = "amount_anomaly"
    GEO_DEVIATION = "geo_deviation"
    CLIENT_FAILURE_SPIKE = "client_failure_spike"
    MULTIVARIATE = "multivariate"               # Isolation Forest output


class DetectorName(str, Enum):
    """Anomaly detector identifiers."""
    ZSCORE = "zscore"
    EWMA = "ewma"
    ROLLING_PERCENTILE = "rolling_percentile"
    ISOLATION_FOREST = "isolation_forest"
    BURST_SILENCE = "burst_silence"
    SEASONAL_BASELINE = "seasonal_baseline"


class AlertStatus(str, Enum):
    """Lifecycle state of an alert."""
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"           # Cooldown / dedup suppression


class WindowSize(int, Enum):
    """Standard aggregation window sizes in seconds."""
    ONE_MIN = 60
    FIVE_MIN = 300
    FIFTEEN_MIN = 900
    ONE_HOUR = 3600
    ONE_DAY = 86400
    SEVEN_DAYS = 604800
