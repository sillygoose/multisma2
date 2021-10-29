"""Exceptions used in multisma2."""

from enum import Enum, auto


class NormalCompletion(Exception):
    """Normal completion, no errors."""


class AbnormalCompletion(Exception):
    """Abnormal completion, error or exception detected."""


class FailedInitialization(Exception):
    """multisma2 initialization failed."""


class TerminateSignal(Exception):
    """SIGTERM."""


class InfluxDBWriteError(Exception):
    """InfluxDB write error."""


class InfluxDBQueryError(Exception):
    """InfluxDB query error."""


class InfluxDBBucketError(Exception):
    """InfluxDB bucket error."""


class InfluxDBFormatError(Exception):
    """Illegal or unsupported database output format."""


class InfluxDBInitializationError(Exception):
    """InfluxDB is not properly initialized."""


class InternalError(Exception):
    """Unexpected/inconsistant state."""


class SmaException(Exception, Enum):
    """Base exception of the pysma library."""
    PASSWORD_REQUIRED = auto()
    PASSWORD_TOO_LONG = auto()
    BAD_USER_TYPE = auto()
    NO_SESSION = auto()
    ERR_RETURNED = auto()
    NO_RESULT = auto()
    UNEXPECTED_BODY = auto()
    SESSION_ID_EXPECTED = auto()
    MAX_SESSIONS = auto()
    START_SESSION = auto()
