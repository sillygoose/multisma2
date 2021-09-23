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
