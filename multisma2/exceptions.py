"""Exceptions used in multisma2."""


class NormalCompletion(Exception):
    """Normal completion, no errors."""


class AbnormalCompletion(Exception):
    """Abnormal completion, error or exception detected."""


class FailedInitialization(Exception):
    """multisma2 initialization failed."""


class TerminateSignal(Exception):
    """SIGTERM."""


class SmaException(Exception):
    """Base exception of the pysma library."""
