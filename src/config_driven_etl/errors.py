"""Shared helpers for error messages."""


def error_reason(error: BaseException) -> str:
    """First line of an error's message, or its type name if it is empty.

    Library errors often carry multi-line details; the first line keeps
    one-line messages readable.
    """
    lines = str(error).splitlines()
    return lines[0] if lines and lines[0] else type(error).__name__
