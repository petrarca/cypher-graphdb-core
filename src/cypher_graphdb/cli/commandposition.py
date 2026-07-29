"""Where a command may appear in a pipeline.

Its own module, with no imports, because both ``promptparser`` (which enforces
the rule) and ``commands.base_command`` (which declares it) need it, and those
two would otherwise import each other.
"""

from enum import StrEnum


class CommandPosition(StrEnum):
    """Where a command may appear in a ``cmd | cmd | cmd`` pipeline.

    A single command is always both first and last, so it satisfies every
    position.
    """

    ANY = "any"
    """Anywhere in the pipeline."""

    FIRST = "first"
    """Only as the first command -- it produces data rather than consuming it."""

    LAST = "last"
    """Only as the last command -- it consumes the pipeline's output."""

    FIRST_OR_LAST = "first_or_last"
    """At either end, but not in the middle."""
