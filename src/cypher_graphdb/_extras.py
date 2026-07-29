"""Optional-dependency guards.

The library core is deliberately free of CLI dependencies: importing
``cypher_graphdb`` must not pull in ``typer``, ``rich``, ``prompt_toolkit`` or
``art``. Those live in the ``cli`` extra.

When a CLI entry point is reached without that extra installed, the failure
should name the fix rather than surface a bare ``ModuleNotFoundError`` for a
transitive package the user never heard of.
"""

from __future__ import annotations

DISTRIBUTION = "cypher-graphdb"


def require_extra(extra: str, exc: ModuleNotFoundError) -> ModuleNotFoundError:
    """Build a ModuleNotFoundError naming the extra that provides the module.

    Args:
        extra: Name of the optional-dependency group (e.g. ``"cli"``).
        exc: The original import failure, kept as the cause.

    Returns:
        A ModuleNotFoundError carrying an actionable message. Raise it from the
        original: ``raise require_extra("cli", exc) from exc``.
    """
    missing = exc.name or "a required package"
    return ModuleNotFoundError(
        f"{DISTRIBUTION} needs the '{extra}' extra for this feature "
        f"(missing: {missing}).\n\n"
        f"    pip install '{DISTRIBUTION}[{extra}]'\n"
        f"    uv add '{DISTRIBUTION}[{extra}]'\n"
    )
