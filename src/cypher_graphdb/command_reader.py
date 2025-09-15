"""Iterate over semicolon terminated CLI command statements.

Features:
* Multi-line commands (explicit `;` terminator required for every statement).
* Skip empty lines and lines whose first non-space char is `#`.
* Ignores semicolons inside single or double quoted strings.
* Yields statements without the trailing semicolon, stripped.

Limitations (kept intentionally lightweight):
* Only basic quote state tracking; no triple quotes.
* Quote escape detection handles backslash escapes (\\').
* Unterminated final statement (missing `;`) is discarded silently.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TextIO


class CommandReader(Iterable[str]):
    """Iterates over commands from an existing TextIO stream.

    Caller is responsible for opening and closing the stream.
    Use `FileCommandReader` for a path-based convenience wrapper.
    """

    def __init__(self, fd: TextIO):
        self._fd = fd

    def __iter__(self) -> Iterator[str]:
        """Yield each complete statement (no trailing ';')."""
        acc = ""
        in_single = in_double = False
        for raw_line in self._fd:  # type: ignore[arg-type]
            line = raw_line.rstrip("\n")
            stripped = line.strip()
            if not stripped and not acc:
                continue
            if (stripped.startswith("#") or stripped.startswith("//")) and not acc:
                continue
            acc += ("\n" if acc else "") + line

            i = 0
            while i < len(acc):
                ch = acc[i]
                if ch == "'" and not in_double and not _is_escaped(acc, i):
                    in_single = not in_single
                elif ch == '"' and not in_single and not _is_escaped(acc, i):
                    in_double = not in_double
                elif ch == ";" and not in_single and not in_double:
                    before = acc[:i]
                    after = acc[i + 1 :]
                    stmt = _normalize_indentation(before).strip()
                    if stmt:
                        yield stmt
                    acc = after.lstrip("\n")
                    in_single = in_double = False
                    i = -1
                i += 1

    def _skip_line(self, *_):  # noqa: D401
        return False


class FileCommandReader(CommandReader):
    """File path based reader with context manager support."""

    def __init__(self, path: str):
        self._path = path
        self._file: TextIO | None = None
        super().__init__(fd=None)  # type: ignore[arg-type]

    def __enter__(self):
        self._file = open(self._path, encoding="utf-8")  # noqa: P201
        self._fd = self._file
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        if self._file:
            self._file.close()


def _is_escaped(line: str, pos: int) -> bool:
    backslashes = 0
    j = pos - 1
    while j >= 0 and line[j] == "\\":
        backslashes += 1
        j -= 1
    return backslashes % 2 == 1


def _normalize_indentation(text: str) -> str:
    lines = text.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    indents = [len(line) - len(line.lstrip()) for line in lines if line.strip()]
    if not indents:
        return ""
    min_indent = min(indents)
    return "\n".join(line[min_indent:] if line.strip() else "" for line in lines)
