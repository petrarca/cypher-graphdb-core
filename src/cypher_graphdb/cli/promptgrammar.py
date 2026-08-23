"""Grammars for the CLI prompt language.

Two small Lark grammars replace the hand-written character scanners that used to
split the prompt line and parse a command's arguments. Both quoting rules --
escapes, nested brackets, separators inside quotes -- are now declarative rather
than four mutable flags in a loop.

Deliberately *not* handled here: matching the leading command tokens. That stays
prefix-based, because the completer parses half-typed input where a grammar
cannot yet know which command is being written.

The language:

    line      := pipeline [ "||" shell_command ]
    pipeline  := segment { "|" segment }
    arglist   := item { "," item }
    item      := value | NAME "=" value
    value     := python literal | bare word
"""

from __future__ import annotations

import builtins
from typing import Any

from cypher_graphdb._extras import require_extra

try:
    from lark import Lark, Token, Transformer, v_args
    from lark.exceptions import LarkError
except ModuleNotFoundError as exc:  # pragma: no cover - depends on install shape
    raise require_extra("cli", exc) from exc

# ── Line splitting ────────────────────────────────────────────────────────────
# Segments are kept as raw text: the caller still needs the original spelling to
# match command tokens against it.
_LINE_GRAMMAR = r"""
start     : pipeline (SHELLSEP shell)?
pipeline  : segment (PIPE segment)*
segment   : CHUNK*
shell     : /.+/s

SHELLSEP  : "||"
PIPE      : "|"
CHUNK     : /'(\\.|[^'\\])*'/     // single-quoted, escapes respected
          | /"(\\.|[^"\\])*"/     // double-quoted
          | /[^|'"]+/             // unquoted run; stops at a quote so the
                                  // alternatives above can claim it, and at a
                                  // separator so the pipeline rule can
"""

# ── Argument lists ────────────────────────────────────────────────────────────
_ARGS_GRAMMAR = r"""
start     : (item (","  item)*)?
?item     : NAME "=" value    -> kwarg
          | value             -> arg
?value    : dict
          | list
          | SQ_STRING
          | DQ_STRING
          | SIGNED_NUMBER
          | BAREWORD
list      : "[" [value ("," value)*] "]"
dict      : "{" [pair ("," pair)*] "}"
pair      : value ":" value

SQ_STRING : /'(\\.|[^'\\])*'/
DQ_STRING : /"(\\.|[^"\\])*"/
NAME      : /[A-Za-z_][A-Za-z0-9_.-]*/
BAREWORD  : /[^,=\[\]{}:'"\s][^,=\[\]{}:]*/

%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
"""


class _LineTransformer(Transformer):
    """Turn a parsed line into (segments, shell_command)."""

    def segment(self, chunks: list[Token]) -> str:
        return "".join(str(c) for c in chunks).strip()

    def shell(self, parts: list[Token]) -> tuple[str, str]:
        return ("__shell__", str(parts[0]).strip())

    def pipeline(self, segments: list[Any]) -> list[str]:
        return [s for s in segments if not isinstance(s, Token)]

    def start(self, children: list[Any]) -> tuple[list[str], str]:
        segments: list[str] = []
        shell = ""
        for child in children:
            if isinstance(child, list):
                segments = child
            elif isinstance(child, tuple) and child and child[0] == "__shell__":
                shell = child[1]
        return segments, shell


class _ArgsTransformer(Transformer):
    """Turn a parsed argument list into (args, kwargs).

    Positional arguments keep their **raw source text**: a node reference such
    as ``844424930131970`` must stay a string, and a quoted positional keeps its
    quotes. Only keyword values (and the elements of a collection inside one)
    are evaluated as Python literals.
    """

    def __init__(self, source: str) -> None:
        super().__init__()
        self._source = source

    def start(self, items: list[Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args: list[Any] = []
        kwargs: dict[str, Any] = {}
        for item in items:
            if isinstance(item, _Kwarg):
                kwargs[item.key] = item.value
            else:
                args.append(item)
        return tuple(args), kwargs

    @v_args(inline=True)
    def kwarg(self, name: Token, value: Any) -> _Kwarg:
        return _Kwarg(str(name), value)

    @v_args(meta=True)
    def arg(self, meta, children: list[Any]) -> Any:  # noqa: ARG002 - value comes from the source slice
        return self._source[meta.start_pos : meta.end_pos].strip()

    def list(self, children: list[Any]) -> list[Any]:
        return list(children)

    def dict(self, children: builtins.list[Any]) -> dict[Any, Any]:
        # `builtins.list` is required here: the `list` method defined just above
        # shadows the builtin `list` name for the rest of this class body.
        return dict(children)

    @v_args(inline=True)
    def pair(self, key: Any, value: Any) -> tuple[Any, Any]:
        return key, value

    def SQ_STRING(self, token: Token) -> str:  # noqa: N802 - Lark terminal callback
        return _literal(str(token))

    def DQ_STRING(self, token: Token) -> str:  # noqa: N802 - Lark terminal callback
        return _literal(str(token))

    def SIGNED_NUMBER(self, token: Token) -> Any:  # noqa: N802 - Lark terminal callback
        return _literal(str(token))

    def BAREWORD(self, token: Token) -> Any:  # noqa: N802 - Lark terminal callback
        return _literal(str(token).strip())

    def NAME(self, token: Token) -> Token:  # noqa: N802 - Lark terminal callback
        return token


class _Kwarg:
    """A key/value pair produced by the argument grammar."""

    __slots__ = ("key", "value")

    def __init__(self, key: str, value: Any) -> None:
        self.key = key
        self.value = value


def _literal(text: str) -> Any:
    """Evaluate a Python literal, falling back to the raw text.

    Mirrors the previous behaviour, where values went through
    ``ast.literal_eval`` and non-literals (bare words such as a label name)
    stayed strings.
    """
    import ast

    try:
        return ast.literal_eval(text)
    except ValueError, SyntaxError:
        return text


_line_parser = Lark(_LINE_GRAMMAR, parser="earley", start="start")
_args_parser = Lark(_ARGS_GRAMMAR, parser="earley", start="start", propagate_positions=True)


def split_line(line: str) -> tuple[list[str], str]:
    """Split a prompt line into pipeline segments and a shell command.

    Separators inside quoted sections are not split points, and escapes are
    preserved verbatim -- unescaping is the argument parser's job.

    Args:
        line: The raw prompt line.

    Returns:
        Tuple of (segments, shell_command). ``shell_command`` is "" when the
        line has no ``||`` part.
    """
    if not line or not line.strip():
        return [""], ""

    try:
        tree = _line_parser.parse(line)
    except LarkError:
        # An unparseable line is still a single command; the command layer
        # reports the problem with better context than a parser error would.
        return [line.strip()], ""

    segments, shell = _LineTransformer().transform(tree)
    return (segments or [""]), shell


def parse_args(text: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Parse an argument list into positional and keyword arguments.

    Args:
        text: The text following a command's tokens.

    Returns:
        Tuple of (args, kwargs).
    """
    if not text or not text.strip():
        return (), {}

    try:
        tree = _args_parser.parse(text)
    except LarkError:
        # Half-typed input reaches here from the completer; yield what is
        # unambiguous rather than failing the whole parse.
        return _best_effort_args(text)

    return _ArgsTransformer(text).transform(tree)


def _best_effort_args(text: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Recover args from input the grammar rejects, e.g. a half-typed kwarg."""
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            key, _, value = part.partition("=")
            key, value = key.strip(), value.strip()
            # A kwarg with no value yet -- the user is mid-keystroke -- is
            # dropped rather than bound to "", so it does not silently become a
            # filter that matches nothing.
            if key and value:
                kwargs[key] = _literal(value)
        else:
            args.append(_literal(part))
    return tuple(args), kwargs
