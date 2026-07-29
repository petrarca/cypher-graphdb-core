"""Tests for the Lark grammars behind the CLI prompt language.

These cover the two jobs the hand-written character scanners used to do:
splitting a line into pipeline segments plus a shell command, and parsing a
command's argument list. Quoting, escaping and nested collections are the
interesting cases -- separators inside them must not split.
"""

import pytest

from cypher_graphdb.cli.promptgrammar import parse_args, split_line

# ── Line splitting ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("line", "segments", "shell"),
    [
        ("labels", ["labels"], ""),
        ("labels | tree", ["labels", "tree"], ""),
        ("fetch nodes Person | table | json", ["fetch nodes Person", "table", "json"], ""),
        ("MATCH (n) RETURN n | tree", ["MATCH (n) RETURN n", "tree"], ""),
        ("labels || grep Person", ["labels"], "grep Person"),
        ("a | b || c | d", ["a", "b"], "c | d"),
        ("", [""], ""),
        ("   ", [""], ""),
    ],
)
def test_split_line(line, segments, shell):
    assert split_line(line) == (segments, shell)


@pytest.mark.parametrize(
    ("line", "segments"),
    [
        (r"create node P, name='a|b'", [r"create node P, name='a|b'"]),
        (r'create node P, name="x||y"', [r'create node P, name="x||y"']),
        (r"create node P, name='a|b' | tree", [r"create node P, name='a|b'", "tree"]),
    ],
)
def test_separators_inside_quotes_do_not_split(line, segments):
    """A pipe inside a quoted value is data, not a pipeline separator."""
    assert split_line(line)[0] == segments


def test_escapes_survive_splitting():
    """Splitting is lossless; unescaping belongs to the argument parser."""
    line = r"create node P, name='O\'Brien' | tree"
    assert split_line(line)[0] == [r"create node P, name='O\'Brien'", "tree"]


# ── Argument lists ────────────────────────────────────────────────────────────


def test_positional_and_keyword_arguments():
    args, kwargs = parse_args("arg1, arg2, arg3, opt1=4711, opt2='val', opt3=['1',2],opt4={'f1':'a','f2':10}")
    assert args == ("arg1", "arg2", "arg3")
    assert kwargs == {
        "opt1": 4711,
        "opt2": "val",
        "opt3": ["1", 2],
        "opt4": {"f1": "a", "f2": 10},
    }


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("flag=True", {"flag": True}),
        ("ratio=1.5", {"ratio": 1.5}),
        ("none=None", {"none": None}),
        ("count=-3", {"count": -3}),
        ("name='Alice'", {"name": "Alice"}),
        ('name="Alice"', {"name": "Alice"}),
        ("label=Person", {"label": "Person"}),  # bare word stays a string
    ],
)
def test_values_are_literal_evaluated(text, expected):
    assert parse_args(text)[1] == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("note='a,b,c'", {"note": "a,b,c"}),
        ("tags=[1,2,3]", {"tags": [1, 2, 3]}),
        ("props={'a':1,'b':2}", {"props": {"a": 1, "b": 2}}),
        ("path='/usr/local/bin'", {"path": "/usr/local/bin"}),
    ],
)
def test_separators_inside_values_do_not_split(text, expected):
    assert parse_args(text)[1] == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (r"name='O\'Brien'", {"name": "O'Brien"}),
        (r'name="say \"hi\""', {"name": 'say "hi"'}),
    ],
)
def test_escaped_quotes_are_unescaped(text, expected):
    assert parse_args(text)[1] == expected


@pytest.mark.parametrize("text", ["", "   "])
def test_empty_argument_list(text):
    assert parse_args(text) == ((), {})


# ── Partial input (the completer parses half-typed lines) ─────────────────────


def test_half_typed_kwarg_is_dropped_not_bound_to_empty():
    """`fetch nodes label=` reaches here while the user is still typing.

    Binding it to "" would turn a half-typed filter into one that matches
    nothing, so the incomplete pair is dropped instead.
    """
    assert parse_args("label=") == ((), {})
    assert parse_args("Person, empty=") == (("Person",), {})
    assert parse_args("a=1, b=") == ((), {"a": 1})


def test_trailing_separator_does_not_raise():
    args, kwargs = parse_args("Person,")
    assert args == ("Person",)


# ── Positional arguments stay raw ─────────────────────────────────────────────


def test_numeric_positional_arguments_stay_strings():
    """Node references are positional and must not become ints.

    `create edge WORKS_FOR, <id>, <id>` passes internal ids positionally, and
    PromptParserCmd.args is typed list[str] -- evaluating them breaks the
    command with a validation error.
    """
    args, kwargs = parse_args("WORKS_FOR, 844424930131970, 844424930131969, since=2020")
    assert args == ("WORKS_FOR", "844424930131970", "844424930131969")
    assert kwargs == {"since": 2020}


def test_quoted_positional_keeps_its_quotes():
    """Only keyword values are literal-evaluated; positionals are verbatim."""
    args, _ = parse_args("a, 'quoted', 42")
    assert args == ("a", "'quoted'", "42")


def test_collection_inside_a_keyword_is_still_evaluated():
    """The raw-positional rule must not disable evaluation of kwarg values."""
    _, kwargs = parse_args("opt3=['1',2], opt4={'f1':'a','f2':10}")
    assert kwargs == {"opt3": ["1", 2], "opt4": {"f1": "a", "f2": 10}}
