"""Characterization tests for the CLI prompt parser.

The prompt language is hand-parsed in three places -- ``partition_str`` /
``split_str`` for the ``||`` and ``|`` splits, ``_parse_cmd_with_map`` for
longest-prefix action matching, and ``split_into_args_kwargs`` for the argument
scanner. None of it was covered by tests.

These tests pin the observable behaviour of that language so it can be changed
safely:

    line      := pipeline [ "||" shell_cmd ]
    pipeline  := command { "|" command }
    command   := <multi-word token, longest-prefix match> [ arglist ]
    arglist   := item { "," item }
    item      := value | key "=" value
    value     := python literal (ast.literal_eval) | bare word
"""

import pytest

from cypher_graphdb.cli import command_map
from cypher_graphdb.cli.promptparser import PromptParser

DEFAULT_ACTION = "execute_cypher"


@pytest.fixture
def parser() -> PromptParser:
    """A parser wired to the real command map, as the CLI builds it."""
    return PromptParser(command_map.CMD_MAP, DEFAULT_ACTION)


def _only(parser: PromptParser, line: str):
    """Parse a single-command line and return that command."""
    result = parser.parse_prompt(line)
    assert len(result.cmds) == 1, f"expected one command, got {len(result.cmds)}"
    return result.cmds[0]


# ── Action resolution ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("line", "action"),
    [
        ("labels", "dump_labels"),
        ("graphs", "dump_graphs"),
        ("indexes", "dump_indexes"),
        ("backends", "dump_backends"),
        ("help", "help"),
        ("exit", "exit"),
    ],
)
def test_single_token_commands_resolve_to_their_action(parser, line, action):
    assert _only(parser, line).action == action


@pytest.mark.parametrize(
    ("line", "action", "cmd"),
    [
        ("use graph mygraph", "use_graph", "use graph"),
        ("create graph staging", "create_graph", "create graph"),
        ("drop graph old", "drop_graph", "drop graph"),
        ("graph exists demo", "graph_exists", "graph exists"),
        ("fetch nodes Person", "fetch_nodes", "fetch nodes"),
        ("fetch edges WORKS_FOR", "fetch_edges", "fetch edges"),
    ],
)
def test_multi_word_tokens_resolve_by_longest_prefix(parser, line, action, cmd):
    parsed = _only(parser, line)
    assert parsed.action == action
    assert parsed.cmd == cmd


def test_longest_prefix_wins_over_shorter_overlapping_token(parser):
    """'create linked node' must not be truncated to 'create node'."""
    assert _only(parser, "create linked node Person").cmd == "create linked node"
    assert _only(parser, "create node Person").cmd == "create node"


def test_unrecognised_input_falls_through_to_cypher(parser):
    parsed = _only(parser, "MATCH (n) RETURN n")
    assert parsed.action == "execute_cypher"
    assert parsed.cmd == "MATCH (n) RETURN n"


def test_unknown_command_is_treated_as_cypher_not_an_error(parser):
    assert _only(parser, "unknowncommand foo bar").action == "execute_cypher"


@pytest.mark.parametrize("line", ["", "   "])
def test_blank_input_yields_an_empty_cypher_command(parser, line):
    parsed = _only(parser, line)
    assert parsed.action == "execute_cypher"
    assert parsed.cmd == ""


# ── Arguments and keyword arguments ───────────────────────────────────────────


def test_positional_arguments(parser):
    parsed = _only(parser, "create node Person")
    assert list(parsed.args) == ["Person"]
    assert parsed.kwargs == {}


def test_keyword_arguments_are_literal_evaluated(parser):
    parsed = _only(parser, "fetch nodes flag=True, ratio=1.5, none=None, count=3")
    assert parsed.kwargs == {"flag": True, "ratio": 1.5, "none": None, "count": 3}


def test_mixed_positional_and_keyword_arguments(parser):
    parsed = _only(parser, "create node Person, name='Alice', age=30")
    assert list(parsed.args) == ["Person"]
    assert parsed.kwargs == {"name": "Alice", "age": 30}


def test_collection_values_survive_their_internal_commas(parser):
    parsed = _only(parser, "fetch nodes tags=[1,2,3]")
    assert parsed.kwargs == {"tags": [1, 2, 3]}


def test_dict_values_survive_their_internal_commas_and_colons(parser):
    parsed = _only(parser, "fetch nodes props={'a':1,'b':2}")
    assert parsed.kwargs == {"props": {"a": 1, "b": 2}}


def test_quoted_value_may_contain_the_argument_separator(parser):
    parsed = _only(parser, "create node Person, note='a,b,c'")
    assert parsed.kwargs == {"note": "a,b,c"}


def test_quoted_value_may_contain_path_separators(parser):
    parsed = _only(parser, "create node Person, path='/usr/local/bin'")
    assert parsed.kwargs == {"path": "/usr/local/bin"}


def test_escaped_single_quote_inside_a_single_quoted_value(parser):
    r"""``name='O\'Brien'`` must yield the unescaped text, not the raw quotes."""
    parsed = _only(parser, r"create node Person, name='O\'Brien'")
    assert parsed.kwargs == {"name": "O'Brien"}


def test_escaped_double_quote_inside_a_double_quoted_value(parser):
    r"""``name="say \"hi\""`` must yield the unescaped text."""
    parsed = _only(parser, r'create node Person, name="say \"hi\""')
    assert parsed.kwargs == {"name": 'say "hi"'}


# ── Pipelines ─────────────────────────────────────────────────────────────────


def test_pipeline_splits_into_successive_commands(parser):
    result = parser.parse_prompt("labels | tree")
    assert [c.action for c in result.cmds] == ["dump_labels", "graph_to_tree"]


def test_pipeline_of_three_commands(parser):
    result = parser.parse_prompt("fetch nodes Person | table | json")
    assert [c.action for c in result.cmds] == ["fetch_nodes", "format_output", "format_output"]


def test_cypher_may_start_a_pipeline(parser):
    result = parser.parse_prompt("MATCH (n) RETURN n | tree")
    assert [c.action for c in result.cmds] == ["execute_cypher", "graph_to_tree"]


def test_command_position_is_tracked_across_a_pipeline(parser):
    result = parser.parse_prompt("labels | tree")
    assert result.cmds[0].is_firstcmd()
    assert result.cmds[-1].is_lastcmd()


def test_single_command_is_both_first_and_last(parser):
    parsed = _only(parser, "labels")
    assert parsed.is_singlecmd()


# ── Shell escape ──────────────────────────────────────────────────────────────


def test_double_pipe_separates_a_shell_command(parser):
    result = parser.parse_prompt("labels || grep Person")
    assert result.shell_cmd == "grep Person"
    assert [c.action for c in result.cmds] == ["dump_labels"]


def test_shell_command_is_not_split_as_a_pipeline(parser):
    result = parser.parse_prompt("graphs || wc -l")
    assert result.shell_cmd == "wc -l"
    assert len(result.cmds) == 1


def test_absent_shell_command_is_empty(parser):
    assert parser.parse_prompt("labels").shell_cmd == ""


# ── Partial input (the completer parses incomplete lines) ─────────────────────


def test_command_with_trailing_space_parses(parser):
    """completer.py calls parse_cmd() on the text before the cursor."""
    parsed = parser.parse_cmd("fetch nodes ")
    assert parsed.action == "fetch_nodes"
    assert not parsed.args
