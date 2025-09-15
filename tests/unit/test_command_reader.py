from io import StringIO
from pathlib import Path

from cypher_graphdb.command_reader import CommandReader, FileCommandReader


def test_single_line_commands():
    data = """
    # comment line
    MATCH (n) RETURN n;
    // a comment line using double slash
    MATCH (m) RETURN m;MATCH (x) RETURN x;
    """
    # The line starting with '//' is now also treated as a comment and skipped.
    sio = StringIO(data)

    cmds = list(CommandReader(sio))

    assert cmds == [
        "MATCH (n) RETURN n",
        "MATCH (m) RETURN m",
        "MATCH (x) RETURN x",
    ]


def test_multi_line_command():
    data = """
    MATCH (n)
    WHERE n.age > 30
    RETURN n.name, n.age
    ;
    """
    sio = StringIO(data)

    cmds = list(CommandReader(sio))

    assert cmds == ["MATCH (n)\nWHERE n.age > 30\nRETURN n.name, n.age"]


def test_semicolon_inside_quotes():
    data = """
    MATCH (n {text: \"a;b;c\"}) RETURN n;
    """
    sio = StringIO(data)

    cmds = list(CommandReader(sio))

    assert cmds == ['MATCH (n {text: "a;b;c"}) RETURN n']


def test_unterminated_last_statement_ignored():
    data = """
    MATCH (n) RETURN n
    # missing semicolon means it should be ignored
    """
    sio = StringIO(data)

    cmds = list(CommandReader(sio))

    assert cmds == []


def test_multiple_statements_same_line():
    data = """MATCH (a) RETURN a;MATCH (b) RETURN b;   MATCH(c) RETURN c;\n"""
    sio = StringIO(data)

    cmds = list(CommandReader(sio))

    assert cmds == [
        "MATCH (a) RETURN a",
        "MATCH (b) RETURN b",
        "MATCH(c) RETURN c",
    ]


def test_semicolon_with_trailing_spaces():
    data = "MATCH (n) RETURN n;     \n"
    sio = StringIO(data)
    cmds = list(CommandReader(sio))
    assert cmds == ["MATCH (n) RETURN n"]


def test_internal_comment_lines_preserved():
    data = """
    MATCH (n)
    # inner comment
    // second comment
    WHERE n.age > 10
    RETURN n;
    """
    sio = StringIO(data)
    cmds = list(CommandReader(sio))
    assert cmds == [("MATCH (n)\n# inner comment\n// second comment\nWHERE n.age > 10\nRETURN n")]


def test_empty_file():
    sio = StringIO("")
    cmds = list(CommandReader(sio))
    assert cmds == []


def test_consecutive_semicolons():
    data = "MATCH (n) RETURN n;; MATCH (m) RETURN m;"
    sio = StringIO(data)
    cmds = list(CommandReader(sio))
    assert cmds == ["MATCH (n) RETURN n", "MATCH (m) RETURN m"]


def test_escaped_quote_and_semicolon_inside_string():
    data = r"""
    CREATE (n {text: "value\";still"});
    """
    sio = StringIO(data)
    cmds = list(CommandReader(sio))
    assert cmds == ['CREATE (n {text: "value\\";still"})']


def test_file_command_reader_basic(tmp_path: Path):
    script = tmp_path / "script.cypher"
    script.write_text(
        """
        # leading comment should be skipped
        MATCH (n)
        RETURN n;
        // another comment
        MATCH (m {text: "a;b"}) RETURN m;
        """,
        encoding="utf-8",
    )

    with FileCommandReader(str(script)) as cr:
        cmds = list(cr)

    assert cmds == [
        "MATCH (n)\nRETURN n",
        'MATCH (m {text: "a;b"}) RETURN m',
    ]


def test_file_command_reader_ignores_unterminated(tmp_path: Path):
    script = tmp_path / "unterminated.cypher"
    script.write_text(
        """
        CREATE (n);
        CREATE (m)
        # missing semicolon means previous line is ignored
        """,
        encoding="utf-8",
    )

    with FileCommandReader(str(script)) as cr:
        cmds = list(cr)

    assert cmds == ["CREATE (n)"]
