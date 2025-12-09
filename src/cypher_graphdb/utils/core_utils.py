"""Core utility functions and constants.

This module provides fundamental utility functions including scalar type checking,
unique ID generation, file format resolution, and path manipulation utilities.
"""

import os
import secrets
import string
import uuid
from typing import Final

SCALAR_TYPES: Final[tuple[type, ...]] = (int, float, str, bool)


def is_scalar_type(value: object) -> bool:
    """Returns if an object is a scalar type.

    Checks if the given value is one of the basic scalar types:
    int, float, str, or bool.

    Args:
        value: The object to be checked

    Returns:
        True if the object is a scalar type, False otherwise

    Examples:
        >>> is_scalar_type(42)
        True

        >>> is_scalar_type("hello")
        True

        >>> is_scalar_type([1, 2, 3])
        False

        >>> is_scalar_type({"key": "value"})
        False

        >>> is_scalar_type(None)
        False
    """
    return isinstance(value, SCALAR_TYPES)


def generate_unique_string_id(length: int = 16) -> str:
    """Generate a unique string identifier combining UUID and random characters.

    Creates a unique identifier by combining a UUID prefix with random
    alphanumeric characters to reach the desired length.

    Args:
        length: Total length of the generated ID (default: 16).

    Returns:
        Unique string identifier.

    Examples:
        >>> id1 = generate_unique_string_id(8)
        >>> len(id1)
        8

        >>> id2 = generate_unique_string_id(20)
        >>> len(id2)
        20

        >>> id3 = generate_unique_string_id()
        >>> len(id3)
        16

        >>> # IDs should be unique
        >>> id1 != id2 != id3
        True
    """
    part1 = length // 2
    part2 = length - part1

    uuid_str = str(uuid.uuid4())[:part1]

    random_string = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(part2))

    # Combine the UUIDv3 and the random string to create a unique string ID
    unique_id = uuid_str + random_string

    return unique_id


def resolve_fileformat(file_or_dirname: str) -> str | None:
    """Resolve file format from file extension.

    Determines the file format based on file extension, supporting common
    formats like Excel, CSV, JSON, XML, and YAML. Also handles glob patterns.

    Args:
        file_or_dirname: File path, directory name, or glob pattern.

    Returns:
        File format string (excel, csv, json, xml, yaml) or None if unknown.

    Examples:
        >>> resolve_fileformat("data.xlsx")
        'excel'

        >>> resolve_fileformat("data.csv")
        'csv'

        >>> resolve_fileformat("config.json")
        'json'

        >>> resolve_fileformat("*.csv")  # Glob pattern
        'csv'

        >>> resolve_fileformat("./output/*.xlsx")  # Path with glob
        'excel'

        >>> resolve_fileformat("unknown.txt")
        None

        >>> resolve_fileformat("")
        None
    """
    ext_to_format = {
        ".xlsx": "excel",
        ".xls": "excel",
        ".csv": "csv",
        ".json": "json",
        ".xml": "xml",
        ".yaml": "yaml",
        ".yml": "yaml",
    }

    if not file_or_dirname:
        return None

    # Handle glob patterns like "*.csv" or "./out/*.csv"
    if "*" in file_or_dirname:
        # Extract extension from glob pattern
        # Examples: "*.csv" -> ".csv", "./out/*.xlsx" -> ".xlsx"
        import re

        pattern_match = re.search(r"\*\.([a-zA-Z0-9]+)$", file_or_dirname)
        if pattern_match:
            ext = "." + pattern_match.group(1)
            return ext_to_format.get(ext)
        return None

    # Handle direct extension input like ".csv" (not paths like "./file.csv")
    if file_or_dirname.startswith(".") and "/" not in file_or_dirname:
        return ext_to_format.get(file_or_dirname)

    # Handle full file path - extract extension
    _, ext = os.path.splitext(file_or_dirname)
    return ext_to_format.get(ext)


def split_path(path: str) -> tuple[str, str, str]:
    """Split file path into directory, basename, and extension.

    Separates a file path into its component parts for easier manipulation
    and analysis.

    Args:
        path: File path to split (must not be empty).

    Returns:
        Tuple of (directory, basename, extension).

    Examples:
        >>> split_path("/home/user/document.pdf")
        ('/home/user', 'document', '.pdf')

        >>> split_path("./data/file.csv")
        ('./data', 'file', '.csv')

        >>> split_path("filename.txt")
        ('', 'filename', '.txt')

        >>> split_path("/path/to/file")
        ('/path/to', 'file', '')

        >>> split_path("simple")
        ('', 'simple', '')
    """
    assert path, "Path cannot be empty"

    dirfile, ext = os.path.splitext(path)
    parts = dirfile.split("/")

    basename = parts[-1]
    dirname = "/".join(parts[:-1])

    return dirname, basename, ext
