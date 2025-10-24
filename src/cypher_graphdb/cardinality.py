"""Cardinality module: Defines relationship cardinality constraints."""

from enum import Enum


class Cardinality(str, Enum):
    """Relationship cardinality enumeration.

    Defines how many target nodes can be connected via a relationship.

    Attributes:
        ONE: Single target node (1:1 or N:1 relationship).
        MANY: Multiple target nodes (1:N or N:N relationship).
    """

    ONE = "ONE"
    MANY = "MANY"
