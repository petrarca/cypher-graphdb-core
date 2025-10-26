"""Cardinality module: Defines relationship cardinality constraints."""

from enum import Enum


class Cardinality(str, Enum):
    """Relationship cardinality enumeration.

    Defines how many target nodes can be connected via a relationship.

    Attributes:
        ONE_TO_ONE: Single target node (1:1 or N:1 relationship).
        ONE_TO_MANY: Multiple target nodes (1:N or N:N relationship).
    """

    ONE_TO_ONE = "ONE_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"
