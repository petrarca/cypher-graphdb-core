"""Display configuration for graph models.

Provides configuration classes for controlling how node and edge labels
are displayed in the UI, including label property selection and sorting hints.
"""

from typing import Literal

from pydantic import BaseModel, Field


class DisplayConfig(BaseModel):
    """Configuration for display properties of nodes and edges.

    Attributes:
        labelProperty: Property name to use for display labels.
        fallbackProperty: Property to use if labelProperty is missing.
        sortProperty: Property to use for sorting.
        sortOrder: Sort direction (default: "ASC").
        filterProperty: Optional property for grouping/filtering in UI.
    """

    labelProperty: str | None = Field(
        default=None,
        description="Property for display labels (default: 'name')",
    )
    fallbackProperty: str | None = Field(
        default=None,
        description="Fallback if labelProperty missing",
    )
    sortProperty: str | None = Field(
        default=None,
        description="Property for sorting (default: labelProperty)",
    )
    sortOrder: Literal["ASC", "DESC"] | None = Field(
        default=None,
        description="Sort direction (default: 'ASC')",
    )
    filterProperty: str | None = Field(
        default=None,
        description="Optional grouping/filtering column",
    )
