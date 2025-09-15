"""options module: Base class for typed configuration option models.

Provides TypedOptionModel for creating Pydantic models that can be constructed
from boolean and value option dictionaries.
"""

from typing import Any

from pydantic import BaseModel


class TypedOptionModel[T](BaseModel):
    """Base class for creating typed option models from boolean and value options."""

    @classmethod
    def from_opts(cls: type[T], boolean_opts: set[str] = None, value_opts: dict[str, Any] = None) -> T:
        """Create an instance from boolean flags and value options.

        Args:
            boolean_opts: Set of boolean option strings (e.g., {'+debug', '-verbose'}).
            value_opts: Dictionary of option name to value mappings.

        Returns:
            New instance with fields populated from the options.

        """
        if boolean_opts is None:
            boolean_opts = set()
        if value_opts is None:
            value_opts = {}

        return cls(**cls.to_field_values(boolean_opts, value_opts))

    @classmethod
    def to_field_values(cls, boolean_opts: set[str], value_opts: dict[str, Any]) -> dict[str, Any]:
        """Convert option sets to field values for model instantiation.

        Args:
            boolean_opts: Set of boolean option strings with optional +/- prefixes.
            value_opts: Dictionary of option name to value mappings.

        Returns:
            Dictionary of field names to values for model construction.

        """
        # option or +option result in True, -option in False
        field_values = {opt.lstrip("-"): not opt.startswith("-") for opt in boolean_opts if opt.lstrip("-") in cls.model_fields}
        # option = value
        field_values.update(value_opts.items())

        return field_values

    def set_all(self, value: bool) -> None:
        """Set all boolean fields to the specified value.

        Args:
            value: Boolean value to assign to all boolean fields.

        """
        for field, fieldinfo in self.model_fields.items():
            if fieldinfo.annotation is bool:
                setattr(self, field, value)
