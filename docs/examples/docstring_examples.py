"""
Example module demonstrating proper docstring formatting.

This module provides examples of Google-style docstrings as recommended
in the documentation guide. It includes examples for module, class,
function, method, and property docstrings.

Typical usage example:
    from examples import docstring_examples
    result = docstring_examples.example_function("test")
"""

from pydantic import BaseModel


class ExampleClass(BaseModel):
    """A class demonstrating proper docstring formatting.

    This class shows how to document a class, its attributes,
    methods, and properties using Google-style docstrings.

    Attributes:
        name (str): The name of the example.
        value (int): The value associated with the example.
        options (dict[str, str]): Configuration options.
    """

    name: str
    value: int
    options: dict[str, str] = {}

    def example_method(self, param1: str, param2: int | None = None) -> bool:
        """Demonstrate a method with proper documentation.

        This method shows how to document parameters, return values,
        and exceptions using Google-style docstrings.

        Args:
            param1: The first parameter.
            param2: The second parameter. Defaults to None.
                This is an example of a multi-line parameter description.

        Returns:
            True if the operation was successful, False otherwise.

        Raises:
            ValueError: If param1 is empty.
            TypeError: If param2 is provided but not an integer.

        Example:
            >>> obj = ExampleClass(name="test", value=42)
            >>> obj.example_method("hello")
            True
        """
        if not param1:
            raise ValueError("param1 cannot be empty")

        if param2 is not None and not isinstance(param2, int):
            raise TypeError("param2 must be an integer")

        return True

    @property
    def formatted_name(self) -> str:
        """Get the formatted name.

        Returns:
            The name in uppercase.
        """
        return self.name.upper()


def example_function(input_data: str, limit: int = 10) -> list[str]:
    """Process the input data and return results.

    This function demonstrates how to document a standalone function
    with parameters, return values, and examples.

    Args:
        input_data: The data to process.
        limit: Maximum number of results to return. Defaults to 10.

    Returns:
        A list of processed strings.

    Example:
        >>> example_function("test", 2)
        ['TEST', 'TEST']
    """
    return [input_data.upper()] * limit


def complex_function(
    data: list[dict[str, str]] | dict[str, list[str]], options: dict[str, bool] | None = None
) -> tuple[int, list[str]]:
    """Handle complex data structures with clear documentation.

    This example shows how to document functions with complex
    parameter and return types.

    Args:
        data: Complex nested data structure that can be either:
            - A list of dictionaries mapping strings to strings
            - A dictionary mapping strings to lists of strings
        options: Optional configuration flags. Defaults to None.

    Returns:
        A tuple containing:
            - The count of processed items
            - A list of result messages

    Raises:
        TypeError: If data is not in the expected format.
    """
    results = []
    count = 0

    # Implementation would go here
    if data and options:  # Use parameters to avoid unused argument lint
        count = 1 if isinstance(data, dict) else len(data)

    return count, results
