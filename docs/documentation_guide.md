# Documentation Guide for Contributors

This guide explains how to write effective documentation for the CypherGraph DB project, focusing on docstrings and code comments.

## Docstring Format

We use **Google-style** docstrings for all Python code. This format is well-supported by mkdocstrings and provides clear structure.

### Module-Level Docstrings

Every Python module should have a docstring at the top that explains its purpose:

```python
"""
Module name and short description.

Extended description of the module's purpose, key features,
and how it fits into the overall project architecture.

Typical usage example:
    from cypher_graphdb import module_name
    result = module_name.function_name()
"""
```

### Class Docstrings

Document classes with:

```python
class MyClass:
    """Short description of the class.

    Extended description explaining the class's purpose,
    behavior, and any important implementation details.

    Attributes:
        attr_name (type): Description of the attribute.
        another_attr (type): Description of another attribute.
    """
```

### Method and Function Docstrings

Document methods and functions with:

```python
def my_function(param1: type, param2: type) -> return_type:
    """Short description of what the function does.

    Extended description with more details about behavior,
    edge cases, or implementation notes.

    Args:
        param1: Description of the first parameter.
        param2: Description of the second parameter.
            Indented continuation for long descriptions.

    Returns:
        Description of the return value.

    Raises:
        ExceptionType: When and why this exception might be raised.

    Example:
        >>> result = my_function("value", 42)
        >>> print(result)
        Expected output
    """
```

### Property Docstrings

Document properties with:

```python
@property
def my_property(self) -> type:
    """Short description of the property.

    Extended description if needed.

    Returns:
        Description of the return value.
    """
```

## Type Annotations

Always use type annotations for:
- Function parameters
- Return values
- Class attributes
- Variable annotations where helpful

```python
def process_data(data: list[str], limit: int = 10) -> dict[str, int]:
    """Process the input data.

    Args:
        data: List of strings to process.
        limit: Maximum number of items to process.

    Returns:
        Dictionary mapping processed items to their counts.
    """
```

## Documentation Best Practices

1. **Be Concise**: Keep descriptions clear and to the point.

2. **Use Examples**: Include usage examples for complex functions or classes.

3. **Document Exceptions**: Note when and why exceptions might be raised.

4. **Update Documentation**: Keep documentation in sync with code changes.

5. **Document Public APIs**: Focus on documenting public interfaces thoroughly.

6. **Cross-Reference**: Use references to other parts of the API when relevant.

## Building and Testing Documentation

To build the documentation locally:

```bash
task build:docs
```

To view the documentation with live reloading:

```bash
task serve:docs
```

## Documentation Structure

- `docs/`: Main documentation directory
  - `index.md`: Project overview
  - `usage/`: Usage guides and tutorials
  - `reference/`: API reference (auto-generated)
  - `contributing.md`: Contribution guidelines
  - `documentation_guide.md`: This guide

## Example File

We've created a sample file with examples of properly formatted docstrings that you can reference:

- [Docstring Examples](examples/docstring_examples.py)

## Additional Resources

- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- [MkDocs Documentation](https://www.mkdocs.org/)
- [mkdocstrings Documentation](https://mkdocstrings.github.io/)
