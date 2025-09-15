# Command Template for Help Documentation

This template provides a unified format for creating help documentation for CLI commands.

## Structure

```markdown
# {Command Name}

**Command:** `{primary_command}` | `{alias1}` | `{alias2}`

**Description:** {Brief description of what the command does}

## Syntax
```
{command} <required_param>[,option=value][,option2=value2,...]
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `param1` | string | Yes | Description of required parameter |
| `option1` | string | No | Description of optional parameter |
| `option2` | boolean | No | Description with default value (default: `true`) |

## Examples

### Basic Usage
```bash
# Simple example
{command} basic_example

# Example with options
{command} example,option1=value,option2=false
```

### Advanced Usage
```bash
# Complex example with multiple options
{command} advanced_example,option1=value1,option2=value2,option3=true
```

## Options Details

### Option Categories (if applicable)
- **option1**: Detailed explanation
- **option2**: Detailed explanation with possible values
- **option3**: Boolean option explanation

## Interactive vs Non-Interactive Mode (if applicable)

### Interactive Mode (CLI prompt)
Explanation of interactive behavior

### Non-Interactive Mode (Command Line)
Explanation of non-interactive usage with --yes flag or piped input

## Output

Description of command output and feedback

## Error Handling

Common error scenarios and solutions

## Related Commands

- `related_command1` - Brief description
- `related_command2` - Brief description

## Notes

- Important notes about the command
- Limitations or special considerations
- Performance considerations
```

## Guidelines

1. **Command Line Syntax**: Always use comma-separated parameters: `command param,option=value,option2=value`
2. **Parameter Types**: Specify clear types (string, boolean, number, etc.)
3. **Examples**: Provide both basic and advanced examples
4. **Cross-references**: Link to related commands
5. **Error Handling**: Include common error scenarios
6. **Interactive Modes**: Explain both interactive and non-interactive usage
7. **Formatting**: Use consistent markdown formatting with code blocks

## Parameter Syntax Rules

- **Required parameters**: `<parameter>`
- **Optional parameters**: `[,option=value]`
- **Multiple options**: `[,option1=value1,option2=value2,...]`
- **Boolean options**: `option=true` or `option=false`
- **String values with spaces**: `option="value with spaces"`
- **No spaces around commas**: `param1,option=value,option2=value`
