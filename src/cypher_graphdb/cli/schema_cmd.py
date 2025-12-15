"""Schema management commands for cgdb-cli."""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from cypher_graphdb.schema import GenerateResult, SchemaGenerator

app = typer.Typer(help="Manage graph schemas")
console = Console()


def _prompt_for_confirmation(schema_file: Path) -> bool:
    """Prompt user for confirmation when file exists or for new file creation.

    Args:
        schema_file: Path to the schema file

    Returns:
        True if should overwrite/proceed, False to cancel

    Raises:
        typer.Exit: If user cancels
    """
    if schema_file.exists():
        console.print(f"[yellow]Schema file already exists: {schema_file}[/yellow]")
        prompt = "Overwrite existing schema file? [y/N]: "
        default_proceed = False
    else:
        console.print(f"[dim]Schema will be written to: {schema_file}[/dim]")
        prompt = "Proceed? [Y/n]: "
        default_proceed = True

    try:
        answer = input(prompt).strip().lower()
    except (EOFError, KeyboardInterrupt):
        console.print("\n[yellow]Operation cancelled.[/yellow]")
        raise typer.Exit(0) from None

    if default_proceed:
        if answer in {"n", "no"}:
            console.print("[yellow]Operation cancelled.[/yellow]")
            raise typer.Exit(0)
        return schema_file.exists()  # Return True only if overwriting
    else:
        if answer not in {"y", "yes"}:
            console.print("[yellow]Operation cancelled.[/yellow]")
            raise typer.Exit(0)
        return True


def _display_generation_results(result: GenerateResult, output_path: Path, verbose: bool) -> None:
    """Display schema generation results.

    Args:
        result: GenerateResult from schema generator
        output_path: Path where schema file was written
        verbose: Whether to show detailed output

    Raises:
        typer.Exit: If generation failed
    """
    if result.success:
        console.print(f"[green]✓ Successfully generated schema file with {result.total_generated} schema(s)[/green]\n")

        if verbose and result.schemas:
            console.print("[bold]Generated schemas:[/bold]")
            for schema_info in result.schemas:
                console.print(f"  - {schema_info['schema_key']} ({schema_info['type']})")

        # Determine actual file path for display
        if output_path.is_dir():
            file_path = output_path / "graph.schema.json"
        else:
            file_path = output_path

        console.print(f"\nSchema file written to: {file_path}")
    else:
        console.print("[red]Schema generation failed![/red]\n")
        for error in result.errors:
            console.print(f"  [red]- {error}[/red]")
        raise typer.Exit(1)


@app.command()
def generate(
    models: Annotated[
        Path,
        typer.Option(
            "--models",
            "-m",
            help="Path to Python file or directory containing graph models (@node/@edge decorators)",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output path (directory uses 'graph.schema.json', or specify filename)",
        ),
    ],
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show detailed output including schema content"),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite existing schema file without prompting"),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt and proceed automatically (same as --overwrite)"),
    ] = False,
) -> None:
    """Generate a single schema JSON file from Python graph models.

    This command loads Python models decorated with @node or @edge,
    extracts their JSON Schema definitions, and writes them to a single
    graph.schema.json file in the output directory.

    Examples:
        # Generate from a single Python file
        cgdb-cli schema generate --models ./models.py --output ./schemas/

        # Generate from a directory (loads all .py files)
        cgdb-cli schema generate -m ./models/ -o ./schemas/

        # Specify output filename
        cgdb-cli schema generate -m ./models.py -o ./my-schema.json

        # With verbose output
        cgdb-cli schema generate -m ./models.py -o ./schemas/ --verbose

        # Skip confirmation prompt
        cgdb-cli schema generate -m ./models.py -o ./schemas/ --yes
    """
    # Validate models path exists
    if not models.exists():
        console.print(f"[red]Error: Models path does not exist: {models}[/red]")
        raise typer.Exit(1)

    # Determine output file path and prepare directory
    schema_file = _resolve_output_path(output)

    # Handle confirmation logic
    bypass_confirmation = _handle_confirmation(schema_file, overwrite, yes)

    # Generate schemas
    _run_schema_generation(models, output, verbose, bypass_confirmation)


def _resolve_output_path(output: Path) -> Path:
    """Resolve output path to actual schema file path."""
    if output.is_dir():
        return output / "graph.schema.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    return output


def _handle_confirmation(schema_file: Path, overwrite: bool, yes: bool) -> bool:
    """Handle confirmation logic for file overwrite.

    Returns:
        True if should overwrite existing file
    """
    if overwrite:
        return True

    if yes:
        if schema_file.exists():
            console.print(f"[red]Error: Schema file already exists: {schema_file}[/red]")
            console.print("[red]Use --overwrite to replace existing file, or run without --yes to confirm.[/red]")
            raise typer.Exit(1)
        return False

    return _prompt_for_confirmation(schema_file)


def _run_schema_generation(models: Path, output: Path, verbose: bool, overwrite: bool) -> None:
    """Run the schema generation process."""
    console.print(f"[bold]Generating schemas from {models}...[/bold]\n")

    try:
        generator = SchemaGenerator()
        result = generator.generate_to_file(
            models_path=str(models),
            output_dir=output,
            verbose=verbose,
            overwrite=overwrite,
        )
        _display_generation_results(result, output, verbose)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e
