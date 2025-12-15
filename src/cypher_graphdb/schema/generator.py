"""Schema Generator - Generate JSON schema files from Python graph models."""

import json
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field

from ..utils import combine_schemas


class GenerateResult(BaseModel):
    """Result of schema generation operation."""

    success: bool = True
    total_generated: int = 0
    schemas: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    def add_schema(self, schema_info: dict[str, Any]) -> None:
        """Add a generated schema to the result.

        Args:
            schema_info: Schema metadata (schema_key, type, file_path)
        """
        self.schemas.append(schema_info)
        self.total_generated += 1

    def add_error(self, error: str) -> None:
        """Add an error to the result.

        Args:
            error: Error message
        """
        self.errors.append(error)
        self.success = False


class SchemaGenerator:
    """Generate JSON schema files from Python graph models."""

    def _load_schemas(self, models_path: str | Path) -> list[dict[str, Any]]:
        """Load schemas from Python models.

        Args:
            models_path: Path to Python file or directory with models

        Returns:
            List of individual schema dicts
        """
        from ..modelprovider import model_provider

        return model_provider.generate_schemas_from_path(str(models_path), combine=False)

    def generate_schemas(
        self,
        models_path: str | Path,
        combine: bool = True,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Generate schemas from Python models (in-memory, no file I/O).

        Args:
            models_path: Path to Python file or directory with models
            combine: If True, return combined schema; if False, return list of schemas

        Returns:
            Combined schema dict or list of individual schemas
        """
        all_schemas = self._load_schemas(models_path)

        if combine and all_schemas:
            return combine_schemas(all_schemas)
        return all_schemas

    def generate_to_file(
        self,
        models_path: str | Path,
        output_dir: Path,
        verbose: bool = False,
        overwrite: bool = False,
    ) -> GenerateResult:
        """Generate a single schema JSON file from Python models.

        Loads models from the specified path (file or directory) and writes them
        to a single graph.schema.json file.

        Args:
            models_path: Path to Python file or directory with models
            output_dir: Directory where schema file will be written
            verbose: If True, log detailed information
            overwrite: If True, overwrite existing file without prompting

        Returns:
            GenerateResult with success status, generated schemas, and errors
        """
        result = GenerateResult()

        logger.info(f"Loading models from: {models_path}")
        try:
            all_schemas = self._load_schemas(models_path)
        except (FileNotFoundError, ValueError) as e:
            error_msg = f"Failed to load models from {models_path}: {e}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result

        if not all_schemas:
            error_msg = f"No models loaded from {models_path}"
            logger.warning(error_msg)
            result.add_error(error_msg)
            return result

        logger.info(f"Loaded {len(all_schemas)} schema(s)")

        for json_schema in all_schemas:
            schema_type = json_schema.get("x-graph", {}).get("type", "UNKNOWN")
            schema_label = json_schema.get("x-graph", {}).get("label") or json_schema.get("title", "unknown")
            result.add_schema(
                {
                    "schema_key": schema_label,
                    "type": schema_type,
                    "file_path": str(output_dir / "graph.schema.json"),
                }
            )

            if verbose:
                logger.info(f"Collected schema: {schema_label} ({schema_type})")

        if all_schemas:
            try:
                file_path = self._write_combined_schema_file(
                    schemas=all_schemas,
                    output_path=output_dir,
                    overwrite=overwrite,
                )
                logger.info(f"Written combined schema file: {file_path}")
            except (FileExistsError, OSError) as e:
                error_msg = f"Failed to write combined schema file: {e}"
                logger.error(error_msg)
                result.add_error(error_msg)

        return result

    def _write_combined_schema_file(
        self,
        schemas: list[dict[str, Any]],
        output_path: Path,
        overwrite: bool = False,
    ) -> Path:
        """Write combined schema data to JSON file using enriched $defs format.

        Args:
            schemas: List of JSON schema objects to write
            output_path: Output path - if directory, uses 'graph.schema.json'; if file, uses that filename
            overwrite: If True, overwrite existing file

        Returns:
            Path to written file

        Raises:
            FileExistsError: If file exists and overwrite is False
        """
        if output_path.is_dir():
            file_path = output_path / "graph.schema.json"
        else:
            file_path = output_path

        if file_path.exists() and not overwrite:
            raise FileExistsError(f"Schema file already exists: {file_path}. Use --overwrite to replace.")

        enriched_schema = combine_schemas(schemas)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(enriched_schema, f, indent=2, ensure_ascii=False)

        logger.debug(f"Written enriched schema file: {file_path}")
        return file_path
