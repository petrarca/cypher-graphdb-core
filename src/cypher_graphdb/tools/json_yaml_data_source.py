"""JSON/YAML data source for document-based file handling.

Provides a unified interface for loading and saving both JSON and YAML files,
treating them as the same underlying Python dictionary data structure.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class JsonYamlDataSource:
    """Handles both JSON and YAML as the same data structure.

    This class provides a unified interface for loading and saving JSON and YAML
    files, abstracting away the format-specific details. Once loaded, both formats
    are represented as Python dictionaries and can be processed identically.
    """

    @staticmethod
    def load_file(filename: str) -> dict[str, Any]:
        """Load JSON or YAML file into Python dict.

        Args:
            filename: Path to the JSON or YAML file to load.

        Returns:
            Dictionary containing the loaded data.

        Raises:
            ValueError: If file format is not supported.
            FileNotFoundError: If file does not exist.
            yaml.YAMLError: If YAML file is malformed (for YAML files).
            json.JSONDecodeError: If JSON file is malformed (for JSON files).
        """
        file_path = Path(filename)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {filename}")

        try:
            with file_path.open("r", encoding="utf-8") as f:
                if filename.endswith((".json",)):
                    return json.load(f)
                elif filename.endswith((".yaml", ".yml")):
                    if not YAML_AVAILABLE:
                        raise ValueError("YAML support not available. Install PyYAML package.") from None
                    return yaml.safe_load(f) or {}
                else:
                    raise ValueError(f"Unsupported file format: {filename}") from None
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in {filename}: {e.msg}", e.doc, e.pos) from e
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {filename}: {e}") from e

    @staticmethod
    def save_file(data: dict[str, Any], filename: str) -> None:
        """Save Python dict to JSON or YAML file.

        Args:
            data: Dictionary data to save.
            filename: Target file path (.json, .yaml, or .yml).

        Raises:
            ValueError: If file format is not supported or YAML unavailable.
            OSError: If file cannot be written.
        """
        file_path = Path(filename)

        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with file_path.open("w", encoding="utf-8") as f:
                if filename.endswith((".json",)):
                    json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=False)
                elif filename.endswith((".yaml", ".yml")):
                    if not YAML_AVAILABLE:
                        raise ValueError("YAML support not available. Install PyYAML package.")
                    yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False, indent=2)
                else:
                    raise ValueError(f"Unsupported file format: {filename}")
        except OSError as e:
            raise OSError(f"Failed to write file {filename}: {e}") from e

    @staticmethod
    def get_file_extensions() -> set[str]:
        """Return supported file extensions.

        Returns:
            Set of supported file extensions including dot prefixes.
        """
        extensions = {".json"}
        if YAML_AVAILABLE:
            extensions.update({".yaml", ".yml"})
        return extensions

    @staticmethod
    def is_format_supported(filename: str) -> bool:
        """Check if file format is supported.

        Args:
            filename: File name or path to check.

        Returns:
            True if format is supported, False otherwise.
        """
        return any(filename.endswith(ext) for ext in JsonYamlDataSource.get_file_extensions())

    @staticmethod
    def detect_format(filename: str) -> str | None:
        """Detect file format from filename.

        Args:
            filename: File name or path.

        Returns:
            String format ('json', 'yaml') or None if not supported.
        """
        if filename.endswith((".json",)):
            return "json"
        elif filename.endswith((".yaml", ".yml")):
            return "yaml"
        return None


__all__ = [
    "JsonYamlDataSource",
    "YAML_AVAILABLE",
]
