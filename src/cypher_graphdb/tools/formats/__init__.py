"""Built-in data formats.

Each module registers one format on the global ``format_provider`` when
imported. They are imported lazily, by name, so a format whose dependency lives
in an optional extra costs nothing until it is used.
"""
