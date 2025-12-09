# Hierarchical Export Format Specification

## Goal

Define JSON/YAML export formats for graph data that support round-trip import/export with proper deduplication using `gid_` identifiers.

## Overview

Two export formats are supported:

| Format | Use Case | Trigger |
|--------|----------|---------|
| Flat | Standard graph queries | `export <file>` |
| Tree | Hierarchical tree data | `| tree | export <file>` |

---

## Flat Format

Nodes grouped by label, edges reference targets by `gid_`.

### Structure

```yaml
node:<Label>:
- gid_: <node-gid>
  <property>: <value>
  edge:<RELATION>:
  - gid_: <edge-gid>
    target_gid: <target-node-gid>
    target_label: <target-label>
```

### Example

```yaml
node:ArchitectureCategory:
- gid_: 4fe7991araKMMXTT
  name: Architecture Characteristics
  edge:BELONGS_TO:
  - gid_: c976ccb5q3oMBAja
    target_gid: 0ab25b6d2Zdd84cy
    target_label: ArchitectureRoot

node:ArchitectureRoot:
- gid_: 0ab25b6d2Zdd84cy
  name: Architecture
```

### Key Points

- **Nodes**: Grouped under `node:<Label>` keys
- **Edges**: Nested under source nodes with `edge:<RELATION>` keys
- **Target**: Uses `target_gid` and `target_label` for reference resolution
- **Properties**: Edge properties included at edge level (excluding internal fields)

---

## Edge Direction

Edge direction is specified via suffix on the edge key:

| Suffix | Direction | Database Relationship |
|--------|-----------|----------------------|
| (none) | forward | source → target |
| `:forward` | forward | source → target |
| `:reverse` | reverse | source ← target |

### Forward Direction (default)

```yaml
node:Child:
- gid_: child-gid
  edge:BELONGS_TO:
  - target_gid: parent-gid
```

Creates: `(Child)-[:BELONGS_TO]->(Parent)`

### Reverse Direction

```yaml
node:Parent:
- gid_: parent-gid
  edge:BELONGS_TO:reverse:
  - node:Child:
      gid_: child-gid
```

Creates: `(Child)-[:BELONGS_TO]->(Parent)` (edge points FROM child TO parent)

Used in tree exports where we traverse from root down but edges point upward (child belongs to parent).

---

## Tree Format

Hierarchical nested structure preserving parent-child relationships inline.

### Structure

```yaml
node:<RootLabel>:
- gid_: <root-gid>
  <property>: <value>
  edge:<RELATION>:
  - gid_: <edge-gid>
    node:<ChildLabel>:
      gid_: <child-gid>
      <property>: <value>
      edge:<RELATION>:
      - gid_: <edge-gid>
        node:<GrandchildLabel>:
          ...
```

### Example

```yaml
node:ArchitectureRoot:
- gid_: 0ab25b6d2Zdd84cy
  name: Architecture
  edge:BELONGS_TO:reverse:
  - gid_: c976ccb5q3oMBAja
    node:ArchitectureCategory:
      gid_: 4fe7991araKMMXTT
      name: Architecture Characteristics
      edge:BELONGS_TO:reverse:
      - gid_: 91a890a1p7g5kpAX
        node:ArchitectureCategory:
          gid_: 04f7f2d2bWJxCnnw
          name: Cross-Cutting Characteristics
```

### Key Points

- **Root nodes**: Listed under `node:<Label>` at top level
- **Edges**: Array of edge entries, each containing edge `gid_` and nested child node
- **Child nodes**: Embedded directly under `node:<Label>` key within edge entry
- **Recursion**: Children can have their own edges with nested grandchildren
- **Direction**: `:reverse` suffix indicates incoming edges (child → parent in DB)

---

## Deduplication

Both formats use `gid_` for deduplication during import:

| Element | Identifier | Behavior on Re-import |
|---------|------------|----------------------|
| Node | `gid_` | Merge if exists, create if new |
| Edge | `gid_` | Merge if exists, create if new |

This enables idempotent imports - running the same import multiple times produces identical results.

### GID Generation

`gid_` is **optional** in input files:

| Scenario | Behavior |
|----------|----------|
| `gid_` present | Used for deduplication (merge if exists) |
| `gid_` missing | Auto-generated on first create |

When `gid_` is missing:
- New nodes/edges get auto-generated GIDs on creation
- Re-importing the same file creates **duplicates** (no way to match existing)

For idempotent imports, always include `gid_` in your input files.

---

## Format Detection

The importer auto-detects format based on structure:

| Pattern | Detected Format |
|---------|-----------------|
| `edge:X: [{gid_:, node:Y: {...}}]` | Tree (nested) |
| `edge:X: [{gid_:, target_gid:, target_label:}]` | Flat (explicit) |

---

## Internal Fields

Defined in `config.py`:

### Node Fields (`NODE_FIELDS`)

| Field | Purpose | Export Handling |
|-------|---------|-----------------|
| `id_` | Internal database ID | Excluded |
| `label_` | Node label | In key `node:<LABEL>` |
| `properties_` | Property container | Flattened |

### Edge Fields (`EDGE_FIELDS`)

Includes all node fields plus:

| Field | Purpose | Export Handling |
|-------|---------|-----------------|
| `start_id_` | Start node internal ID | Excluded |
| `end_id_` | End node internal ID | Excluded |

### Additional Edge Fields (excluded from edge properties)

| Field | Purpose |
|-------|---------|
| `start_gid_`, `end_gid_` | Edge endpoint GIDs |
| `gid_` | Exported separately at edge level |

---

## File Formats

Both JSON and YAML are supported with identical data structure. Format is determined by file extension.

| Extension | Format |
|-----------|--------|
| `.json` | JSON |
| `.yaml`, `.yml` | YAML |

### JSON Example

```json
{
  "node:Category": [
    {
      "gid_": "abc123",
      "name": "Example",
      "edge:BELONGS_TO": [
        {
          "gid_": "edge123",
          "target_gid": "root456",
          "target_label": "Root"
        }
      ]
    }
  ]
}
```

### YAML Example

```yaml
node:Category:
- gid_: abc123
  name: Example
  edge:BELONGS_TO:
  - gid_: edge123
    target_gid: root456
    target_label: Root
```

### Why Edges Use Arrays

YAML/JSON do not allow duplicate keys at the same level. When a node has multiple edges of the same type, they must be grouped under a single `edge:<RELATION>` key as an array:

```yaml
node:Parent:
- gid_: parent-gid
  name: Parent Node
  edge:BELONGS_TO:reverse:
  - gid_: edge1
    node:Child:
      gid_: child1
      name: First Child
  - gid_: edge2
    node:Child:
      gid_: child2
      name: Second Child
```

Each array element represents one edge-node pair with:
- Edge properties (`gid_`, etc.) at the element level
- Target node nested under `node:<Label>`

YAML is the default format for hierarchical export due to better readability for nested structures.
