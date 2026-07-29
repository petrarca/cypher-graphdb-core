okf_version: "0.2"

# cypher-graphdb-core documentation

- **Usage** (task-oriented, for consumers) — `usage/` is an
  [OKF](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
  bundle: install and connect, run queries, type your models, load data in bulk,
  and drive the CLI. Start at [`usage/index.md`](usage/index.md).

- **Design** (the architecture and why) — `design/` describes the backend seam,
  the layer stack, and one focused document per concept. Each doc declares
  whether the thing it describes actually exists. Start at
  [`design/index.md`](design/index.md).

- **Decisions** — `adr/` records direction changes and the reasoning behind
  them. Start at [`adr/index.md`](adr/index.md).

Open development tasks and bugs live in
[GitHub issues](https://github.com/petrarca/cypher-graphdb-core/issues), not
here. A `records/` directory may be added for point-in-time artifacts
(migration logs, post-mortems) if the need arises.

There is **no documentation build**: no mkdocs, no generated API reference, no
published site. These are plain markdown files with YAML frontmatter — read them
in the repo or on GitHub. Docstrings in the code are the API reference.

See the repository [`README.md`](../README.md) for orientation, and
[`CONTRIBUTING.md`](../CONTRIBUTING.md) for how to work on the library.
