import json

from cypher_graphdb.tools import CsvImporter

from ..mock_backend import build_db  # reuse shared MockBackend + patches


def write_csv(tmp_path, name, header, rows):
    path = tmp_path / name
    with path.open("w", encoding="utf-8") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(r) + "\n")
    return str(path)


def test_csv_node_import(tmp_path):
    db = build_db()
    header = ["gid_", "properties_"]
    rows = [["g1", json.dumps({"age": 30})], ["g2", json.dumps({"age": 40})]]
    filename = write_csv(tmp_path, "person.csv", header, rows)
    CsvImporter(db).load(filename)
    assert len(db._backend.nodes) == 2
    assert {n.properties_["gid_"] for n in db._backend.nodes.values()} == {"g1", "g2"}


def test_csv_node_import_with_label_column(tmp_path):
    db = build_db()
    header = ["label_", "gid_", "properties_"]
    rows = [["Person", "g1", json.dumps({"age": 22})]]
    filename = write_csv(tmp_path, "ignoredlabel.csv", header, rows)
    CsvImporter(db).load(filename)
    assert len(db._backend.nodes) == 1
    node = list(db._backend.nodes.values())[0]
    assert node.label_ == "Person"
    assert node.properties_["age"] == 22


def test_csv_edge_import_by_gid(tmp_path):
    db = build_db()
    header_nodes = ["label_", "gid_", "properties_"]
    rows_nodes = [["Person", "s1", json.dumps({})], ["Person", "e1", json.dumps({})]]
    person_file = write_csv(tmp_path, "person.csv", header_nodes, rows_nodes)
    CsvImporter(db).load(person_file)

    header_edge = ["label_", "start_gid_", "end_gid_", "properties_"]
    rows_edge = [["Knows", "s1", "e1", json.dumps({"since": 2020})]]
    edge_file = write_csv(tmp_path, "_knows.csv", header_edge, rows_edge)
    CsvImporter(db).load(edge_file)

    assert len(db._backend.edges) == 1
    edge = list(db._backend.edges.values())[0]
    assert edge.label_ == "Knows"
    assert edge.properties_["since"] == 2020
