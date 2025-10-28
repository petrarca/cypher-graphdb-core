"""Contains configuration for the cypher_graphdb module."""

from enum import Enum

GID_LENGTH = 16

PROP_ID = "id_"
PROP_LABEL = "label_"
PROP_PROPERTIES = "properties_"
PROP_GID = "gid_"
PROP_START_ID = "start_id_"
PROP_END_ID = "end_id_"
PROP_START_GID = "start_gid_"
PROP_END_GID = "end_gid_"
PROP_START_LABEL = "start_label_"
PROP_END_LABEL = "end_label_"
PROP_START_KEY = "start_key_"
PROP_END_KEY = "end_key_"

NODE_FIELDS = (PROP_ID, PROP_LABEL, PROP_PROPERTIES)
EDGE_FIELDS = NODE_FIELDS + (PROP_START_ID, PROP_END_ID)

CREATE_OR_MERGE_STRAGEY = ("merge", "force_create")

REF_PROPS_BY_ID = (PROP_ID,)
REF_PROPS_BY_GID = (PROP_GID,)

# Properties for referencing start/end nodes by gid
REF_PROPS_BY_SE_GID = (PROP_START_GID, PROP_END_GID)

# Properties for referencing end nodes by keys
REF_PROPS_BY_KEY = (PROP_START_LABEL, PROP_START_KEY, PROP_END_LABEL, PROP_END_KEY)


class MatchReference(Enum):
    """Enumeration of different ways to match graph objects."""

    BY_ID = ("id",)
    BY_GID = ("gid",)
    BY_KEY = "key"


EDGE_FILE_NAME = "prefix"  # or "postfix"

CGDB_BACKEND = "CGDB_BACKEND"
CGDB_CINFO = "CGDB_CINFO"
CGDB_GRAPH = "CGDB_GRAPH"

TREE_DIRECTION_INCOMING = "incoming"
TREE_DIRECTION_OUTGOING = "outgoing"
DEFAULT_TREE_DIRECTION = TREE_DIRECTION_INCOMING

# Model source URI schemes
MODEL_SOURCE_FILE_URI = "file://"  # File-based models (e.g., "file:///path/to/models.py")
MODEL_SOURCE_DB_URI = "db://"  # Database-loaded models (e.g., "db://metadata")
MODEL_SOURCE_SCHEMA_URI = "schema://"  # Schema-injected models (e.g., "schema://dynamic")
