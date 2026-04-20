"""CLI auto-completion module: Provides tab completion for CLI commands.

This module implements intelligent auto-completion for the CLI interface,
including completion for commands, graph labels, property names, and more.
"""

import re
from collections.abc import Callable, Generator
from typing import Any

from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document
from pydantic import BaseModel, Field

import cypher_graphdb.utils as utils
from cypher_graphdb.models import GraphObject, GraphObjectType
from cypher_graphdb.options import TypedOptionModel

from .promptparser import PromptParser, PromptParserCmd
from .provider import CLIProviders

_FIND_WORD_RE = re.compile(r"([a-zA-Z0-9_\$]+|[^a-zA-Z0-9_\s]+)")


class CompleterConfig(BaseModel):
    """Configuration class for CLI completion settings."""

    _cmd_props: dict[str, Field] = {}
    _parse_result: PromptParserCmd

    @property
    def parse_result(self):
        return self._parse_result

    def __init__(self, parse_result: PromptParserCmd, opts_cls: type[TypedOptionModel] = None, **kwargs):
        super().__init__(**kwargs)
        self._parse_result = parse_result

        if opts_cls is not None:
            for name, field in opts_cls.model_fields.items():
                if name.endswith("_"):
                    # skip over internal fields
                    continue
                self.update_cmd_property(name, prop_type=field.annotation, default=field.default)

    def update_cmd_property(
        self, name: str, prop_type: type = str, default: Any = None, completer: Callable = None, ctx: Any = None
    ) -> Field:
        field = self._cmd_props.get(name)
        if field is None:
            field = Field()
            self._cmd_props[name] = field

        if prop_type is not None:
            field.annotation = prop_type
        if default is not None:
            field.default = default

        if completer is not None:
            field.metadata = {"completer": completer, "ctx": ctx}

        return field

    def resolve_props(self, word: str) -> dict[str, Field]:
        result = {}
        for name, prop_info in self._cmd_props.items():
            if "|" in name:
                parts = name.split("|")
                if not any(self._parse_result.kwargs.get(part.strip()) for part in parts):
                    result.update({part.strip(): prop_info for part in parts if part.startswith(word)})
            else:
                if not self._parse_result.kwargs.get(name) and name.startswith(word):
                    result[name] = prop_info

        return result

    def get_cmd_property(self, name: str) -> Field:
        for name_, prop_info in self._cmd_props.items():
            if name in name_.split("|"):
                return prop_info

        return None


class LabelPropsCompleterConfig(CompleterConfig):
    """Configuration for completing both labels and properties."""

    complete_mandatory_props: bool = False
    object_type: GraphObjectType = None
    with_label: bool = True
    with_props: bool = True
    label_from_model: bool = True
    resolve_model_props: bool = True
    default_from_values: bool = False

    def __init__(self, parse_result: PromptParserCmd, opts_cls: type[TypedOptionModel] = None, **kwargs):
        super().__init__(parse_result, opts_cls, **kwargs)

        if self.object_type is None:
            self.object_type = self._parse_result.mappings.get("object_type") if self._parse_result else None


class LabelCompleterConfig(LabelPropsCompleterConfig):
    """Configuration for completing only labels."""

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)

        self.with_label = True
        self.with_props = False


class PropsCompleterConfig(LabelPropsCompleterConfig):
    """Configuration for completing only properties."""

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)

        self.with_label = False
        self.with_props = True


class ListProviderConfig(CompleterConfig):
    """Configuration for completion using list providers."""

    list_provider: Callable[[None], tuple[str]] | list[str]


class CommandLineCompleter(Completer):
    """Main command line completer for the CLI interface."""

    def __init__(
        self,
        cmd_map: dict[str, list[str]],
        default_action: str | None,
        providers: CLIProviders,
    ):
        self._cmd_map = cmd_map
        self._tokens_to_cmd = self._cmd_to_token_map(cmd_map)
        self._providers = providers

        self._prompt_parser = PromptParser(cmd_map, default_action)
        self._obj_cache = {}
        self._props_cache = None

        # Initialize command map for completion handlers
        self._init_command_map()

    def get_completions(self, document: Document, _: CompleteEvent) -> Generator[Completion]:
        text, separator = self._text_before_cursor_to_sep(document)

        if separator == "||":
            # we are after shell separator, no completion
            return

        matches = [token for token in self._tokens_to_cmd if token.startswith(text)]

        if len(matches) > 1 or (matches and matches[0] != text):
            for token in matches:
                yield Completion(
                    f"{token} ",
                    start_position=-len(text),
                    display=token,
                )
            return

        if document.is_cursor_at_the_end:
            yield from self._complete_by_cmd(self._prompt_parser.parse_cmd(text), document)

    def reset(self):
        self._obj_cache.clear()
        self._props_cache = None

    def _init_command_map(self):
        """Initialize the command map for completion handlers.

        Maps each command directly to a function that creates the appropriate completion configuration.
        """
        self._completion_map = {
            # Fetch commands
            "fetch_nodes": lambda parse_result: LabelPropsCompleterConfig(parse_result),
            "fetch_edges": lambda parse_result: LabelPropsCompleterConfig(parse_result),
            "fetch_all": lambda parse_result: LabelPropsCompleterConfig(parse_result),
            # Create commands
            "create_node": lambda parse_result: LabelPropsCompleterConfig(parse_result, complete_mandatory_props=True),
            "create_edge": lambda parse_result: LabelPropsCompleterConfig(parse_result, complete_mandatory_props=True),
            "create_linked_node": lambda parse_result: self._create_linked_node_config(parse_result),
            # Delete and update commands
            "delete": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, resolve_model_props=False
            ),
            "delete_node": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, resolve_model_props=False
            ),
            "delete_edge": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, resolve_model_props=False
            ),
            "update": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, default_from_values=True
            ),
            "update_node": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, default_from_values=True
            ),
            "update_edge": lambda parse_result: LabelPropsCompleterConfig(
                parse_result, label_from_model=False, default_from_values=True
            ),
            # Schema and model commands
            "dump_schema": lambda parse_result: LabelCompleterConfig(parse_result),
            "dump_models": lambda parse_result: LabelCompleterConfig(parse_result),
            "search": lambda parse_result: self._create_search_config(parse_result),
            # Database and variable commands
            "drop_graph": lambda parse_result: ListProviderConfig(
                parse_result, list_provider=self._providers.graphdb_provider.get_graphs
            ),
            "use_graph": lambda parse_result: ListProviderConfig(
                parse_result, list_provider=self._providers.graphdb_provider.get_graphs
            ),
            "graph_exists": lambda parse_result: ListProviderConfig(
                parse_result, list_provider=self._providers.graphdb_provider.get_graphs
            ),
            "dump_indexes": lambda parse_result: ListProviderConfig(parse_result, list_provider=["all"]),
            "get": lambda parse_result: ListProviderConfig(parse_result, list_provider=self._providers.var_provider.get_varnames),
            "set": lambda parse_result: ListProviderConfig(parse_result, list_provider=self._providers.var_provider.get_varnames),
            "config": lambda parse_result: ListProviderConfig(
                parse_result, list_provider=self._providers.config_provider.get_props
            ),
        }

    def _complete_by_cmd(self, parse_result: PromptParserCmd, document: Document) -> Generator[Completion]:
        """Complete command based on parsed command and document.

        This method uses the command map to get the appropriate completion configuration.
        """
        # Look up the config generator in the command map
        config = None
        config_generator = self._completion_map.get(parse_result.action)
        if config_generator:
            config = config_generator(parse_result)

        # Process the configuration and yield completions
        if isinstance(config, LabelPropsCompleterConfig):
            yield from self._complete_label_props(document, config)
        elif isinstance(config, ListProviderConfig):
            yield from self._complete_from_list(document, config, True)

    def _create_linked_node_config(self, parse_result: PromptParserCmd) -> CompleterConfig:
        """Create configuration for linked node completion."""
        config = LabelPropsCompleterConfig(parse_result, complete_mandatory_props=True)
        config.update_cmd_property("from_|to_")
        config.update_cmd_property("edge_label_", completer=self._complete_label_choice, ctx=GraphObjectType.EDGE)
        return config

    def _create_search_config(self, parse_result: PromptParserCmd) -> CompleterConfig:
        """Create configuration for search command completion."""
        config = PropsCompleterConfig(parse_result)
        config.update_cmd_property("language")
        return config

    def _complete_label_props(self, document: Document, config: LabelPropsCompleterConfig) -> Generator[Completion]:
        def select_from_model():
            for label in self._providers.model_provider.find(word, graph_objectype=config.object_type):
                if label not in exclude_labels:
                    yield Completion(f"{label},", start_position=-len(word), display=label)

        word, stopchar, exclude_labels = self._resolve_type_names(config.parse_result, document, True)

        if config.with_label and (stopchar in ("", " ") or exclude_labels is not None):
            # reset property cache, only valid for prior label or object reference
            self._props_cache = None

            if config.label_from_model:
                exclude_labels = exclude_labels or []
                yield from select_from_model()
            else:
                yield (Completion(""))
            return

        if config.with_props:
            if stopchar == "=":
                # try to complete value of <propery>=<value>
                yield from self._complete_property_value(word, document, config)
            else:
                # try to complete property of <propery>{=<value}
                yield from self._complete_property(word, config)

    def _complete_label_choice(self, word: str, ctx: GraphObjectType) -> Generator[Completion]:
        for label in self._providers.model_provider.find(word, graph_objectype=ctx):
            yield Completion(f"{label},", start_position=-len(word), display=label)

    def _complete_property(self, word: str, config: LabelPropsCompleterConfig) -> Generator[Completion]:
        parse_result = config.parse_result

        properties = (self._resolve_props(word, config) or {}) if config.resolve_model_props else {}
        properties.update(config.resolve_props(word))

        if not properties:
            return

        if not word and not parse_result.kwargs and config.complete_mandatory_props:
            try:
                result = next(self._complete_mandatory_props(properties))
            except StopIteration:
                return

            # has mandatory properties
            if result:
                yield result
                return

        # try to select properties
        yield from self._complete_property_choice(word, properties)

    def _complete_property_choice(self, word: str, properties: dict[str, Field]) -> Generator[Completion]:
        for name, prop_info in properties.items():
            yield Completion(
                self._prop_completion_text(name, prop_info),
                start_position=-len(word),
                display=self._prop_completion_display(name, prop_info),
            )

    def _complete_property_value(self, word: str, document: Document, config: LabelPropsCompleterConfig) -> Generator[Completion]:
        if not (name := self._get_prop_name(word, document)):
            return

        # handle propery base completers
        if prop_info := config.get_cmd_property(name):
            if isinstance(prop_info.metadata, dict) and (completer := prop_info.metadata.get("completer", None)):
                yield from completer(word, prop_info.metadata.get("ctx", None))
            return

        if self._props_cache is None:
            self._resolve_props(name, config)

        if prop_info := self._props_cache.get(name):
            yield Completion(utils.convert_to_str(prop_info.default))

    def _complete_mandatory_props(self, properties: dict[str, Field]) -> Generator[Completion]:
        mandatory_props = {name: prop_info for name, prop_info in properties.items() if prop_info.is_required()}

        if mandatory_props:
            text = ",".join([self._prop_completion_text(name, prop_info) for name, prop_info in mandatory_props.items()])
            yield Completion(text, start_position=0)
        else:
            yield None

    def _complete_from_list(
        self, document: Document, config: ListProviderConfig, add_space: bool = False
    ) -> Generator[Completion]:
        def select_items(items):
            for item in items:
                if item.startswith(word):
                    yield Completion(f"{item}", start_position=-len(word), display=item)

        word, _, _ = self._resolve_type_names(config.parse_result, document, False)

        if word is not None:
            items = config.list_provider() if callable(config.list_provider) else config.list_provider

            yield from select_items(items)
        elif add_space:
            # add space after command
            yield Completion(" ")

    def _prop_completion_text(self, name: str, prop_info: Field) -> str:
        quotes = '""' if prop_info.annotation is str else ""
        return f"{name}={quotes}"

    def _prop_completion_display(self, name: str, prop_info: Field) -> str:
        required_marker = " *" if prop_info.is_required() else ""
        return f"{name}{required_marker}"

    def _resolve_props(self, word: str, config: LabelPropsCompleterConfig) -> dict[str, Field] | None:
        def filter_props(props):
            return {
                name: prop_info
                for name, prop_info in props.items()
                if name.startswith(word) and name not in config.parse_result.kwargs
            }

        def props_from_models():
            model_provider = self._providers.model_provider
            # Retrieve model infos for the given labels
            model_infos = [model_provider.get(label) for label in labels if model_provider.get(label)]

            return {name: prop_info for model_info in model_infos for name, prop_info in model_info.fields.items()}

        word = "" if word is None else word

        if self._props_cache:
            # return cached properties, reset cache when label or object reference chanced
            return filter_props(self._props_cache)

        parse_result = config.parse_result

        if not parse_result.args:
            # missing label, nothing to complete further
            return None

        # first argument is either label, id_ or gid_
        value, is_literal = utils.try_literal_eval(parse_result.get_arg(0))

        if config.label_from_model:
            if not is_literal:
                # id_ or gid_
                return None

            labels = utils.str_to_collection(value) if is_literal else value

            if isinstance(labels, str):
                labels = [labels]

            return props_from_models()
        else:
            graph_obj: GraphObject = self._resolve_graph_obj(value)
            properties = graph_obj.resolve_model_properties(default_from_values=config.default_from_values) if graph_obj else None

        # prevent from re-resolving same properties
        self._props_cache = properties

        return filter_props(properties)

    def _resolve_graph_obj(self, graph_obj_ref) -> GraphObject:
        if (graph_obj := self._obj_cache.get(graph_obj_ref)) is None:
            graph_obj = self._providers.graphdata_provider.fetch_graph_obj(graph_obj_ref)
            self._obj_cache[graph_obj_ref] = graph_obj

        return graph_obj

    def _resolve_type_names(self, parse_result: PromptParserCmd, document: Document, allow_list: bool):
        word, stopchar = self._get_word_before_cursor(document, (",", "=", "("))

        bracket_pos = -1
        if allow_list and (bracket_pos := self._find_open_bracket(document, len(parse_result.cmd), None)) != -1:
            exclude_types = document.text[bracket_pos + 1 : document.cursor_position - len(word) - 1] if bracket_pos != -1 else []
        else:
            exclude_types = None

        return word, stopchar, exclude_types

    def _cmd_to_token_map(self, cmd_map):
        result = {}

        for cmd, mappings in cmd_map.items():
            cmd = cmd.strip("[]_")
            tokens = mappings.get("tokens")
            if tokens[-1] is None:
                for token in tokens[:-1]:
                    result[token] = cmd
            else:
                result[tokens[0]] = cmd

        # sort by token alphabetically but then reversed by length
        # to match specialized command before generic ones like 'update node' before 'update'
        return dict(sorted(result.items(), key=lambda x: (x[0], -len(x), x)))

    def _text_before_cursor_to_sep(self, document: Document) -> tuple[str, str]:
        if pos := document.find_backwards("||"):
            return document.text[pos + 2 : document.cursor_position].strip(), "||"

        if pos := document.find_backwards("|"):
            return document.text[pos + 1 : document.cursor_position].strip(), "|"

        return document.text_before_cursor.strip(), ""

    def _get_word_before_cursor(self, document: Document, stopchars: tuple[str] | None) -> tuple[str, str]:
        if stopchars is None:
            stopchars = ()

        if (c := document.char_before_cursor) in stopchars:
            return "", c

        val = document.get_word_before_cursor(pattern=_FIND_WORD_RE)

        if (pos := document.cursor_position - len(val) - 1) == -1:
            return (
                None,
                "",
            )

        while pos >= 0:
            if document.text[pos] in (" ", '"', "'"):
                pos -= 1
                continue

            return (val, document.text[pos]) if document.text[pos] in stopchars else (val, "")

    def _get_prop_name(self, word: str, document: Document) -> str:
        pos = document.cursor_position - len(word) - 1

        if document.text[pos] != "=":
            return None

        pos -= 1

        while pos >= 0:
            if document.text[pos] in (" ", ","):
                break
            pos -= 1

        return document.text[pos + 1 : document.cursor_position - len(word) - 1]

    def _find_open_bracket(self, document: Document, start_pos: int | None, end_pos: int | None) -> int:
        if start_pos is None:
            start_pos = 0

        if end_pos is None:
            end_pos = document.cursor_position - 1

        while end_pos >= start_pos:
            if document.text[end_pos] in ("]", ")", "}"):
                return -1

            if document.text[end_pos] in ("[", "(", "{"):
                return end_pos

            end_pos -= 1

        return -1
