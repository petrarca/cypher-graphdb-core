"""CLI prompt parser module: Parses and interprets CLI command input.

This module provides functionality for parsing user input commands,
extracting arguments, and mapping them to appropriate handlers.
"""

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from cypher_graphdb import utils


class PromptParserCmd(BaseModel):
    """Parsed command representation with arguments, options, and metadata."""

    options: str | None
    action: str
    raw_action: str
    cmd: str | None
    args: list[str] | None
    kwargs: dict[str, Any] | None
    pos: int | None = None
    require_backend: bool
    parse_result: PromptParserResult = None
    mappings: dict[str, Any] | None
    _input: object = None
    _output: object = None

    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, value: object):
        self._input = value

    @property
    def output(self):
        return self._output

    @output.setter
    def output(self, value: object):
        self._output = value

    def is_action(self, action: str | tuple[str, ...] | list[str] | set[str]):
        action = action if isinstance(action, tuple | list | set) else tuple(action)

        return any(self.action.upper() == v.upper() for v in action)

    def is_singlecmd(self) -> bool:
        return len(self.parse_result.cmds) == 1

    def is_firstcmd(self) -> bool:
        return self == self.parse_result.cmds[0]

    def is_lastcmd(self) -> bool:
        return self == self.parse_result.cmds[-1]

    def priorcmd(self):
        return self.parse_result.cmds[self.pos - 1] if not self.is_firstcmd() else None

    def nextcmd(self):
        return self.parse_result.cmds[self.pos + 1] if not self.is_lastcmd() else None

    def has_nextcmd(self) -> bool:
        return self.nextcmd() is not None

    def has_priorcmd(self) -> bool:
        return self.priorcmd() is not None

    def has_shellcmd(self):
        return self.parse_result.has_shellcmd()

    def is_finalcmd(self) -> bool:
        return (self.is_singlecmd() or self.is_lastcmd()) and not self.has_shellcmd()

    def set_output_to_input(self):
        self._output = self._input

    def get_kwarg(self, name, default: Any = None) -> str | None:
        return self.kwargs.get(name, default)

    def get_arg(self, pos, default=None, cast_to: type = None) -> object | None:
        result = default if pos < 0 or pos >= len(self.args) else self.args[pos]

        if cast_to is not None:
            result = cast_to(result)

        return result

    def has_arg(self, name: str) -> bool:
        return name in self.args

    def has_num_args(self, min_len: int = 0, max_len: int | None = None) -> bool:
        if max_len is None:
            max_len = min_len
        elif max_len == -1:
            max_len = len(self.args) if self.args else 0

        assert min_len <= max_len

        if self.args:
            return len(self.args) >= min_len and len(self.args) <= max_len
        else:
            return min_len == 0

    def replay(self, replaced_cmd: type[PromptParserCmd] | None):
        if replaced_cmd is not None:
            values = replaced_cmd.model_dump()
            del values["pos"]
            del values["parse_result"]

            self.__dict__.update(values)

        # replay the command
        self.parse_result.replay_cmd = True


class PromptParserResult(BaseModel):
    """Complete parsing result containing all parsed commands and metadata."""

    cmds: list[PromptParserCmd] = []
    cmdline: str
    shell_cmd: str
    exit_cmd: bool = False
    replay_cmd: bool = False
    failed_reason: str = None

    def failed(self) -> bool:
        return self.failed_reason is not None

    def output_from_pipe(self):
        return self.cmds[-1]._output if len(self.cmds) > 0 else None

    def has_shellcmd(self):
        return self.shell_cmd


class PromptParser:
    """Command line parser that maps user input to structured commands."""

    def __init__(self, cmd_map: dict[str, list[str]], default_action=None) -> None:
        self.cmd_map = self._prepare_cmd_map(cmd_map)

        self._default_raw_action = default_action
        self._default_action = self._raw_to_action(default_action)

    @property
    def default_action(self) -> str:
        return self._default_action

    def parse_prompt(self, prompt_line) -> PromptParserResult:
        # split internal and shell commands, sepated by double pipe, ignoring separators in quoted strings
        internal_cmds, _, shell_cmd = utils.partition_str(prompt_line, "||")

        parse_result = PromptParserResult(cmdline=prompt_line, shell_cmd=shell_cmd.strip())

        # handle cmd | ..., ignoring separators in quoted strings
        commands = utils.split_str(internal_cmds, "|")

        pos = 0
        for command in commands:
            if pos == len(commands) - 1 and len(commands) > 1:
                # end of command chain
                pos = -1

            cmd = self.parse_cmd(command)
            cmd.parse_result = parse_result
            cmd.pos = pos
            parse_result.cmds.append(cmd)
            pos += 1

        # if failure, result will be marked as failed
        self._validate_sequence(parse_result)

        return parse_result

    def parse_cmd(self, text: str) -> PromptParserCmd:
        assert text is not None

        raw_action, cmd, options, args, kwargs, mappings = self._parse_cmd_with_map(self.cmd_map, text.strip())

        if cmd:
            cmd = cmd.strip()

        if not raw_action:
            raw_action = self._default_raw_action

        if raw_action and raw_action.endswith("_"):
            require_backend = False
            raw_action = raw_action[:-1]
        else:
            require_backend = True

        action = self._raw_to_action(raw_action)

        return PromptParserCmd(
            options=options,
            action=action,
            raw_action=raw_action,
            cmd=cmd,
            args=args,
            kwargs=kwargs,
            require_backend=require_backend,
            mappings=mappings,
        )

    def execute_cmds(
        self,
        parse_result: PromptParserResult,
        exec_cmd: Callable,
        pipe_to_shellcmd: Callable,
    ) -> bool:
        priorcmd: PromptParserCmd = None

        for cmd in parse_result.cmds:
            while True:
                if priorcmd:
                    # pass data from one command to another
                    cmd.input = priorcmd.output

                if not exec_cmd(cmd):
                    return False

                if parse_result.exit_cmd:
                    return True

                # check if command needs to be "replayed"
                if not parse_result.replay_cmd:
                    break

                parse_result.replay_cmd = False

            priorcmd = cmd

        return not (parse_result.has_shellcmd() and not pipe_to_shellcmd(parse_result))

    def _prepare_cmd_map(self, cmd_map):
        result = dict(sorted(cmd_map.items(), key=lambda item: (-len(item[0]), item[0])))

        for _, value in result.items():
            tokens = value["tokens"]
            sorted_tokens = sorted(tokens, key=lambda x: (x is None, -len(x) if x else 0, x if x else ""))
            value["tokens"] = sorted_tokens

        return result

    def _raw_to_action(self, raw_action: str | None) -> str | None:
        if raw_action:
            return raw_action.replace("[", "").replace("]", "")

        return None

    def _validate_sequence(self, parse_result: PromptParserResult) -> bool:
        for cmd in parse_result.cmds:
            require_singlecmd = False

            if (require_start := cmd.raw_action.startswith("[")) and cmd.raw_action.startswith("[["):
                require_singlecmd = True

            if (require_end := cmd.raw_action.endswith("]")) and cmd.raw_action.endswith("]]"):
                require_singlecmd = True

            # validate [command sequence condition
            cond_start = cmd.is_firstcmd() if require_start else True
            # validate command sequence condition]
            cond_end = cmd.is_lastcmd() if require_end else True
            # validate [] single command condition
            cond_singlecmd = cmd.is_singlecmd() if require_singlecmd else True

            # handle [command] or [[command]] -> command has to start either at beginning or at the end
            cond_pos = cond_start or cond_end if require_start and require_end else cond_start and cond_end

            if not (cond_pos or cond_singlecmd):
                parse_result.failed_reason = f"Invalid command sequence: {cmd.cmd}"
                return False

        return True

    def _parse_cmd_with_map(self, cmd_map, command):
        def match_cmd(tokens, split_args):
            pos = utils.starts_with(command, tokens)

            if pos > 0:
                options = command[pos:].strip()
                cmd_ = command[:pos]

                # split into args and kwargs
                if split_args:
                    args, kwargs = utils.split_into_args_kwargs(options)
                else:
                    args, kwargs = [], {}

                return cmd_, options, args, kwargs
            else:
                return None, None, None, None

        for raw_action, mappings in cmd_map.items():
            if raw_action.endswith("*"):
                raw_action = raw_action[:-1]
                split_args = False
            else:
                split_args = True

            cmd, options, args, kwargs = match_cmd(mappings.get("tokens"), split_args)

            if cmd:
                return raw_action, cmd, options, args, kwargs, mappings

        return self._default_action, command, options, None, None, None
