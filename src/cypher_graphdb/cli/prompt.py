"""cli_prompt module: Command-line prompt interface.

Provides CommandLinePrompt class for interactive command-line sessions
with history, completion, and key bindings.
"""

from collections.abc import Callable

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings, KeyPressEvent
from prompt_toolkit.shortcuts import CompleteStyle

from .completer import CommandLineCompleter
from .promptparser import PromptParser
from .provider import CLIProviders


class CommandLinePrompt:
    """Interactive command line prompt with auto-completion and command parsing."""

    def __init__(
        self,
        prompt_parser: PromptParser,
        providers: CLIProviders,
        on_resolve_prompt: Callable[[], str] = None,
    ):
        self._session_store = ".cgdb_history"
        self._prompt_parser = prompt_parser
        self._providers = providers

        self.on_resolve_prompt = on_resolve_prompt or (lambda: "")

    def runloop(self, handler: Callable[[str], bool]):
        assert handler is not None

        history = FileHistory(self._session_store) if self._session_store else InMemoryHistory()
        session = PromptSession(history=history)

        kb = KeyBindings()

        @kb.add("escape", "enter")
        def _(event: KeyPressEvent):
            event.current_bufer.insert_text("\n")

        @kb.add("enter")
        def _(event: KeyPressEvent):
            if event.current_buffer.complete_state:
                # finish current completion
                event.current_buffer.complete_state = None
            else:
                event.current_buffer.validate_and_handle()

        completer = CommandLineCompleter(
            self._prompt_parser.cmd_map,
            self._prompt_parser.default_action,
            self._providers,
        )

        while True:
            try:
                prompt_str = self.on_resolve_prompt()

                # reset caches etc.
                completer.reset()

                cmdline = session.prompt(
                    f"{prompt_str}> ",
                    enable_history_search=True,
                    key_bindings=kb,
                    multiline=True,
                    vi_mode=True,
                    completer=completer,
                    complete_style=CompleteStyle.MULTI_COLUMN,
                )
            except KeyboardInterrupt:
                # Ctrl-C pressed
                continue
            except EOFError:
                # Ctrl-D pressed. Exit.
                break

            if not (cmdline := cmdline.strip()):
                continue

            if not handler(cmdline):
                break
