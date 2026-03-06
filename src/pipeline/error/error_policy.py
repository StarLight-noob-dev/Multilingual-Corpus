from collections.abc import Callable
from enum import Enum, auto
from typing import Dict, Type, Tuple, Optional


class Action(Enum):
    SKIP = auto()
    PAUSE = auto()
    STOP = auto()
    RETRY = auto()

class ErrorPolicy:
    def __init__(self, default_action: Action = Action.SKIP):
        """
        Holds mappings from exception types and message substrings to recovery actions, along with optional
        behaviors to execute for each action. The policy allows for flexible error handling strategies based on the
        nature of the exception or its message content.

        The Callables for behaviors are expected to take the exception and the data that caused it as
        arguments, allowing for context-aware recovery actions.

        Args:
            default_action (Action): The default action to take when no specific mapping is found for an exception
                type or message. Defaults to Action.SKIP
        """
        self.type_map: Dict[Type[Exception], Action] = {}
        self.message_map: Dict[str, Action] = {}
        self.behavior_map: Dict[Action, Callable] = {}
        self.default_action = default_action

    def map_exception(self, exc_type: Type[Exception], action: Action, behavior: Callable = None):
        """Map an exception type to a recovery action, with an optional behavior to execute when that action is taken."""
        self.type_map[exc_type] = action
        if behavior:
            self.behavior_map[action] = behavior
        return self

    def register_behavior(self, action: Action, behavior: Callable):
        """Register a behavior to execute for a specific action, regardless of the exception type."""
        self.behavior_map[action] = behavior
        return self

    def map_message(self, message_substring: str, action: Action):
        """Map a substring found in an exception message to a recovery action"""
        self.message_map[message_substring] = action
        return self

    def get_action(self, e: Exception) -> Action:
        """Determine the recovery action for a given exception based on its type and message content."""
        action = self.type_map.get(type(e))
        if not action:
            msg = str(e).lower()
            action = next((act for sub, act in self.message_map.items() if sub in msg), self.default_action)
        return action

    def get_recovery_details(self, e: Exception) -> Tuple[Action, Optional[Callable]]:
        """Get the recovery action and associated behavior for a given exception."""
        action = self.get_action(e)
        behavior = self.behavior_map.get(action)
        return action, behavior