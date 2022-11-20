from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

import uuid

from . import connection


if TYPE_CHECKING:
    from .engine import IPCEngine
    from .role import Role


__all__ = (
    "Device",
)


logger = getLogger(__name__)


class Device(connection.IPCCore):
    def __init__(self, uuid_override: str | None = None):
        super().__init__()
        self._uuid: str = uuid_override or uuid.uuid1().hex
        self._engine: IPCEngine | None = None
        """Set when the Device is added to an Engine."""
        self._role: Role | None = None
        """Set when the Device is added to a Role."""

    @property
    def role(self) -> Role | None:
        return self._role

    def set_role(self, role: Role):
        self._role = role

    @property
    def engine(self) -> IPCEngine | None:
        return self._engine

    def set_engine(self, engine: IPCEngine):
        self._engine = engine
        self.on_set_engine(engine)

    def on_set_engine(self, engine: IPCEngine):
        """Ran when the engine is set. Exists for the sole purpose of being optionally overridden."""
        pass

    @property
    def uuid(self) -> str:
        return self._uuid
