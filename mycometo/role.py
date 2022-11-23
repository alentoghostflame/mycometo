from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING


from .enums import EngineEvents, IPCClassType
from .connection import IPCRoutedConnection
from . import connection, device


if TYPE_CHECKING:
    from .engine import IPCEngine
    from .device import Device


__all__ = (
    "Role",
    "SingleDeviceRole",
)


logger = getLogger(__name__)


class Role(connection.IPCCore):
    def __init__(self, name: str):
        super().__init__()
        self._name: str = name
        self._engine: IPCEngine | None = None
        """Set with the Role is added to an Engine."""
        self._devices: dict[str, Device] = {}
        """{"Device UUID": Device}"""

    @property
    def name(self) -> str:
        return self._name

    @property
    def engine(self) -> IPCEngine | None:
        return self._engine

    @property
    def local_devices(self) -> dict[str, Device]:
        return self._devices.copy()

    @property
    def devices(self) -> list[str]:
        """Returns a list of device UUIDs that have this as their role. Includes remote devices."""
        return [device for device, role_node in self.engine.map.devices if role_node[0] == self.name]

    def set_engine(self, engine: IPCEngine):
        self._engine = engine
        for device in self._devices.values():
            if device.engine is None:
                device.set_engine(engine)

        self.on_set_engine(engine)

    def on_set_engine(self, engine: IPCEngine):
        """Ran when the engine is set. Exists for the sole purpose of being optionally overridden."""
        pass

    def add_device(self, device: Device):
        if device.uuid in self._devices and self._devices[device.uuid] is not device:
            raise ValueError(f"Device with UUID {device.uuid} has already been added to this role.")

        self._devices[device.uuid] = device
        if self._engine is not None:
            device.set_engine(self.engine)

        device.set_role(self)
        if self.engine and self.engine.running.is_set():
            self.engine.events.dispatch(EngineEvents.LOCAL_DEVICE_ADDED, device)


class SingleDeviceRole(Role):
    def __init__(self, name: str):
        super().__init__(name)
        self._single_device: Device | str | None = None
        self._single_device_node: str | None = None
        """Actual Device object if it's local, str UUID of the device if remote, None if not set."""

    def on_set_engine(self, engine: IPCEngine):
        engine.events.add_listener(self.on_device_added, EngineEvents.DEVICE_ADDED)

    def add_device(self, device: Device):
        if self._single_device is None:
            super().add_device(device)
            self._single_device = device
            self._single_device_node = None
        else:
            raise ValueError("Only one device can be added to this role!")

    async def on_device_added(self, device_uuid: str, device_role: str, node_uuid: str):
        if device_role == self.name:
            if self._single_device is None:
                self._single_device = device_uuid
                self._single_device_node = node_uuid
            elif (isinstance(self._single_device, device.Device) and self._single_device.uuid != device_uuid) or \
                    (isinstance(self._single_device, str) and self._single_device != device_uuid):
                raise ValueError(
                    f"There is only supposed to be a single device for this role, but Device {device_uuid} was added "
                    f"when we already had one? Full list: {self.devices}"
                )

    async def on_incoming_chat(self, packet: connection.IPCPacket, node_uuid: str):
        logger.debug("Received incoming connection from %s %s", packet.origin_type, packet.origin_name)
        if self._single_device is None:
            logger.debug("Incoming connection denied due to no device being added.")
            conn = connection.IPCRoutedConnection.from_packet(self, packet, node_uuid)
            await conn.deny_chat_request(packet.origin_conn_uuid)
        else:
            logger.debug("Incoming connection redirected to single device.")
            device_uuid = self._single_device.uuid if isinstance(self._single_device, device.Device) else self._single_device
            dev_node_uuid = self.engine.uuid if self._single_device_node is None else self._single_device_node
            conn = connection.IPCRoutedConnection.from_packet(self, packet, node_uuid)
            await conn.redirect_chat_request(IPCClassType.DEVICE, device_uuid, dev_node_uuid, packet.origin_conn_uuid)




























