from __future__ import annotations

import aiohttp
import asyncio
import signal
import uuid

from aiohttp import web
from logging import getLogger
from typing import Coroutine, TYPE_CHECKING

from .connection import IPCRawConnection, IPCRoutedConnection, IPCCore, IPCPacket, get_requestor_info
from .enums import CoreEvents, EngineEvents, IPCClassType, IPCPayloadType


if TYPE_CHECKING:
    from .device import Device
    from .role import Role


__all__ = (
    "ConnectionMap",
    "IPCEngine",
)


logger = getLogger(__name__)


CONNECT_RETRY_SLEEP = 10  # Time in seconds.
ENGINE_IPC_ROUTE = "engine/ipc"
ON_SIGNAL_CLOSE = (signal.SIGINT, signal.SIGTERM)
"""On what signals should the engine attempt to close itself with."""


class ConnectionMap:

    class MISSING:
        pass

    def __init__(self, engine: IPCEngine):
        self._engine = engine

        self._remote_nodes: dict[str, web.WebSocketResponse | aiohttp.ClientWebSocketResponse] = {}
        """{"Node UUID": (Client)WebSocketResponse object}"""
        self._remote_roles: dict[str, set[str]] = {}
        """{"Role Name": {"Node UUID 1", "Node UUID 2", "etc."}}"""
        self._remote_devices: dict[str, tuple[str, str]] = {}
        """{"Device UUID": ("Role Name", "Node UUID")}"""

    @property
    def remote_nodes(self) -> dict[str, web.WebSocketResponse | aiohttp.ClientWebSocketResponse]:
        return self._remote_nodes.copy()

    @property
    def remote_roles(self) -> dict[str, set[str]]:
        return self._remote_roles.copy()

    @property
    def remote_devices(self) -> dict[str, tuple[str, str]]:
        return self._remote_devices.copy()

    @property
    def local_roles(self) -> set[str]:
        # return self._local_roles.copy()
        return set(self._engine._roles.keys())

    @property
    def local_devices(self) -> set:
        ret = set()
        for role in self._engine._roles.values():
            for device in role.local_devices.values():
                ret.add(device.uuid)

        return ret

    @property
    def nodes(self) -> list[str]:
        return list(self.remote_nodes.keys()) + [self._engine.uuid, ]

    @property
    def roles(self) -> list[str]:
        return list(self.local_roles) + list(self._remote_roles.keys())

    @property
    def devices(self) -> list[str]:
        return list(self.local_devices) + list(self._remote_devices.keys())

    @property
    def full_device_info(self) -> dict[str, tuple[str, str]]:
        ret = self._remote_devices.copy()
        ret.update({
            device_uuid: (device.role.name, self._engine.uuid) for device_uuid, device in self._engine.devices.items()
        })
        return ret

    def clean(self):
        for node_uuid, ws in self.remote_nodes.items():
            if ws.closed:
                self.remove_node(node_uuid)

    def add_node(self, ws: web.WebSocketResponse | aiohttp.ClientWebSocketResponse, node_uuid: str) -> bool:
        if node_uuid in self._remote_nodes:
            return False
        else:
            self._remote_nodes[node_uuid] = ws
            return True

    def remove_node(self, rm_node_uuid: str) -> bool:
        if rm_node_uuid not in self._remote_nodes:
            return False

        logger.debug("Fully removing node %s from NodeMap.", rm_node_uuid)

        # Removes the node UUID from the dict of remote roles.
        for role_name, node_list in self._remote_roles.items():
            if rm_node_uuid in node_list:
                node_list.remove(rm_node_uuid)
                self._engine.events.dispatch(EngineEvents.EXTERNAL_ROLE_REMOVED, role_name, rm_node_uuid)

        # Removes devices from the dict if the node they were attached to is no longer connected.
        for device_uuid, role_node in self.remote_devices.items():
            if role_node[1] == rm_node_uuid:
                logger.debug("Removing device %s", device_uuid)
                self._remote_devices.pop(device_uuid)
                self._engine.events.dispatch(EngineEvents.EXTERNAL_DEVICE_REMOVED, device_uuid, role_node)

        # Finally, removes the node UUID and websocket object for it from the dict.
        self._remote_nodes.pop(rm_node_uuid)
        self._engine.events.dispatch(EngineEvents.NODE_REMOVED, rm_node_uuid)

        return True

    def add_external_role(self, role_name: str, node_uuid: str):
        if node_uuid in self._remote_nodes:
            if role_name not in self._remote_roles:
                self._remote_roles[role_name] = set()

            self._remote_roles[role_name].add(node_uuid)
            logger.debug("Added Node %s to list for external role %s", node_uuid, role_name)
        else:
            raise ValueError(f"Node UUID {node_uuid} not found, cannot add it to the set for role {role_name}.")

    def add_external_device(self, device_uuid: str, device_role: str, node_uuid: str):
        if node_uuid in self._remote_nodes:
            self._remote_devices[device_uuid] = (device_role, node_uuid)
        else:
            raise ValueError(
                f"Node UUID {node_uuid} not found, cannot add device with UUID {device_uuid} and role {device_role}."
            )

    def resolve_node_conn(self, node_uuid: str) -> aiohttp.ClientWebSocketResponse | web.WebSocketResponse | None:
        if node_uuid == self._engine.uuid:
            return None
        elif node_uuid in self._remote_nodes:
            return self._remote_nodes[node_uuid]
        else:
            raise ValueError(f"Node with UUID {node_uuid} not found.")

    def resolve_role_conn(
            self,
            role_name: str,
            prefer_self: bool = True
    ) -> tuple[str, aiohttp.ClientWebSocketResponse | web.WebSocketResponse | None]:
        if prefer_self and role_name in self.local_roles:
            return self._engine.uuid, None
        elif role_name in self.remote_roles and len(self.remote_roles[role_name]) > 0:
            node_uuid = list(self.remote_roles[role_name])[0]
            return node_uuid, self.resolve_node_conn(node_uuid)
        else:
            raise ValueError(f"No nodes with Role {role_name} found.")

    def resolve_device_conn(
            self,
            device_uuid: str,
    ) -> tuple[str, aiohttp.ClientWebSocketResponse | web.WebSocketResponse | None]:
        if device_uuid in self.local_devices:
            return self._engine.uuid, None
        elif role_node := self._remote_devices.get(device_uuid):
            return role_node[1], self._remote_nodes[role_node[1]]
        else:
            raise ValueError(f"Device with UUID {device_uuid} not found.")

    def resolve_conn(
            self,
            uuid_or_name: str,
            prefer_self: bool = True,
    ) -> tuple[str, aiohttp.ClientWebSocketResponse | web.WebSocketResponse | None]:
        if uuid_or_name in self.nodes:
            return self.resolve_device_conn(uuid_or_name)
        elif uuid_or_name in self.roles:
            return self.resolve_role_conn(uuid_or_name, prefer_self)
        elif uuid_or_name in self.devices:
            return self.resolve_device_conn(uuid_or_name)
        else:
            raise ValueError(f"No Nodes, Roles, or Devices found with the UUID or name of {uuid_or_name}")

    def get_packet_destination(self, packet: IPCPacket) -> IPCEngine | Role | Device:
        if packet.destination_type is IPCClassType.ENGINE:
            return self._engine
        elif packet.destination_type is IPCClassType.ROLE:
            return self._engine.roles[packet.destination_name]
        elif packet.destination_type is IPCClassType.DEVICE:
            return self._engine.devices[packet.destination_name]
        else:
            raise ValueError(f"Cannot handle destination type %s.", packet.destination_type)

    def get_destination_info_of(self, uuid_or_name: str) -> tuple[
        aiohttp.ClientWebSocketResponse | web.WebSocketResponse | None,
        str,
        IPCClassType,
        str
    ]:
        if uuid_or_name in self.nodes:
            conn = self.resolve_node_conn(uuid_or_name)
            dest_node = uuid_or_name
            dest_type = IPCClassType.ENGINE
            dest_name = uuid_or_name
        elif uuid_or_name in self.roles:
            dest_node, conn = self.resolve_role_conn(uuid_or_name)
            dest_type = IPCClassType.ROLE
            dest_name = uuid_or_name
        elif uuid_or_name in self.devices:
            dest_node, conn = self.resolve_device_conn(uuid_or_name)
            dest_type = IPCClassType.DEVICE
            dest_name = uuid_or_name
        else:
            raise ValueError(f"No Nodes, Roles, or Devices found with the UUID or name of {uuid_or_name}")

        return conn, dest_node, dest_type, dest_name

    def raw_conn_to(
            self,
            requestor: IPCEngine | Role | Device,
            uuid_or_name: str
    ) -> IPCRawConnection:

        conn, dest_node, dest_type, dest_name = self.get_destination_info_of(uuid_or_name)
        origin_type, origin_name, origin_role = get_requestor_info(requestor)

        ret = IPCRawConnection(
            engine=self._engine,
            conn=conn,
            origin_type=origin_type,
            origin_name=origin_name,
            origin_role=origin_role,
            dest_node=dest_node,
            dest_type=dest_type,
            dest_name=dest_name
        )
        return ret

    def routed_conn_to(
            self,
            requestor: IPCEngine | Role | Device,
            uuid_or_name: str,
            dest_chat_uuid: str | None = None
    ):
        conn, dest_node, dest_type, dest_name = self.get_destination_info_of(uuid_or_name)
        ret = IPCRoutedConnection(
            requestor=requestor,
            conn=conn,
            dest_node=dest_node,
            dest_type=dest_type,
            dest_name=dest_name,
            dest_chat_uuid=dest_chat_uuid,
        )
        return ret


class IPCEngine(IPCCore):
    def __init__(self, uuid_override: str | None = None):
        super().__init__()
        self._uuid: str = uuid_override or uuid.uuid1().hex
        self._roles: dict[str, Role] = {}
        self.map: ConnectionMap = ConnectionMap(self)
        self.session: aiohttp.ClientSession | None = None
        self.running: asyncio.Event = asyncio.Event()

        self.events.add_listener(self.on_engine_ready, EngineEvents.ENGINE_READY)
        self.events.add_listener(self.on_engine_closing, EngineEvents.ENGINE_CLOSING)

        self.events.add_listener(self.on_ws_node_added, EngineEvents.WS_NODE_ADDED)
        self.events.add_listener(self.on_node_added, EngineEvents.NODE_ADDED)
        self.events.add_listener(self.on_ws_node_removed, EngineEvents.WS_NODE_REMOVED)
        self.events.add_listener(self.on_node_removed, EngineEvents.NODE_REMOVED)

        self.events.add_listener(self.on_external_role_added, EngineEvents.EXTERNAL_ROLE_ADDED)
        self.events.add_listener(self.on_local_role_added, EngineEvents.LOCAL_ROLE_ADDED)
        self.events.add_listener(self.on_role_added, EngineEvents.ROLE_ADDED)
        self.events.add_listener(self.on_external_role_removed, EngineEvents.EXTERNAL_ROLE_REMOVED)
        self.events.add_listener(self.on_local_role_removed, EngineEvents.LOCAL_ROLE_REMOVED)
        self.events.add_listener(self.on_role_removed, EngineEvents.ROLE_REMOVED)

        self.events.add_listener(self.on_external_device_added, EngineEvents.EXTERNAL_DEVICE_ADDED)
        self.events.add_listener(self.on_local_device_added, EngineEvents.LOCAL_DEVICE_ADDED)
        self.events.add_listener(self.on_device_added, EngineEvents.DEVICE_ADDED)
        self.events.add_listener(self.on_external_device_removed, EngineEvents.EXTERNAL_DEVICE_REMOVED)
        self.events.add_listener(self.on_local_device_removed, EngineEvents.LOCAL_DEVICE_REMOVED)
        self.events.add_listener(self.on_device_removed, EngineEvents.DEVICE_REMOVED)

        self.events.add_listener(self.on_ws_packet, EngineEvents.WS_PACKET)
        self.events.add_listener(self.on_local_packet, EngineEvents.LOCAL_PACKET)
        self.events.add_listener(self.on_packet, EngineEvents.PACKET)
        # self.events.add_listener(self.on_communication, EngineEvents.COMMUNICATION)

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def roles(self) -> dict[str, Role]:
        return self._roles.copy()

    @property
    def devices(self) -> dict[str, Device]:
        ret = {}
        for role in self.roles.values():
            for dev_uuid, device in role.local_devices.items():
                ret[dev_uuid] = device

        return ret

    @property
    def engine(self) -> IPCEngine:
        return self

    def discovery_payload(self) -> IPCPacket:
        ret = IPCPacket(
            payload_type=IPCPayloadType.DISCOVERY,
            origin_type=IPCClassType.ENGINE,
            origin_name=self.uuid,
            origin_role=None,
            dest_type=IPCClassType.ENGINE,
            dest_name=None,
            data=self.uuid,
        )
        return ret

    async def run_stoppable_task(self, coroutine: Coroutine, timeout=0.1) -> asyncio.Task:
        loop = asyncio.get_running_loop()
        task = loop.create_task(coroutine)
        while True:
            try:
                await self.events.wait_for(EngineEvents.ENGINE_CLOSING, timeout=timeout)
            except asyncio.TimeoutError:
                pass
            else:
                logger.debug("Engine is closing, canceling the task.")
                task.cancel()
                break

            if task.done():
                logger.debug("Task %s finished, ending the loop.", coroutine.__name__)
                break

        return task

    async def quick_send_to(self, uuid_or_name: str, packet: IPCPacket):
        node_uuid, conn = self.map.resolve_conn(uuid_or_name)
        if conn is None:
            self.events.dispatch(EngineEvents.LOCAL_PACKET, packet)
        else:
            await conn.send_json(packet.to_dict())

    async def broadcast_to_nodes(self, packet: IPCPacket):
        for node_uuid, ws in self.map.remote_nodes.items():
            packet.destination_type = IPCClassType.ENGINE
            packet.destination_name = node_uuid
            await ws.send_json(packet.to_dict())

    async def update_node(self, node_uuid: str):
        # This prevents roles/devices that are added while running through this from breaking anything.
        current_roles = self.roles
        current_devices = self.devices
        # async with ipc_conn as conn:
        async with self.map.raw_conn_to(self, node_uuid) as conn:
            for role_name in current_roles:
                packet = IPCPacket(
                    payload_type=IPCPayloadType.ROLE_ADD,
                    origin_type=IPCClassType.ENGINE,
                    origin_name=self.uuid,
                    origin_role=None,
                    dest_type=IPCClassType.ENGINE,
                    dest_name=node_uuid,
                    data=role_name,
                )
                await conn.send_packet(packet)

            for device_uuid, device in current_devices.items():
                packet = IPCPacket(
                    payload_type=IPCPayloadType.DEVICE_ADD,
                    origin_type=IPCClassType.ENGINE,
                    origin_name=self.uuid,
                    origin_role=None,
                    dest_type=IPCClassType.ENGINE,
                    dest_name=node_uuid,
                    data={"uuid": device_uuid, "role": device.role.name},
                )
                await conn.send_packet(packet)

    def update_self(self):
        current_roles = self.roles.values()
        current_devices = self.devices.values()

        for role in current_roles:
            self.events.dispatch(EngineEvents.LOCAL_ROLE_ADDED, role)

        for device in current_devices:
            self.events.dispatch(EngineEvents.LOCAL_DEVICE_ADDED, device)

    async def ipc_ws_connect(self, url: str) -> aiohttp.ClientWebSocketResponse | None:
        ws = None
        logger.debug("Starting attempt(s) to establish WS IPC connection to %s", url)
        while not self.session.closed:
            ws = None
            discovered_node = False
            node_uuid = None
            try:
                ws = await self.session.ws_connect(url=url, heartbeat=5)
                await ws.send_json(self.discovery_payload().to_dict())
                async for message in ws:
                    if isinstance(message, aiohttp.WSMessage):
                        if message.type is aiohttp.WSMsgType.text:
                            packet = IPCPacket.from_dict(message.json())
                            if packet.type == IPCPayloadType.DISCOVERY:
                                node_uuid = packet.data
                                self.map.clean()
                                if discovered_node and node_uuid in self.map.nodes:
                                    logger.warning(
                                        "Outgoing Node %s sent a discovery packet despite already being connected. "
                                        "Ignoring it, but is everything okay?", node_uuid
                                    )
                                if self.map.add_node(ws, node_uuid):
                                    logger.debug("Node %s sent a discovery, dispatching event.", node_uuid)
                                    discovered_node = True
                                    self.events.dispatch(EngineEvents.WS_NODE_ADDED, ws, node_uuid)
                                else:
                                    logger.debug(
                                        "Node %s is already connected to us. Closing outgoing connection.", node_uuid
                                    )
                                    await ws.close()
                                    return ws
                            elif discovered_node is False:
                                logger.debug("Non-discovery JSON message received before a discovery one, discarding.")
                            else:
                                self.events.dispatch(EngineEvents.WS_PACKET, ws, packet, node_uuid)
                        else:
                            logger.debug("Unhandled non-text message received, discarding.")

                self.map.clean()

            except (aiohttp.ClientConnectorError, ConnectionRefusedError) as e:
                logger.warning(
                    "Cannot connect or connection lost to %s, waiting %s seconds before retrying.",
                    url,
                    CONNECT_RETRY_SLEEP
                )
                # TODO: More gracefully handle it so _handle_outgoing_ws is also reset?
                await asyncio.sleep(CONNECT_RETRY_SLEEP)

            except aiohttp.WSServerHandshakeError as e:
                match e.status:
                    case 404:
                        logger.error(
                            "Received a 404: %s when connecting to %s, is that route set up for Websockets?",
                            e.message, url
                        )
                        break
                    case _:
                        logger.error(
                            "Received a %s:%s when connecting to %s, no handler available?",
                            e.status, e.message, url
                        )

            except RuntimeError as e:
                logger.exception(
                    "Encountered runtime error while connected to %s, waiting %s seconds before retrying.",
                    url,
                    CONNECT_RETRY_SLEEP,
                    exc_info=e
                )
                await asyncio.sleep(CONNECT_RETRY_SLEEP)

        return ws

    def ipc_middleware(self, route: str = f"/{ENGINE_IPC_ROUTE}"):
        @web.middleware
        async def engine_ipc_middleware(request: web.Request, handler):
            if request.path == route and request.method == "GET":
                return await self.run_stoppable_task(self._ws_ipc_receive_handler(request), timeout=20)
            else:
                return await handler(request)

        return engine_ipc_middleware

    async def _ws_ipc_receive_handler(self, request: web.Request) -> web.WebSocketResponse:
        logger.debug("Incoming WS IPC connection from %s", request.remote)
        discovered_node = False
        node_uuid = None
        ws = web.WebSocketResponse(heartbeat=5)
        await ws.prepare(request)

        await ws.send_json(self.discovery_payload().to_dict())
        async for message in ws:
            if isinstance(message, aiohttp.WSMessage):
                if message.type is aiohttp.WSMsgType.text:
                    packet = IPCPacket.from_dict(message.json())
                    if packet.type == IPCPayloadType.DISCOVERY:
                        node_uuid = packet.data
                        self.map.clean()
                        if self.map.add_node(ws, node_uuid):
                            if discovered_node and node_uuid in self.map.nodes:
                                logger.warning(
                                    "Incoming Node %s sent a discovery packet despite already being connected. "
                                    "Ignoring it, but is everything okay?", node_uuid
                                )
                            else:
                                logger.debug("Incoming Node %s sent a discovery, dispatching event.", node_uuid)
                                discovered_node = True
                                self.events.dispatch(EngineEvents.WS_NODE_ADDED, ws, node_uuid)
                        else:
                            logger.debug(
                                "Incoming Node %s is already connected to us, closing incoming connection.", node_uuid
                            )
                            await ws.close()
                            return ws
                    elif not discovered_node:
                        logger.debug("Non-discovery JSON message was sent before a discovery one, ignoring.")
                    else:
                        self.events.dispatch(EngineEvents.WS_PACKET, ws, packet, node_uuid)

        self.map.clean()

        return ws

    async def on_engine_ready(self):
        logger.debug("IPC Engine is ready.")

    async def on_engine_closing(self):
        logger.debug("IPC Engine is closing.")
        self.running.clear()

    async def on_ws_node_added(self, ws: web.WebSocketResponse | aiohttp.ClientWebSocketResponse, node_uuid: str):
        self.map.add_node(ws, node_uuid)
        self.events.dispatch(EngineEvents.NODE_ADDED, node_uuid)

    async def on_node_added(self, node_uuid: str):
        logger.info("Node %s added.", node_uuid)
        await self.update_node(node_uuid)

    async def on_ws_node_removed(self, ws: web.WebSocketResponse | aiohttp.ClientWebSocketResponse, node_uuid: str):
        self.events.dispatch(EngineEvents.NODE_REMOVED, node_uuid)

    async def on_node_removed(self, node_uuid: str):
        logger.info("Node removed: %s", node_uuid)

    async def on_external_role_added(
            self,
            packet: IPCPacket,
            node_uuid: str
    ):
        self.map.add_external_role(packet.data, node_uuid)
        self.events.dispatch(EngineEvents.ROLE_ADDED, packet.data, node_uuid)

    async def on_local_role_added(self, role: Role):
        self.events.dispatch(EngineEvents.ROLE_ADDED, role.name, self.uuid)
        if self.session:
            packet = IPCPacket(
                payload_type=IPCPayloadType.ROLE_ADD,
                origin_type=IPCClassType.ENGINE,
                origin_name=self.uuid,
                origin_role=None,
                dest_type=IPCClassType.ENGINE,
                dest_name=None,
                data=role.name
            )
            await self.broadcast_to_nodes(packet)

    async def on_role_added(self, role_name: str, node_uuid: str):
        logger.debug("New Role %s from Node %s", role_name, node_uuid)

    async def on_external_role_removed(
            self,
            packet: IPCPacket | str,
            node_uuid: str
    ):
        if isinstance(packet, IPCPacket):
            role_name = packet.data
        elif isinstance(packet, str):
            role_name = packet
        else:
            logger.warning("Unhandled type %s for packet variable", type(packet))
            return

        self.events.dispatch(EngineEvents.ROLE_REMOVED, role_name, node_uuid)

    async def on_local_role_removed(self, role: Role):
        self.events.dispatch(EngineEvents.ROLE_REMOVED, role.name, self.uuid)

    async def on_role_removed(self, role_name: str, node_uuid: str):
        logger.debug("Node %s removed role %s.", node_uuid, role_name)

    async def on_external_device_added(self, packet: IPCPacket, node_uuid: str):
        device_uuid = packet.data["uuid"]
        device_role = packet.data["role"]
        logger.debug("Node %s informed us of device %s with role %s, adding.", node_uuid, device_uuid, device_role)
        self.map.add_external_device(device_uuid, device_role, node_uuid)
        self.events.dispatch(EngineEvents.DEVICE_ADDED, device_uuid, device_role, node_uuid)

    async def on_local_device_added(self, device: Device):
        self.events.dispatch(EngineEvents.DEVICE_ADDED, device.uuid, device.role.name, self.uuid)
        if self.session:
            packet = IPCPacket(
                payload_type=IPCPayloadType.DEVICE_ADD,
                origin_type=IPCClassType.ENGINE,
                origin_name=self.uuid,
                origin_role=None,
                dest_type=IPCClassType.ENGINE,
                dest_name=None,
                data={"uuid": device.uuid, "role": device.role.name}
            )
            await self.broadcast_to_nodes(packet)

    async def on_device_added(self, device_uuid: str, device_role: str, node_uuid: str):
        logger.debug("New device %s with role %s from node %s added.", device_uuid, device_role, node_uuid)

    async def on_external_device_removed(self, packet: IPCPacket | str, node_uuid: str | tuple[str, str]):
        if isinstance(packet, IPCPacket) and isinstance(node_uuid, str):
            device_role = packet.data
            self.events.dispatch(EngineEvents.DEVICE_REMOVED, device_role[0], device_role[1], node_uuid)
        elif isinstance(packet, str) and isinstance(node_uuid, tuple):
            self.events.dispatch(EngineEvents.DEVICE_REMOVED, packet, node_uuid[0], node_uuid[1])
        else:
            logger.warning("Unhandled packet and node_uuid type combination: %s %s", type(packet), type(node_uuid))

    async def on_local_device_removed(self, device: Device):
        self.events.dispatch(EngineEvents.DEVICE_REMOVED, device.uuid, device.role.name, self.uuid)

    async def on_device_removed(self, device_uuid: str, device_role: str, node_uuid: str):
        logger.debug("Node %s removed device %s with role %s.", node_uuid, device_uuid, device_role)

    async def on_ws_packet(
            self,
            ws: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
            packet: IPCPacket,
            node_uuid: str
    ):
        self.events.dispatch(EngineEvents.PACKET, packet, node_uuid)

    async def on_local_packet(self, packet: IPCPacket):
        self.events.dispatch(EngineEvents.PACKET, packet, self.uuid)

    async def on_packet(self, packet: IPCPacket, node_uuid: str):
        match packet.type:
            case IPCPayloadType.COMMUNICATION:
                dest = self.map.get_packet_destination(packet)
                logger.debug("Chat communication received, routing to %s", dest)
                if packet.event is None or packet.dest_conn_uuid is not None:
                    dest.events.dispatch(CoreEvents.ROUTED_CONN_MESSAGE, packet, node_uuid)
                else:
                    dest.events.dispatch(packet.event, packet, node_uuid)
            case IPCPayloadType.COMMUNICATION_REQUEST:
                dest = self.map.get_packet_destination(packet)
                logger.debug("Chat communication request received, routing to %s", dest)
                dest.events.dispatch(CoreEvents.ROUTED_CONN_INCOMING, packet, node_uuid)
                # TODO: Put in logic to route to the correct place.
            case IPCPayloadType.COMMUNICATION_ACCEPTED:
                dest = self.map.get_packet_destination(packet)
                logger.debug("Chat communication acceptance received, routing to %s", dest)
                dest.events.dispatch(CoreEvents.ROUTED_CONN_MESSAGE, packet, node_uuid)
                # TODO: Put in logic to route to the correct place.
            case IPCPayloadType.COMMUNICATION_DENIED:
                dest = self.map.get_packet_destination(packet)
                logger.debug("Chat communication denial received, routing to %s", dest)
                dest.events.dispatch(CoreEvents.ROUTED_CONN_MESSAGE, packet, node_uuid)
                # TODO: Put in logic to route to the correct place.
            case IPCPayloadType.COMMUNICATION_REDIRECT:
                dest = self.map.get_packet_destination(packet)
                logger.debug("Chat communication redirect received, routing to %s", dest)
                dest.events.dispatch(CoreEvents.ROUTED_CONN_MESSAGE, packet, node_uuid)
                # TODO: Put in logic to route to the correct place.
            case IPCPayloadType.DISCOVERY:
                logger.warning("We aren't supposed to be able to handle discovery here?")
            case IPCPayloadType.ROLE_ADD:
                self.events.dispatch(EngineEvents.EXTERNAL_ROLE_ADDED, packet, node_uuid)
            case IPCPayloadType.ROLE_REMOVE:
                self.events.dispatch(EngineEvents.EXTERNAL_ROLE_REMOVED, packet, node_uuid)
            case IPCPayloadType.DEVICE_ADD:
                self.events.dispatch(EngineEvents.EXTERNAL_DEVICE_ADDED, packet, node_uuid)
            case IPCPayloadType.DEVICE_REMOVE:
                self.events.dispatch(EngineEvents.EXTERNAL_DEVICE_REMOVED, packet, node_uuid)
            case _:
                logger.warning(
                    "Unknown WS IPC message type encountered from node %s: %s", node_uuid, packet.type
                )

    def add_role(self, role: Role):
        if role.name in self._roles:
            raise ValueError(f"A role with name {role.name} has already been added.")

        self._roles[role.name] = role
        role.set_engine(self)
        if self.running.is_set():
            packet = IPCPacket(
                payload_type=IPCPayloadType.ROLE_ADD,
                origin_type=IPCClassType.ENGINE,
                origin_name=self.uuid,
                origin_role=None,
                dest_type=IPCClassType.ENGINE,
                dest_name=None,
                data=role.name,
            )
            loop = asyncio.get_running_loop()
            loop.create_task(self.broadcast_to_nodes(packet))

    async def start(self):
        self.running.set()
        self.events.dispatch(EngineEvents.ENGINE_READY)
        self.update_self()
        logger.debug("IPC Engine started.")

    async def close(self, closing_time: float = 1.0):
        self.events.dispatch(EngineEvents.ENGINE_CLOSING)
        await self.session.close()

        await asyncio.sleep(closing_time)

    async def start_serverless(self, *, discover_nodes: list[tuple[str, int]] | None) -> None:
        self.session = aiohttp.ClientSession()
        logger.info("%s starting serverless.", self.__class__.__name__)
        await self.start()
        if discover_nodes:
            loop = asyncio.get_running_loop()
            for node_address, node_port in discover_nodes:
                loop.create_task(self.run_stoppable_task(
                    self.ipc_ws_connect(f"http://{node_address}:{node_port}/{ENGINE_IPC_ROUTE}")
                ))

    def run_serverless(
            self,
            *,
            loop: asyncio.AbstractEventLoop | None = None,
            closing_time: float = 1.0,
            discover_nodes: list[tuple[str, int]] | None
    ):
        loop = loop or asyncio.new_event_loop()
        task = loop.create_task(self.start_serverless(discover_nodes=discover_nodes))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt encountered, stopping loop.")
            logger.debug("Cancelling start task.")
            task.cancel()

            loop.run_until_complete(self.close(closing_time))

    async def start_server(
            self,
            *,
            port: int = 8080,
            discover_nodes: list[tuple[str, int]] | None = None,
    ) -> web.BaseSite:
        self.session = aiohttp.ClientSession()
        app = web.Application(middlewares=[self.ipc_middleware()])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port=port, shutdown_timeout=5.0)
        await site.start()
        logger.info("%s listening on %s.", self.__class__.__name__, site.name)
        await self.start()
        if discover_nodes:
            loop = asyncio.get_running_loop()
            for node_address, node_port in discover_nodes:
                loop.create_task(self.run_stoppable_task(
                    self.ipc_ws_connect(f"http://{node_address}:{node_port}/{ENGINE_IPC_ROUTE}")
                ))

        return site

    def run_server(
            self,
            *,
            loop: asyncio.AbstractEventLoop | None = None,
            port: int = 8080,
            closing_time: float = 1.0,
            discover_nodes: list[tuple[str, int]] | None = None,
    ):
        def on_interrupt():
            logger.info("Stop signal encountered, stopping event loop.")
            loop.stop()

        loop = loop or asyncio.new_event_loop()

        for sig in ON_SIGNAL_CLOSE:
            loop.add_signal_handler(sig, on_interrupt)

        task = loop.create_task(self.start_server(port=port, discover_nodes=discover_nodes))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt encountered, stopping loop.")
        except Exception as e:
            logger.error("Exception occurred that broke out of the event loop, wtf?", exc_info=e)

        if site := task.result():
            logger.debug("Site was created, attempting to stop it.")
            loop.run_until_complete(site.stop())
        else:
            logger.debug("Cancelling start task.")
            task.cancel()

        loop.run_until_complete(self.close(closing_time))
