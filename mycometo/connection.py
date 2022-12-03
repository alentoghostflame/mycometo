from __future__ import annotations

from aiohttp import web
from logging import getLogger
from typing import AsyncIterator, TYPE_CHECKING

import aiohttp
import asyncio
import uuid

from .enums import CoreEvents, EngineEvents, IPCClassType, IPCPayloadType
from .core import DispatchFramework


if TYPE_CHECKING:
    from .engine import IPCEngine
    from .role import Role
    from .device import Device


__all__ = (
    "get_requestor_info",
    "IPCRawConnection",
    "IPCRoutedConnection",
    "IPCCore",
    "IPCPacket",
)


logger = getLogger(__name__)


ConnDataTypes = str | dict | bytes | None


class IPCPacket:
    def __init__(
            self,
            *,
            payload_type: IPCPayloadType,
            origin_type: IPCClassType,
            origin_name: str | None,
            origin_role: str | None,
            dest_type: IPCClassType,
            dest_name: str | None,
            data: ConnDataTypes,
            origin_conn_uuid: str | None = None,
            dest_conn_uuid: str | None = None,
            event: str | None = None,
            packet_uuid: str | None = None,
            is_response: bool = False,
    ):
        self.type = payload_type
        self.origin_type = origin_type
        """IPC Class Type that the packet is being sent from."""
        self.origin_name = origin_name
        """Role name for Role origin, Device UUID for Device origin, node UUID for Engine origin."""
        self.origin_role = origin_role
        """Role name of the Device for Device origin. None for Role and Engine origin."""
        self.destination_type = dest_type
        """IPC Class Type that the packet is being sent to."""
        self.destination_name = dest_name
        """Role name for Role origin, Device UUID for Device origin, node UUID for Engine origin."""
        self.data = data
        """Actual data to be transmitted in the packet."""
        self.origin_conn_uuid = origin_conn_uuid
        """Used to indicate what connection a specific packet is from."""
        self.dest_conn_uuid = dest_conn_uuid
        """UUID of the target connection, typically used to indicate that the specific connection should be modified."""
        self.event = event
        """Event that the destination should broadcast with the packet and origin Node UUID as the args."""

        self._uuid = packet_uuid
        """UUID of the packet. Should only be sent if this packet is requesting or responding."""
        self._is_response = is_response
        """Indicates if this packet is a new request or a response to an existing request."""

        self._conn = None

    @property
    def is_request(self) -> bool:
        return self._uuid is not None and self.is_response is False

    @property
    def uuid(self) -> str | None:
        return self._uuid

    @property
    def is_response(self) -> bool:
        return self._is_response

    def set_connection(self, conn: IPCRawConnection):
        self._conn = conn

    @classmethod
    def from_connection(
            cls,
            conn: IPCRawConnection,
            payload_type: IPCPayloadType,
            data: ConnDataTypes,
            *,
            event: str | None = None,
            packet_uuid: str | None = None,
            is_response: bool = False,
    ) -> IPCPacket:
        ret = cls(
            payload_type=payload_type,
            origin_type=conn.origin_type,
            origin_name=conn.origin_name,
            origin_role=conn.origin_role,
            dest_type=conn.dest_type,
            dest_name=conn.dest_name,
            data=data,
            origin_conn_uuid=conn.uuid,
            event=event,
            packet_uuid=packet_uuid,
            is_response=is_response,
        )
        return ret

    def to_dict(self) -> dict:
        ret = {
            "type": self.type.value,
            "origin_type": self.origin_type.value,
            "origin_name": self.origin_name,
            "origin_role": self.origin_role,
            "destination_type": self.destination_type.value,
            "destination_name": self.destination_name,
            "data": self.data,
            "origin_conn_uuid": self.origin_conn_uuid,
            "dest_conn_uuid": self.dest_conn_uuid,
            "event": self.event,
            "_uuid": self._uuid,
            "_is_response": self._is_response,
        }
        return ret

    @classmethod
    def from_dict(cls, packet: dict) -> IPCPacket:
        ret = cls(
            payload_type=IPCPayloadType(packet["type"]),
            origin_type=IPCClassType(packet["origin_type"]),
            origin_name=packet["origin_name"],
            origin_role=packet["origin_role"],
            dest_type=IPCClassType(packet["destination_type"]),
            dest_name=packet["destination_name"],
            data=packet["data"],
            origin_conn_uuid=packet["origin_conn_uuid"],
            dest_conn_uuid=packet["dest_conn_uuid"],
            event=packet["event"],
            packet_uuid=packet["_uuid"],
            is_response=packet["_is_response"]
        )
        return ret

    async def send_response(self, data: ConnDataTypes):
        if self.uuid is None:
            raise ValueError("A response requires the request packet to have a UUID.")
        elif not isinstance(self._conn, IPCRoutedConnection):
            raise ValueError("Responses can only be performed by an IPCChat object.")
        else:
            await self._conn.send_response(data, self.uuid)


class IPCRawConnection:
    class ConnectionClosed(Exception):
        pass

    def __init__(
            self,
            engine: IPCEngine,
            conn: web.WebSocketResponse | aiohttp.ClientWebSocketResponse | None,
            origin_type: IPCClassType,
            origin_name: str,
            origin_role: str | None,
            dest_node: str,
            dest_type: IPCClassType,
            dest_name: str,
            uuid_override: str | None = None,
    ):
        """A "raw" connection to the given destination. Can only send packets, has no receiving capabilities.

        Parameters
        ----------
        engine
        conn
        origin_type
        origin_name
        origin_role
        dest_node
        dest_type
        dest_name
        uuid_override
        """
        self._engine = engine
        self._conn = conn
        self._origin_type = origin_type
        self._origin_name = origin_name
        self._origin_role = origin_role
        self._dest_node = dest_node
        self._dest_type = dest_type
        self._dest_name = dest_name
        self._uuid = uuid_override or uuid.uuid4().hex
        self._is_open: bool = False

    @property
    def origin_type(self) -> IPCClassType:
        return self._origin_type

    @property
    def origin_name(self) -> str:
        return self._origin_name

    @property
    def origin_role(self) -> str | None:
        return self._origin_role

    @property
    def dest_node(self) -> str:
        return self._dest_node

    @property
    def dest_type(self) -> IPCClassType:
        return self._dest_type

    @property
    def dest_name(self) -> str:
        return self._dest_name

    @property
    def dest_role(self) -> str | None:
        if self.dest_type == IPCClassType.DEVICE:
            return self._engine.map.full_device_info[self.dest_name][0]
        else:
            return None

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def is_open(self) -> bool:
        return self._is_open

    @classmethod
    def from_packet(cls, requestor: IPCCore, packet: IPCPacket, node_uuid: str) -> IPCRawConnection:
        """Makes a connection to the origin of the given packet."""
        origin_type, origin_name, origin_role = get_requestor_info(requestor)
        conn = requestor.engine.map.resolve_node_conn(node_uuid)
        ret = cls(
            engine=requestor.engine,
            conn=conn,
            origin_type=origin_type,
            origin_name=origin_name,
            origin_role=origin_role,
            dest_node=node_uuid,
            dest_type=packet.origin_type,
            dest_name=packet.origin_name,
        )
        return ret

    async def open(self) -> bool:
        """Opens the connection, allowing the sending of packets and hooks listeners into the engine."""
        if not self.is_open:
            self._is_open = True
            self._engine.events.add_listener(self._on_engine_close, EngineEvents.ENGINE_CLOSING)
            self._engine.events.add_listener(self._on_node_removed, EngineEvents.NODE_REMOVED)
            return True
        else:
            return False

    async def close(self) -> bool:
        """Closes the connection, preventing sending of packets and unhooking listeners."""
        self._is_open = False
        self._engine.events.remove_listener(self._on_engine_close, EngineEvents.ENGINE_CLOSING)
        self._engine.events.remove_listener(self._on_node_removed, EngineEvents.NODE_REMOVED)
        return True

    async def change_dest(self, dest_node: str, dest_type: IPCClassType, dest_name: str | None):
        self._conn = self._engine.map.resolve_node_conn(dest_node)
        self._dest_node = dest_node
        self._dest_type = dest_type
        self._dest_name = dest_name

    async def _on_engine_close(self):
        await self.close()

    async def _on_node_removed(self, node_uuid: str):
        if node_uuid == self.dest_node:
            await self.close()

    async def send_packet(self, packet: IPCPacket, ignore_checks=False):
        # if not ignore_checks and not self.is_open:
        if ignore_checks is False and self.is_open is False:
            raise self.ConnectionClosed("The IPC connection has been closed.")

        if self._conn is None:
            self._engine.events.dispatch(EngineEvents.LOCAL_PACKET, packet)
        else:
            try:
                await self._conn.send_json(packet.to_dict())
            except Exception as e:
                logger.debug(
                    "%s error when communicating with node %s, closing connection.",
                    e.__class__,
                    self._dest_node
                )
                await self.close()
                raise self.ConnectionClosed("The IPC connection has been closed.")

    async def __aenter__(self):
        if self.is_open is False:
            await self.open()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.is_open:
            await self.close()


class IPCRoutedConnection(IPCRawConnection):
    class CommunicationDenied(Exception):
        pass

    _MESSAGE_REPLY = "MESSAGE_REPLY"

    def __init__(
            self,
            *,
            requestor: IPCCore,
            conn: web.WebSocketResponse | aiohttp.ClientWebSocketResponse | None,
            dest_node: str,
            dest_type: IPCClassType,
            dest_name: str,
            dest_chat_uuid: str | None = None,
            uuid_override: str | None = None
    ):
        """A "routed" connection to the destination, designed for 2-way communication.
        Routed Connections must be accepted by the destination before packets can be sent, and can be denied
        or redirected.

        Parameters
        ----------
        requestor:
            Object requesting this connection to be created.
        conn:
            The specific connection to use for communication to the destination.
        dest_node
        dest_type
        dest_name
        dest_chat_uuid
        uuid_override
        """
        origin_type, origin_name, origin_role = get_requestor_info(requestor)
        super().__init__(
            engine=requestor.engine,
            conn=conn,
            origin_type=origin_type,
            origin_name=origin_name,
            origin_role=origin_role,
            dest_node=dest_node,
            dest_type=dest_type,
            dest_name=dest_name,
            uuid_override=uuid_override
        )
        self._requestor = requestor
        self._dest_chat_uuid: str | None = dest_chat_uuid
        self._ready_for_communication: asyncio.Event = asyncio.Event()
        """If the IPCConnection is ready for the user to attempt to use send_chat. This being set does NOT mean that
        it will be successful, only that the user shouldn't wait for the connection to be set.
        """
        self._sent_chat_request: bool = False
        """If we already sent a chat request. If True, no further communication requests should be sent."""
        self._chat_denied: bool = False
        """If the chat request was denied. If True, no further communication data packets should be sent."""
        self._packet_queue: asyncio.Queue[IPCPacket | None] = asyncio.Queue()
        self._events = DispatchFramework()

    @property
    def dest_chat_uuid(self) -> str:
        return self._dest_chat_uuid

    @classmethod
    def from_packet(cls, requestor: IPCCore, packet: IPCPacket, node_uuid: str) -> IPCRoutedConnection:
        conn = requestor.engine.map.resolve_node_conn(node_uuid)
        ret = cls(
            requestor=requestor,
            conn=conn,
            dest_node=node_uuid,
            dest_type=packet.origin_type,
            dest_name=packet.origin_name,
            dest_chat_uuid=packet.origin_conn_uuid
        )

        return ret

    async def wait_until_comm_ready(self):
        """Blocking call that waits until communication is ready."""
        await self._ready_for_communication.wait()
        if self._chat_denied:
            raise self.CommunicationDenied("Communication request has been denied.")

    async def open(self) -> bool:
        """Opens the connection, """
        if self._chat_denied:
            raise self.CommunicationDenied("Communication request has been denied.")

        if await super().open():
            self._is_open = False
            self._requestor.events.add_listener(self.on_ipc_message, CoreEvents.ROUTED_CONN_MESSAGE)
            if self._sent_chat_request is False:
                await self.send_chat_request()

            return True
        else:
            return False

    async def close(self, force: bool = False) -> bool:
        self._is_open = False
        self._requestor.events.remove_listener(self.on_ipc_message, CoreEvents.ROUTED_CONN_MESSAGE)
        self._ready_for_communication.clear()
        await self._packet_queue.put(None)
        return True

    async def send_chat_request(self):
        """Sends an outgoing chat request, asking permission to communicate."""
        comm_request = IPCPacket.from_connection(self, IPCPayloadType.COMMUNICATION_REQUEST, None)
        comm_request.dest_conn_uuid = self.dest_chat_uuid
        await self.send_packet(comm_request, ignore_checks=True)
        self._sent_chat_request = True

    async def accept_chat_request(self, dest_chat_uuid: str):
        """Accepts the incoming chat request, signalling that we are ready to communicate."""
        packet = IPCPacket.from_connection(self, IPCPayloadType.COMMUNICATION_ACCEPTED, dest_chat_uuid)
        packet.dest_conn_uuid = dest_chat_uuid
        self._dest_chat_uuid = dest_chat_uuid
        self._sent_chat_request = True
        await self.open()
        self._is_open = True
        await self.send_packet(packet)
        self._ready_for_communication.set()

    async def deny_chat_request(self, dest_chat_uuid: str):
        """Denies the incoming chat request, then closes the connection on this side."""
        packet = IPCPacket.from_connection(self, IPCPayloadType.COMMUNICATION_DENIED, dest_chat_uuid)
        packet.dest_conn_uuid = dest_chat_uuid
        self._dest_chat_uuid = dest_chat_uuid
        self._sent_chat_request = True
        await self.open()
        self._is_open = True
        await self.send_packet(packet)
        self._chat_denied = True
        self._ready_for_communication.set()
        await self.close()

    async def redirect_chat_request(self, dest_type: IPCClassType, dest_name: str, dest_node: str, dest_chat_uuid: str):
        """Redirects the incoming chat request to a new destination, then closes the connection on this side."""
        payload = {
            "destination_type": dest_type.value, "destination_name": dest_name, "destination_node": dest_node
        }
        packet = IPCPacket.from_connection(self, IPCPayloadType.COMMUNICATION_REDIRECT, payload)
        packet.dest_conn_uuid = dest_chat_uuid
        self._dest_chat_uuid = dest_chat_uuid
        self._sent_chat_request = True
        await self.open()
        self._is_open = True
        await self.send_packet(packet)
        self._chat_denied = True
        self._ready_for_communication.set()
        await self.close()

    async def send_data(
            self,
            data: ConnDataTypes,
            event: str | None = None,
            *,
            set_dest: bool = True,
            payload_type: IPCPayloadType = IPCPayloadType.COMMUNICATION
    ):
        """Makes and sends a packet with the given data."""
        packet = IPCPacket.from_connection(self, payload_type, data=data, event=event)
        if set_dest:
            packet.dest_conn_uuid = self.dest_chat_uuid

        await self.send_packet(packet)

    async def send_request(
            self,
            data: ConnDataTypes,
            event: str | int | None = None,
            timeout: float = 5.0
    ) -> IPCPacket:
        """Sends a communication data packet, expecting a response from it."""
        packet_uuid = uuid.uuid1().hex
        packet = IPCPacket.from_connection(
            self,
            IPCPayloadType.COMMUNICATION,
            data,
            event=event,
            packet_uuid=packet_uuid
        )
        packet.dest_conn_uuid = self.dest_chat_uuid
        await self.send_packet(packet)
        # logger.critical("WAITING FOR P.UUID TO EQUAL %s", packet_uuid)
        return await self._events.wait_for(self._MESSAGE_REPLY, lambda p: p.uuid == packet_uuid, timeout=timeout)

    async def send_response(self, data: ConnDataTypes, request_packet_uuid: str):
        """Sends a response packet with the given data, responding to the given packet UUID."""
        packet = IPCPacket.from_connection(
            self, IPCPayloadType.COMMUNICATION, data, packet_uuid=request_packet_uuid, is_response=True
        )
        packet.dest_conn_uuid = self.dest_chat_uuid
        await self.send_packet(packet)

    async def receive(self) -> IPCPacket | None:
        """Waits until either a new packet arrives or the IPC connection closes."""
        if self._conn is not None and self._conn.closed:
            raise self.ConnectionClosed("The IPC connection has been closed.")

        ret = await self._packet_queue.get()
        self._packet_queue.task_done()
        if ret is None:
            raise self.ConnectionClosed("The IPC connection has been closed.")

        return ret

    async def __aenter__(self):
        await super().__aenter__()
        await self.wait_until_comm_ready()
        return self

    def __aiter__(self) -> AsyncIterator[IPCPacket]:
        return self

    async def __anext__(self) -> IPCPacket:
        if self.is_open:
            try:
                return await self.receive()
            except self.ConnectionClosed:
                raise StopAsyncIteration

        else:
            raise StopAsyncIteration

    async def on_ipc_message(self, packet: IPCPacket, origin_node: str | None):
        if packet.dest_conn_uuid == self.uuid:
            packet.set_connection(self)
            if self.is_open:
                if packet.type is IPCPayloadType.COMMUNICATION:
                    if packet.is_response:
                        self._events.dispatch(self._MESSAGE_REPLY, packet)
                    elif packet.event:
                        self._requestor.events.dispatch(packet.event, packet)
                    else:
                        await self._packet_queue.put(packet)
            elif self._chat_denied is False:
                match packet.type:
                    case IPCPayloadType.COMMUNICATION_ACCEPTED:
                        self._is_open = True
                        self._dest_chat_uuid = packet.origin_conn_uuid
                        self._ready_for_communication.set()
                    case IPCPayloadType.COMMUNICATION_DENIED:
                        self._chat_denied = True
                        self._ready_for_communication.set()
                        await self.close(force=True)
                    case IPCPayloadType.COMMUNICATION_REDIRECT:
                        dest_type = IPCClassType(packet.data["destination_type"])
                        dest_name = packet.data["destination_name"]
                        dest_node = packet.data["destination_node"]
                        await self.change_dest(dest_node, dest_type, dest_name)
                        await self.send_chat_request()


class IPCCore:
    engine: IPCEngine

    def __init__(self):
        """Contains the core bits required for IPC shared between the Engine, Role, and Device."""
        self.events = DispatchFramework()
        self.events.add_listener(self.on_incoming_chat, CoreEvents.ROUTED_CONN_INCOMING)
        self.events.add_listener(self.on_chat_connection, CoreEvents.ROUTED_CONN_CONNECTION)

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}>"

    async def on_incoming_chat(self, packet: IPCPacket, node_uuid: str):
        """Ran when an incoming connection is established but needs to be accepted."""
        logger.debug("Received incoming connection from node %s.", node_uuid)
        if await self.accept_incoming_chat(packet, node_uuid):
            logger.debug("Incoming connection locally accepted. Creating conn, sending accept, and dispatching.")
            conn = IPCRoutedConnection.from_packet(self, packet, node_uuid)
            await conn.accept_chat_request(packet.origin_conn_uuid)
            self.events.dispatch(CoreEvents.ROUTED_CONN_CONNECTION, conn)
        else:
            logger.debug("Incoming connection locally denied. Creating conn, sending deny, and ignoring.")
            conn = IPCRoutedConnection.from_packet(self, packet, node_uuid)
            await conn.deny_chat_request(packet.origin_conn_uuid)

    # noinspection PyMethodMayBeStatic
    async def accept_incoming_chat(self, packet: IPCPacket, node_uuid: str) -> bool:
        """Used for custom logic for if a connection should be accepted or not. Return True to accept it, False to
        deny the connection.
        """
        return True

    async def on_chat_connection(self, chat: IPCRoutedConnection):
        """Ran when an incoming connection is established and accepted."""
        await chat.close()
        raise NotImplementedError


# Importing all the way down here helps prevent import errors.
from . import engine, role, device


def get_requestor_info(requestor: IPCEngine | Role | Device) -> tuple[IPCClassType, str, str | None]:
    """Grabs the origin type, name, and role from the given requestor."""
    if isinstance(requestor, engine.IPCEngine):
        origin_type = IPCClassType.ENGINE
        origin_name = requestor.uuid
        origin_role = None
    elif isinstance(requestor, role.Role):
        origin_type = IPCClassType.ROLE
        origin_name = requestor.name
        origin_role = None
    elif isinstance(requestor, device.Device):
        origin_type = IPCClassType.DEVICE
        origin_name = requestor.uuid
        origin_role = requestor.role.name
    else:
        raise ValueError("Requester type %s is not supported.", type(requestor))

    return origin_type, origin_name, origin_role
