from enum import Enum, unique


__all__ = (
    "CoreEvents",
    "EngineEvents",
    "IPCClassType",
    "IPCPayloadType",
)


# @unique
class CoreEvents(Enum):
    ROUTED_CONN_INCOMING = "ROUTED_CONN_INCOMING"
    ROUTED_CONN_CONNECTION = "ROUTED_CONN_CONNECTION"
    ROUTED_CONN_MESSAGE = "ROUTED_CONN_MESSAGE"


class EngineEvents(Enum):
    # Base Engine Events
    ENGINE_READY = "ENGINE_READY"
    ENGINE_CLOSING = "ENGINE_CLOSING"
    # Node addition/removal.
    WS_NODE_ADDED = "WS_NODE_ADDED"
    NODE_ADDED = "NODE_ADDED"
    WS_NODE_REMOVED = "WS_NODE_REMOVED"
    NODE_REMOVED = "NODE_REMOVED"
    # Role addition/removal.
    EXTERNAL_ROLE_ADDED = "EXTERNAL_ROLE_ADDED"
    LOCAL_ROLE_ADDED = "LOCAL_ROLE_ADDED"
    ROLE_ADDED = "ROLE_ADDED"
    EXTERNAL_ROLE_REMOVED = "EXTERNAL_ROLE_REMOVED"
    LOCAL_ROLE_REMOVED = "LOCAL_ROLE_REMOVED"
    ROLE_REMOVED = "ROLE_REMOVED"
    # Device addition/removal.
    EXTERNAL_DEVICE_ADDED = "EXTERNAL_DEVICE_ADDED"
    LOCAL_DEVICE_ADDED = "LOCAL_DEVICE_ADDED"
    DEVICE_ADDED = "DEVICE_ADDED"
    EXTERNAL_DEVICE_REMOVED = "EXTERNAL_DEVICE_REMOVED"
    LOCAL_DEVICE_REMOVED = "LOCAL_DEVICE_REMOVED"
    DEVICE_REMOVED = "DEVICE_REMOVED"
    # Packet handling.
    WS_PACKET = "WS_PACKET"
    LOCAL_PACKET = "LOCAL_PACKET"
    PACKET = "PACKET"


@unique
class IPCClassType(Enum):
    ENGINE = 0
    ROLE = 1
    DEVICE = 2


@unique
class IPCPayloadType(Enum):
    COMMUNICATION = 0
    """Valid destination: ENGINE, ROLE, DEVICE
    
    Packet data: IPCPacket dictionary
    """
    DISCOVERY = 1
    """Valid destination: ENGINE

    Packet data: "Node UUID"
    """
    ROLE_ADD = 2
    """Valid destination: ENGINE

    Packet data: "Role name here"
    """
    ROLE_REMOVE = 3
    """Valid destination: ENGINE

    Packet data: "Role name here"
    """
    DEVICE_ADD = 4
    """Valid destination: ENGINE

    Packet data: {"uuid": "Device UUID Here", "role": "Role name here"}
    """
    DEVICE_REMOVE = 5
    """Valid destination: ENGINE

    Packet data: {"uuid": "Device UUID Here", "role": "Role name here"}
    """
    COMMUNICATION_REQUEST = 6
    """Valid destination: ENGINE, ROLE, DEVICE
    
    Packet data: None
    """
    COMMUNICATION_ACCEPTED = 7
    """Valid destination: ENGINE, ROLE, DEVICE (specifically in response to a COMMUNICATION_REQUEST)
    
    Packet data: UUID of the REQUESTing IPCConnection
    """
    COMMUNICATION_DENIED = 8
    """Valid destination: ENGINE, ROLE, DEVICE (specifically in response to a COMMUNICATION_REQUEST)

    Packet data: UUID of the REQUESTing IPCConnection
    """
    COMMUNICATION_REDIRECT = 9
    """Valid destination: ENGINE, ROLE, DEVICE (specifically, origin_type of the IPCConnection to redirect)
    
    Packet data: {
        "destination_type": IPCClassType.value, 
        "destination_name": "UUID/name here", 
        "destination_node": "Node UUID"
    }
    """
