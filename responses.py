from typing import Any, Dict, List, Set
from constants import *


def _response_fields_mapper(res_bytes: bytes, fields: Set[int]):
    total_bytes = len(res_bytes)

    offset = INT_SIZE  # error code size

    field_values: Dict[str, Any] = {}

    while offset < total_bytes:
        res_key = int.from_bytes(
            bytes=res_bytes[offset : (offset + INT_SIZE)],
            byteorder=ENDIAS,
            signed=False,
        )

        if res_key not in fields:
            raise Exception(f"Incorrect response key {res_key}")

        offset += INT_SIZE

        if res_key == OK:
            field_values["success"] = bool.from_bytes(
                bytes=res_bytes[offset : (offset + BOOL_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += BOOL_SIZE
        elif res_key == LEADER_ID:
            field_values["leader_id"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE
        elif res_key == TOTAL_PARTITIONS:
            field_values["total_partitions"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE
        elif res_key == CONTROLLER_CONNECTION_INFO or res_key == PARTITION_NODE_CONNECTION_INFO:
            node = {}

            key_to_use = "controller_nodes"

            if res_key == PARTITION_NODE_CONNECTION_INFO:
                node["partition_id"] = int.from_bytes(
                    bytes=res_bytes[offset : (offset + INT_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                offset += INT_SIZE

                key_to_use = "partition_leader_nodes"

            node["node_id"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            address_size: int = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            node["address"] = res_bytes[offset : (offset + address_size)].decode(
                "utf-8"
            )

            offset += address_size

            node["port"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            if key_to_use not in field_values:
                field_values[key_to_use] = [node]
            else:
                field_values[key_to_use].append(node)
        elif res_key == CONSUMER_ID:
            field_values["consumer_id"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE
        elif res_key == ASSIGNED_PARTITIONS:
            total_partitions = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            assigned_partitions: list[int] = []

            for i in range(total_partitions):
                assigned_partitions.append(
                    int.from_bytes(
                        bytes=res_bytes[offset : (offset + INT_SIZE)],
                        byteorder=ENDIAS,
                        signed=False,
                    )
                )

            field_values["assigned_partitions"] = assigned_partitions
        elif res_key == MESSAGES:
            total_messages = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            total_messages_bytes = int.from_bytes(
                bytes=res_bytes[offset : (offset + INT_SIZE)],
                byteorder=ENDIAS,
                signed=False,
            )

            offset += INT_SIZE

            field_values["total_messages_bytes"] = total_messages_bytes

            if total_messages == 0:
                field_values["messages"] = []
                continue

            for i in range(total_messages):
                nessage_bytes = int.from_bytes(
                    bytes=res_bytes[offset : (offset + INT_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                message = {}

                message["offset"] = int.from_bytes(
                    bytes=res_bytes[(offset + MESSAGE_ID_OFFSET) : (offset + MESSAGE_ID_OFFSET + LONG_LONG_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                message["timestamp"] = int.from_bytes(
                    bytes=res_bytes[(offset + MESSAGE_TIMESTAMP_OFFSET) : (offset + MESSAGE_TIMESTAMP_OFFSET + LONG_LONG_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                key_size = int.from_bytes(
                    bytes=res_bytes[(offset + MESSAGE_KEY_SIZE_OFFSET) : (offset + MESSAGE_KEY_SIZE_OFFSET + INT_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                payload_size = int.from_bytes(
                    bytes=res_bytes[(offset + MESSAGE_PAYLOAD_SIZE_OFFSET) : (offset + MESSAGE_PAYLOAD_SIZE_OFFSET + INT_SIZE)],
                    byteorder=ENDIAS,
                    signed=False,
                )

                message["key"] = res_bytes[(offset + MESSAGE_METADATA_END_OFFSET) : (offset + MESSAGE_METADATA_END_OFFSET + key_size)] if key_size > 0 else None
                message["payload"] = res_bytes[(offset + MESSAGE_METADATA_END_OFFSET + key_size) : (offset + MESSAGE_METADATA_END_OFFSET + key_size + payload_size)] if payload_size > 0 else None

                offset += nessage_bytes

    return field_values


class ControllerConnectionInfo:
    def __init__(self, node_id: int, address: str, port: int):
        self.node_id: int = node_id
        self.address: str = address
        self.port: int = port

    def __str__(self):
        return (
            "{"
            + f'"node_id": {self.node_id}, "address": "{(self.address if self.address != "127.0.0.1" else "localhost")}:{self.port}"'
            + "}"
        )
    
class PartitionLeaderConnectionInfo:
    def __init__(self, partition_id: int, node_id: int, address: str, port: int):
        self.partition_id: int = partition_id
        self.node_id: int = node_id
        self.address: str = address
        self.port: int = port

    def __str__(self):
        return (
            "{"
            + f'"partition_id": {self.partition_id}, "node_id": {self.node_id}, "address": "{(self.address if self.address != "127.0.0.1" else "localhost")}:{self.port}"'
            + "}"
        )


class GetControllersConnectionInfoResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(
            res_bytes, set([LEADER_ID, CONTROLLER_CONNECTION_INFO])
        )

        self.leader_id: int = res_fields["leader_id"] if "leader_id" in res_fields else -1
        self.controller_nodes: List[ControllerConnectionInfo] = []

        if "controller_nodes" not in res_fields["controller_nodes"]:
            res_fields["controller_nodes"] = []

        for controller_info in res_fields["controller_nodes"]:
            self.controller_nodes.append(
                ControllerConnectionInfo(
                    node_id=controller_info["node_id"],
                    address=controller_info["address"],
                    port=controller_info["port"],
                )
            )


class GetLeaderControllerIdResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([LEADER_ID]))

        self.leader_id: int = res_fields["leader_id"] if "leader_id" in res_fields else -1


class CreateQueueResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK]))

        self.success: bool = res_fields["success"] if "success" in res_fields else False

class DeleteQueueResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK]))

        self.success: bool = res_fields["success"] if "success" in res_fields else False

class GetQueuePartitionInfoResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(
            res_bytes, set([TOTAL_PARTITIONS, PARTITION_NODE_CONNECTION_INFO])
        )

        self.total_partitions: int = res_fields["total_partitions"] if "total_partitions" in res_fields else 0
        self.partition_leader_nodes: List[PartitionLeaderConnectionInfo] = []

        if "partition_leader_nodes" not in res_fields:
            res_fields["partition_leader_nodes"] = []

        for node_info in res_fields["partition_leader_nodes"]:
            self.partition_leader_nodes.append(
                PartitionLeaderConnectionInfo(
                    partition_id=node_info["partition_id"],
                    node_id=node_info["node_id"],
                    address=node_info["address"],
                    port=node_info["port"],
                )
            )

class ProduceMessagesResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK]))

        self.success: bool = res_fields["success"] if "success" in res_fields else False

class RegisterConsumerResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK, CONSUMER_ID]))

        self.success: bool = res_fields["success"] if "success" in res_fields else False
        self.consumer_id: int = res_fields["consumer_id"] if "consumer_id" in res_fields else -1

class GetConsumerAssignedPartitions:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([ASSIGNED_PARTITIONS]))

        self.assigned_partitions: list[int] = []

        if "assigned_partitions" not in "assigned_partitions":
            res_fields["assigned_partitions"] = []

        for partition in res_fields["assigned_partitions"]:
            self.assigned_partitions.append(partition)

class Message:
    def __init__(self, payload: bytes, key: bytes | None, offset: int, timestamp: int):
        self.key: bytes | None = None
        self.payload: bytes = []
        self.offset: int = 0
        self.timestamp: int = 0
        self.partition: int = -1

class ConsumeMessagesResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([MESSAGES]))

        self.messages: List[Message] | None = None

        if "messages" in res_fields:
            self.messages = [
                Message(
                    payload=message["payload"],
                    key=message["key"],
                    offset=message["offset"],
                    timestamp=message["timestamp"]
                )
                for message in res_fields["messages"]
            ]