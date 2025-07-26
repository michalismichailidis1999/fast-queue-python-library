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

        self.leader_id: int = res_fields["leader_id"]
        self.controller_nodes: List[ControllerConnectionInfo] = []

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

        self.leader_id: int = res_fields["leader_id"]


class CreateQueueResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK]))

        self.success: bool = res_fields["success"]

class DeleteQueueResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(res_bytes, set([OK]))

        self.success: bool = res_fields["success"]

class GetQueuePartitionInfoResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = _response_fields_mapper(
            res_bytes, set([TOTAL_PARTITIONS, PARTITION_NODE_CONNECTION_INFO])
        )

        self.total_partitions: int = res_fields["total_partitions"]
        self.partition_leader_nodes: List[PartitionLeaderConnectionInfo] = []

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

        self.success: bool = res_fields["success"]