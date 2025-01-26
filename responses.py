from typing import Any, Dict, List, Set
from constants import *


def __response_fields_mapper(res_bytes: bytes, fields: Set[int]):
    total_bytes = len(res_bytes)

    offset = 4  # error code size

    field_values: Dict[str, Any] = {}

    while offset < total_bytes:
        res_key = int.from_bytes(
            bytes=res_bytes[offset : (offset + 4)], byteorder=ENDIAS, signed=False
        )

        if res_key not in fields:
            raise Exception(f"Incorrect response key {res_key}")

        offset += 4

        if res_key == LEADER_ID:
            field_values["leader_id"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + 4)], byteorder=ENDIAS, signed=False
            )

            offset += 4
        elif res_key == CONTROLLER_CONNECTION_INFO:
            node = {}

            node["node_id"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + 4)], byteorder=ENDIAS, signed=False
            )

            offset += 4

            address_size: int = int.from_bytes(
                bytes=res_bytes[offset : (offset + 4)], byteorder=ENDIAS, signed=False
            )

            offset += 4

            node["address"] = res_bytes[offset : (offset + address_size)].decode(
                "utf-8"
            )

            offset += address_size

            node["port"] = int.from_bytes(
                bytes=res_bytes[offset : (offset + 4)], byteorder=ENDIAS, signed=False
            )

            offset += 4

            if "controller_nodes" not in field_values:
                field_values["controller_nodes"] = [node]
            else:
                field_values["controller_nodes"].append(node)

    return field_values


class ControllerConnectionInfo:
    def __init__(self, node_id: int, address: str, port: int):
        self.node_id: int = node_id
        self.address: str = address
        self.port: int = port


class GetControllersConnectionInfoResponse:
    def __init__(self, res_bytes: bytes):
        res_fields = __response_fields_mapper(
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
        res_fields = __response_fields_mapper(res_bytes, set([LEADER_ID]))

        self.leader_id: int = res_fields["leader_id"]
