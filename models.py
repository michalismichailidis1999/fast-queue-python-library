from typing import Any, Dict
import json
import time


class Message:
    def __init__(self, payload: str, key: Any = None):
        self.payload: str = payload

        self.__header: Dict[str, Any] = {}

        self.__header["key"] = key
        self.__header["timestamp"] = int(time.time() * 1000)
        self.__header["payload_size"] = len(payload)
        self.__total_bytes: int = len(str(self))

    def get_key(self):
        return self.__header["key"]

    def get_message_timestamp(self):
        return self.__header["timestamp"]

    def get_message_payload_size(self):
        return self.__header["payload_size"]

    def get_total_bytes(self):
        return self.__total_bytes

    def __str__(self) -> str:
        return json.dumps({"header": self.__header, "payload": self.payload})
