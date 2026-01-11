import threading
import time
from typing import Callable, Dict, List, Tuple

from fast_queue.exceptions import FastQueueException
from .broker_client import BrokerClient
from .producer import Producer
from .conf import ProducerConf
from .lock import ReadWriteLock
from .constants import *
from .responses import BeginTransactionResponse, FinalizeTransactionResponse, RegisterTransactionGroupResponse, VerifyTransactionGroupCreationResponse

class TransactionalProducer:
    """
    :param BrokerClient client: Client that handles communication with leader node.
    :param producers_info: List of tuples that consist of a producer configuration and a message delivery callback. Similar to Producer 2nd and 3rd parameters but in a list to indicate all transaction producers.
    :raise ValueError: If invalid argument is passed.
    """

    def __init__(self, client: BrokerClient, producers_info: List[Tuple[ProducerConf, Callable[[bytes, bytes | None, Exception], None] | None]]) -> None:
        if producers_info is None or len(producers_info) == 0:
            raise ValueError("At least one producer info is needed. List cannot be empty")
        
        self.__client: BrokerClient = client
        self.__producers: Dict[str, Producer] = {}
        self.__registered_queues: List[str] = []

        for prod_conf, msg_cb in producers_info:
            if prod_conf.queue in self.__producers:
                raise ValueError(f"Duplicate queue name {prod_conf.queue} detected in producer configurations")
            
            producer = Producer(client=client, conf=prod_conf, on_delivery_callback=msg_cb)
            producer._set_transaction_group_id_retrieval_cb(self.__get_transaction_group_id)
            self.__producers[prod_conf.queue] = producer
            self.__registered_queues.append(prod_conf.queue)

        self.__lock: ReadWriteLock = ReadWriteLock()
        self.__transaction_group_leader_node_id: int = 0
        self.__transaction_group_id: int = 0
        self.__verified_transaction_group_id: int = 0
        self.__stopped: bool = False

        while self.__transaction_group_id <= 0:
            self.__register_transaction_group(10, 2, True)
        
        while self.__verified_transaction_group_id != self.__transaction_group_id:
            self.__verify_transaction_group_registration(10, 2, True)

        t1 = threading.Thread(target=self.__register_transaction_group, args=[1, 15, False], daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.__verify_transaction_group_registration, args=[1, 15, False], daemon=True)
        t2.start()

    def __register_transaction_group(self, initial_retries: int, time_to_wait: int, called_from_constructor: bool = False):
        while not self.__stopped:
            if self.__get_transaction_group_id() > 0:
                if called_from_constructor: break

                time.sleep(time_to_wait)
                continue

            retries: int = initial_retries

            try:
                while retries > 0:
                    try:
                        leader_node = self.__client._get_leader_node_socket_client()

                        if leader_node is None:
                            raise Exception("No controller leader elected yet")
                        
                        res = RegisterTransactionGroupResponse(
                            leader_node.send_request(
                                self.__client._create_request(
                                    REGISTER_TRANSACTION_GROUP,
                                    [
                                        (REGISTERED_QUEUES, self.__registered_queues, None)
                                    ]
                                )
                            )
                        )

                        if res.transaction_group_id == 0:
                            raise Exception("Unsuccessfull transaction producer registration")
                        
                        self.__set_transaction_group_id(res.leader_id, res.transaction_group_id)

                        break
                    except Exception as e:
                        retries -= 1

                        if retries <= 0: raise e
                        else:
                            time.sleep(time_to_wait)
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to register transaction group. Reason: {e}")

            if called_from_constructor: break

            time.sleep(time_to_wait)

    def __verify_transaction_group_registration(self, initial_retries: int, time_to_wait: int, called_from_constructor: bool = False):
        while not self.__stopped:
            if self.__is_transaction_group_id_verified():
                if called_from_constructor: break

                time.sleep(time_to_wait)
                continue

            retries: int = initial_retries

            try:
                while retries > 0:
                    try:
                        leader_node_id = self.__get_transaction_group_leader_node_id()
                        tx_group_id = self.__get_transaction_group_id()

                        leader_node = self.__client._get_node_socket_client(leader_node_id)

                        if leader_node is None:
                            raise Exception(f"Transaction group leader not found for node {leader_node_id}")
                        
                        res = VerifyTransactionGroupCreationResponse(
                            leader_node.send_request(
                                self.__client._create_request(
                                    VERIFY_TRANSACTION_GROUP_CREATION,
                                    [
                                        (TRANSACTION_GROUP_ID, tx_group_id, LONG_LONG_SIZE)
                                    ]
                                )
                            )
                        )
                        
                        if not res.success:
                            raise Exception("Unsuccessfull try for transaction group creation verification")

                        self.__set_verified_transaction_group_id(tx_group_id)

                        break
                    except Exception as e:
                        retries -= 1

                        if retries <= 0: raise e
                        else:
                            time.sleep(time_to_wait)
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to verify transaction group registration. Reason: {e}")

            if called_from_constructor: break

            time.sleep(time_to_wait)

    def produce(self, queue_name: str, message: str, key: str = None, transaction_id: int = 0) -> None:
        if not self.__can_execute_transactional_command():
            raise Exception("Transaction group registration is not done yet. Cannot produce transactional message")
        
        if queue_name not in self.__producers:
            raise Exception(f"Queue {queue_name} was not registered in transactional's producer constructor")
        
        self.__producers[queue_name].produce(message=message, key=key, transaction_id=transaction_id)

    def begin_transaction(self) -> int:
        if not self.__can_execute_transactional_command():
            raise Exception("Transaction group registration is not done yet. Cannot begin transaction")
        
        leader_id = self.__get_transaction_group_leader_node_id()

        transaction_group_leader_node = self.__client._get_node_socket_client(leader_id)

        if transaction_group_leader_node is None:
            raise Exception(f"Cannot begin transaction. Could not connect yet to transaction's group leader node {leader_id}")

        tx_id = 0

        try:
            tx_id = BeginTransactionResponse(
                transaction_group_leader_node.send_request(
                    self.__client._create_request(
                        BEGIN_TRANSACTION,
                        [
                            (TRANSACTION_GROUP_ID, self.__get_transaction_group_id(), LONG_LONG_SIZE)
                        ]
                    )
                )
            ).transaction_id
        except FastQueueException as e:
            if e.error_code == INCORRECT_LEADER:
                self.__set_transaction_group_id(-1, 0)
            else:
                raise e
        except Exception as e:
            raise e

        if tx_id <= 0:
            raise Exception("Something went wrong. Could not begin transaction")

        return tx_id

    def commit_transaction(self, transaction_id: int) -> None:
        if not self.__can_execute_transactional_command():
            raise Exception("Transaction group registration is not done yet. Cannot commit transaction")
        
        self.__finalize_transaction(transaction_id=transaction_id, commit=True)

    def abort_transaction(self, transaction_id: int) -> None:
        if not self.__can_execute_transactional_command():
            raise Exception("Transaction group registration is not done yet. Cannot abort transaction")
        
        self.__finalize_transaction(transaction_id=transaction_id, commit=False)

    def __finalize_transaction(self, transaction_id: int, commit: bool = False) -> None:
        leader_id = self.__get_transaction_group_leader_node_id()

        transaction_group_leader_node = self.__client._get_node_socket_client(leader_id)

        finalize_action = "commit" if commit else "abort"

        if transaction_group_leader_node is None:
            raise Exception(f"Cannot {finalize_action} transaction. Could not connect yet to transaction's group leader node {leader_id}")
        
        success = False

        try:
            success = FinalizeTransactionResponse(
                transaction_group_leader_node.send_request(
                    self.__client._create_request(
                        FINALIZE_TRANSACTION,
                        [
                            (TRANSACTION_GROUP_ID, self.__get_transaction_group_id(), LONG_LONG_SIZE),
                            (TRANSACTION_ID, transaction_id, LONG_LONG_SIZE),
                            (COMMIT_TRANSACTION, commit, None)
                        ]
                    )
                )
            ).success
        except FastQueueException as e:
            if e.error_code == INCORRECT_LEADER:
                self.__set_transaction_group_id(-1, 0)
            else:
                raise e
        except Exception as e:
            raise e

        if not success:
            raise Exception(f"Could not {finalize_action} transaction. Something went wrong")

    def flush(self):
        for _, producer in self.__producers.items():
            producer.flush()

    def close(self):
        for _, producer in self.__producers.items():
            producer.close()

    def __get_transaction_group_id(self) -> int:
        self.__lock.acquire_read()

        tx_group_id: int = self.__transaction_group_id

        self.__lock.release_read()

        return tx_group_id
    
    def __get_verified_transaction_group_id(self) -> int:
        self.__lock.acquire_read()

        verified_tx_group_id: int = self.__verified_transaction_group_id

        self.__lock.release_read()

        return verified_tx_group_id

    def __get_transaction_group_leader_node_id(self) -> int:
        self.__lock.acquire_read()

        leader_id: int = self.__transaction_group_leader_node_id

        self.__lock.release_read()

        return leader_id
    
    def __set_transaction_group_id(self, leader_id: int, transaction_group_id: int) -> None:
        self.__lock.acquire_write()

        self.__transaction_group_leader_node_id = leader_id
        self.__transaction_group_id = transaction_group_id

        self.__lock.release_write()

    def __set_verified_transaction_group_id(self, verified_transaction_group_id: int) -> None:
        self.__lock.acquire_write()

        self.__verified_transaction_group_id = verified_transaction_group_id

        self.__lock.release_write()

    def __is_transaction_group_id_verified(self) -> bool:
        self.__lock.acquire_read()

        verified = self.__transaction_group_id == self.__verified_transaction_group_id

        self.__lock.release_read()

        return verified

    def __can_execute_transactional_command(self) -> bool:
        self.__lock.acquire_read()

        can_execute_command = self.__transaction_group_id > 0 and self.__transaction_group_leader_node_id > 0 and self.__transaction_group_id == self.__verified_transaction_group_id

        self.__lock.release_read()

        return can_execute_command