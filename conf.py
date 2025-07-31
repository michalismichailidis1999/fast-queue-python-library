class SocketClientConf:

    def __init__(
        self,
        timeoutms: int = 0,
        retries: int = 1,
        retry_wait_ms: int = 0,
        ssl_enable: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        cert_pass: str = None,
        max_pool_connections: int = 10
    ) -> None:
        if timeoutms < 0:
            raise ValueError("timeoutms cannot be less than 0")
        
        if retries < 1:
            raise ValueError("retries cannot be less than 1")
        
        if retry_wait_ms < 0:
            raise ValueError("retry_wait_ms cannot be less than 0")
        
        if max_pool_connections < 1:
            raise ValueError("max_pool_connections cannot be less than 1")
        
        self._timeoutms = timeoutms
        self._retries: int = retries
        self._retry_wait_ms: int = retry_wait_ms
        self._ssl_enable: bool = ssl_enable
        self._root_cert: str = root_cert
        self._cert: str = cert
        self._cert_key: str = cert_key
        self._cert_pass: str = cert_pass
        self._max_pool_connections: int = max_pool_connections

class BrokerClientConf(SocketClientConf):

    def __init__(
        self,
        timeoutms: int = 0,
        retries: int = 1,
        retry_wait_ms: int = 0,
        ssl_enable: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        cert_pass: str = None,
        authentication_enable: bool = False,
        username: str = None,
        password: str = None,
        max_pool_connections: int = 10
    ) -> None:
        super().__init__(
            timeoutms=timeoutms,
            retries=retries,
            retry_wait_ms=retry_wait_ms,
            ssl_enable=ssl_enable,
            root_cert=root_cert,
            cert=cert,
            cert_key=cert_key,
            cert_pass=cert_pass,
            max_pool_connections=max_pool_connections
        )

        self._authentication_enable: bool = authentication_enable
        self._username: str = username
        self._password: str = password
        self._create_queue_command_run: bool = False

class CommonConf:
    def __init__(self, queue: str):
        self.queue:str = queue

class ProducerConf(CommonConf):
    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :param int wait_ms: Milliseconds to wait before sending the messages batch.
    :param int max_batch_size: Maximum batch size in bytes producer can hold locally before sending it to broker (if wait_ms > 0) (default value 16KB).
    :raise ValueError: If invalid argument is passed.
    """

    def __init__(self, queue: str, wait_ms: int = None, max_batch_size: int = 16384, max_produce_request_bytes: int = 10_000_000) -> None:
        if queue is None or queue is "":
            raise ValueError("queue cannot be empty")

        if wait_ms is not None and wait_ms < 0:
            raise ValueError("wait_ms cannot be less than 0")
        
        if max_batch_size is not None and max_batch_size < 0:
            raise ValueError("max_batch_size cannot be less than 0")
        
        if max_produce_request_bytes  < 0:
            raise ValueError("max_produce_request_bytes cannot be less than 0")
        
        super().__init__(queue)
        
        self.max_batch_size: int = max_batch_size # default 16KB
        self.wait_ms: int = 0 if wait_ms is None else wait_ms
        self.max_produce_request_bytes = max_produce_request_bytes

class ConsumerConf(CommonConf):
    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :raise ValueError: If invalid argument is passed.
    """

    def __init__(self, queue: str):
        if queue is None or queue is "":
            raise ValueError("queue cannot be empty")
        
        super().__init__(queue)