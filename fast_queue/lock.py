import threading

class ReadWriteLock:
    def __init__(self):
        self.__readers = 0
        self.__read_lock = threading.Lock()
        self.__write_lock = threading.Lock()

    def acquire_read(self):
        with self.__read_lock:
            self.__readers += 1
            if self.__readers == 1:
                self.__write_lock.acquire()

    def release_read(self):
        with self.__read_lock:
            self.__readers -= 1
            if self.__readers == 0:
                self.__write_lock.release()

    def acquire_write(self):
        self.__write_lock.acquire()

    def release_write(self):
        self.__write_lock.release()