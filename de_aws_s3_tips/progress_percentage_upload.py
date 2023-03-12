from datetime import datetime
from threading import Lock
import logging

class ProgressPercentage(object):
    def __init__(self, filename, size, logger: logging.Logger):
        self._filename = filename
        self._log = logger
        self._size = size
        self._seen_so_far = 0
        self._lock = Lock()
        self.inicio = datetime.now()
        self.inicioGeral = datetime.now()
        self.segundos = 0
    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self.segundos = (datetime.now() - self.inicio).total_seconds()
            segundosGerais = (datetime.now() - self.inicioGeral).total_seconds()
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            msg = (
                "\r upload progress percentage - file: %s, total time: %s s, bytes: %s / %s, percent:  (%.2f%%)" % (
                    self._filename, segundosGerais, self._seen_so_far, self._size,
                    percentage))
            if self.segundos > 1:
                self._log.debug(msg)
                self.inicio = datetime.now()
                self.segundos = 0