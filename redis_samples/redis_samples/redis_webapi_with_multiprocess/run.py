from app import app as apiserver
from handler import Predictor
from utils import Subscriber

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import threading
import os
import queue
import redis
from signal import SIGTERM
from sanic.log import logger
import sys
import signal

"""
1. use event to detect predict finish
2. set max length for queue.
   The predict requset more than max should got response like 'too busy'
"""


class ApiService(multiprocessing.Process):
    def __init__(self, host, port):
        multiprocessing.Process.__init__(self)
        self.host = host
        self.port = port

    def run(self):
        print("Running server")
        apiserver.run(host=self.host, port=self.port, workers=2)


class PredictorRunner(multiprocessing.Process):
    def __init__(self, target_queue) -> None:
        multiprocessing.Process.__init__(self)
        # demon thread
        self.subscriber = Subscriber(
            name="PredictorRunner.subscriber",
            channel="wait4predict",
            target_queue=target_queue,
        )
        self.predictor = Predictor(
            target_queue=target_queue,
            name="PredictorRunner.predictor",
        )


if __name__ == '__main__':
    shared_queue = multiprocessing.Queue()
    # stop_event = threading.Event()

    service = ApiService(host="localhost", port=5001)
    predictor = PredictorRunner(target_queue=shared_queue)
    predictor.daemon = True

    service.start()
    predictor.start()
    # service.join()

    try:
        service.join()
    except KeyboardInterrupt:
        # sanic gracefull join
        service.join(1)
