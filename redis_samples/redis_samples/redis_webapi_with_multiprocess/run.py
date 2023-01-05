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

    def stop(self):
        os.kill(self.pid, SIGTERM)
        print("Stopping server")
        self.join()
        self.terminate()


class PredictorRunner(multiprocessing.Process):
    def __init__(self, target_queue) -> None:
        multiprocessing.Process.__init__(self)
        # self.q = target_queue
        self.subscriber = Subscriber(
            channel="wait4predict", target_queue=target_queue)
        self.predictor = Predictor(target_queue)

    def run(self):
        self.subscriber.run()
        self.predictor.run()
        # self.subscriber = Subscriber(
        #     channel="wait4predict", target_queue=self.q)
        # self.predictor = Predictor(self.q)

    def stop(self):
        self.predictor.stop()
        self.subscriber.stop()

        os.kill(self.pid, SIGTERM)
        print("Stopping PredictorRunner")
        self.join()
        self.terminate()


if __name__ == '__main__':
    shared_queue = multiprocessing.Queue()

    service = ApiService(host="localhost", port=5001)

    predictor = PredictorRunner(target_queue=shared_queue)
    predictor.daemon = True
    service.start()
    predictor.start()

    import time
    time.sleep(30)
    predictor.stop()
    service.stop()
