import redis
import queue
import threading
import time
from abc import ABC, abstractmethod


_sentinel = "__STOP__"


class ThreadRunner(ABC):
    def __init__(self, name) -> None:
        kvs = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.kvs = kvs

        self.thread = threading.Thread(
            name=name, target=self.run)
        self.thread.daemon = True
        # self.thread.start()

    @ abstractmethod
    def run(self, *args, **kwargs):
        ...


class SubscribeRunner(ThreadRunner):
    def __init__(
            self, channel: str, target_queue: queue.Queue,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.pubsub = self.kvs.pubsub()
        self.pubsub.subscribe(channel)
        self.target_queue = target_queue


class Subscriber(SubscribeRunner):
    def __init__(self, channel: str, target_queue: queue.Queue,
                 *args, **kwargs) -> None:
        super().__init__(channel, target_queue, *args, **kwargs)
        self.thread.start()

    def run(self):
        while True:
            msg = self.pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=None,
            )
            if msg:
                print(f"Predictor.Subscriber get: {msg['data'].decode()}")
                self.target_queue.put(msg["data"])
