import redis
import queue
import threading
import time
from abc import ABC, abstractmethod


_sentinel = "__STOP__"


class ThreadRunner(ABC):
    def __init__(self) -> None:
        self.stop_event = threading.Event()

        kvs = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.kvs = kvs

        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        # self.thread.start()

    @ abstractmethod
    def run(self):
        pass

    def stop(self):
        self.stop_event.set()
        self.thread.join()


class SubscribeRunner(ThreadRunner):
    def __init__(self, channel: str, target_queue: queue.Queue) -> None:
        super().__init__()

        self.pubsub = self.kvs.pubsub()
        self.pubsub.subscribe(channel)
        self.target_queue = target_queue
        # self.thread.start()

    @ abstractmethod
    def run(self):
        ...

    def stop(self):
        self.pubsub.unsubscribe()
        super().stop()


class Subscriber(SubscribeRunner):
    def __init__(self, channel: str, target_queue: queue.Queue) -> None:
        super().__init__(channel, target_queue)
        self.thread.start()

    def run(self):
        # while not self.stop_event.is_set():
        #     for message in self.pubsub.listen():
        #         if message["data"] == 0:
        #             continue

        #         print(f"Subscriber get message: {message}")
        #         data = message["data"]
        #         self.target_queue.put(data)

        while not self.stop_event.is_set():
            msg = self.pubsub.get_message(ignore_subscribe_messages=True)
            if msg:
                self.target_queue.put(msg["data"])
            time.sleep(0.1)
