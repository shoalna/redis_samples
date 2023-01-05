import redis
import queue
import threading
import time
from abc import ABC, abstractmethod
from sanic.log import logger


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
        ...

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
        try:
            while not self.stop_event.is_set():
                msg = self.pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=None,
                )
                if msg:
                    print(f"Predictor.Subscriber get: {msg['data'].decode()}")
                    self.target_queue.put(msg["data"])
        except KeyboardInterrupt:
            # raise
            logger.warning("Killing Subscriber...")
            # print("Killing Subscriber...")
        finally:
            # print("Subscriber do die")
            self.target_queue.put(_sentinel)
            self.stop()
            # logger.warning("Subscriber dead")

    # def stop(self):
    #     print("###### Killing Subscriber ######")
    #     self.target_queue.put(_sentinel)
    #     super().stop()
