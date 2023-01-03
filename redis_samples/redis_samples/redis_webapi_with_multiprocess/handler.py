from utils import ThreadRunner, _sentinel
import queue
import random
import time
import json
import pickle
import pandas as pd


def countdown(n=500000000, rn=1):
    ts = time.time()
    while n > 0:
        if rn is None:
            rn = random.randint(1, 3)
        n -= rn
    te = time.time()
    return {"usetime": "{:.3f}".format(te - ts)}


class Predictor(ThreadRunner):
    def __init__(self, target_queue: queue.Queue) -> None:
        super().__init__()
        print("target_queue: ", target_queue)
        self.target_queue = target_queue
        self.thread.start()

    def run(self):
        while True:
            try:
                # use non-block get
                # block get may miss published message in Subscriber
                # since using multithread
                req = self.target_queue.get(False)
            except queue.Empty:
                time.sleep(.1)
                continue

            if req is _sentinel:
                self.target_queue.put(_sentinel)
                break

            req = req.decode()
            print(f"Predictor handling {req}")
            medexam = self.kvs.hget(req, "medexam")
            if medexam is None:
                raise
            medexam = pickle.loads(medexam)

            # ensure same item
            assert req == medexam["time"].values[0]

            # run heavy process and save to redis
            res = countdown()
            self.kvs.hset(req, "predicted", json.dumps(res))

            # publish ids predicted
            print(f"Predictor publish {req}")
            self.kvs.publish('predicted', req)

        self.stop()
