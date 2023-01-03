from sanic import Sanic
from sanic.response import text
import redis
import json

import time
import datetime
import pandas as pd
import pickle


medexam = {
    "name": ["葛の葉", "a", "b"],
    "gender": ["Male", "Male", "Female"],
    "status": ["5.5", "xamp", "NGIX"],
    "like": ["Dragon_Fox", "Dragon_Fox2", "Dragon_Fox3"],
}


app = Sanic("MyHelloWorldApp")


@app.listener("after_server_start")
async def listener_after_server_start(*args, **kwargs):
    redis_server = redis.StrictRedis(host='localhost', port=6379, db=0)
    app.ctx.redis = redis_server
    print("after_server_start")


@app.get("/")
async def hello_world(request):
    user_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    user_medexam = medexam.copy()
    user_medexam = pd.DataFrame(user_medexam)
    user_medexam["time"] = user_id

    # put data to redis
    app.ctx.redis.hset(user_id, 'medexam', pickle.dumps(user_medexam))

    # publish ids need to be handled
    print(f"wait4predict publish: {user_id}")
    app.ctx.redis.publish('wait4predict', user_id)

    # subscribe predicted channel
    p = app.ctx.redis.pubsub()
    p.subscribe('predicted')

    # blocks until a message is available
    # stop if id predicted
    while True:
        msg = p.get_message(ignore_subscribe_messages=True)
        if (msg is not None) and (msg["data"].decode() == user_id):
            res = app.ctx.redis.hget(user_id, 'predicted')
            res = json.loads(res)
            app.ctx.redis.delete(user_id)
            p.unsubscribe()
            break

        time.sleep(0.1)

    # for message in p.listen():
    #     if message == user_id:
    #         res = app.ctx.redis.hget(user_id, 'predicted', user_medexam)
    #         res = json.loads(res)
    #         app.ctx.redis.delete(user_id)
    #         p.unsubscribe()
    #         break

    return text(f"predict done: {user_id}-{res}")