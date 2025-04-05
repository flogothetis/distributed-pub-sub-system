from fastapi import FastAPI, HTTPException
import redis
import hashlib
import json
import requests
import os
import random
from kazoo.client import KazooClient


app = FastAPI()

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Connect to Zookeeper
ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()


@app.post("/subscribe/{email}/{topic_ID}")
def subscribe(email:str, topic_ID: str):
    offset_key = f"offset:{email}:{topic_ID}"
    if not r.exists(offset_key):
        r.set(offset_key, 0)
    return {"status": f"{email} subscribed to {topic_ID}"}


@app.post("/unsubscribe/{email}/{topic_ID}")
def unsubscribe(email:str, topic_ID: str):
    
    offset_key = f"offset:{email}:{topic_ID}"
    #Check that user has subscribed to the topic
    if not r.exists(offset_key):
        raise HTTPException(status_code=403, detail="Not subscribed to topic")
    
    r.delete(offset_key)
    return {"status": f"{email} unsubscribed from {topic_ID}"}



@app.get("/read/{email}/{topic_ID}/{key}")
def read_by_key(email: str, topic_ID: str, key: str):
    
    # If email has not subscribed to the topic, return an error
    offset_key = f"offset:{email}:{topic_ID}"
    if not r.exists(offset_key):
        raise HTTPException(status_code=403, detail="Not subscribed to topic")
    
    # 1. Get topic metadata from Zookeeper
    topic_path = f"/topics/{topic_ID}"
    if not zk.exists(topic_path):
        raise HTTPException(status_code=404, detail="Topic not found")

    topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
    partitions = list(topic_metadata["partitions"].keys())
    num_partitions = len(partitions)

    # 2. Compute the partition based on key
    partition_id = int(hashlib.sha256(key.encode()).hexdigest(), 16) % num_partitions
    partition_info = topic_metadata["partitions"][str(partition_id)]
    # Pick a random follower or fallback to leader
    followers = partition_info.get("followers", [])
    if followers:
        selected_broker = random.choice(followers)
    else:
        selected_broker = partition_info["leader"]

    # 3. Get current offset from Redis
    offset_key = f"offset:{email}:{topic_ID}:{partition_id}"
    offset = int(r.get(offset_key) or 0)
    

    # 4. Read from broker partition API (with offset)
    broker_read_url = f"http://{selected_broker}:5000/read/{topic_ID}/{partition_id}/offset/{offset}"
    response = requests.get(broker_read_url)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to read from broker")

    messages = response.json().get("messages", [])
    end_offset = response.json().get("end_offset", 0)

    # 5. Update the offset in Redis
    r.set(offset_key, end_offset)

    return {
        "email": email,
        "key": key,
        "partition_id": partition_id,
        "start_offset": offset,
        "end_offset": end_offset,
        "messages": messages
    }