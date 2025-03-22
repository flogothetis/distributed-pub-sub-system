from fastapi import FastAPI
import requests
import os
import json
import hashlib
from kazoo.client import KazooClient


ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")
zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()
app = FastAPI()
# API: Create a new topic
@app.post("/create_topic/{topic_ID}/{num_partitions}")
def create_topic(topic_ID: str, num_partitions: int):
    broker_url = f"http://broker:5000/create_topic/{topic_ID}/{num_partitions}"  # Send to one broker
    response = requests.post(broker_url)
    return response.json()

# API: Write a message to a topic using key-based partitioning
@app.post("/write/{topic_ID}")
def write_message(topic_ID: str, message: dict):
    topic_path = f"/topics/{topic_ID}"
    
    # Fetch topic metadata from Zookeeper
    if not zk.exists(topic_path):
        return {"error": "Topic not found"}
    
    topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
    partitions = list(topic_metadata["partitions"].keys())
    
    # Use key-based hashing to determine partition
    key = message.get("key")
    if key is None:
        return {"error": "Key is required for partition selection"}
    
    partition_id = int(hashlib.sha256(key.encode()).hexdigest(), 16) % len(partitions)
    leader_broker = topic_metadata["partitions"][str(partition_id)]["leader"]
    
    # Write message to leader broker
    write_url = f"http://{leader_broker}:5000/write/{topic_ID}/{partition_id}"
    response = requests.post(write_url, json=message)
    return response.json()


# API: Delete a topic
@app.delete("/delete_topic/{topic_ID}")
def delete_topic(topic_ID: str):
    topic_path = f"/topics/{topic_ID}"
    if zk.exists(topic_path):
        zk.delete(topic_path, recursive=True)
        return {"status": f"Topic {topic_ID} deleted"}
    return {"error": "Topic not found"}
