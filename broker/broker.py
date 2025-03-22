from fastapi import FastAPI
from kazoo.client import KazooClient, KazooState, KazooException
import os
import json
import requests
import time


'''
/brokers/                   # Registered brokers
│── broker1/                # Broker 1 metadata
│   ├── {"host": "http://broker1:5000"}
│── broker2/                # Broker 2 metadata
│   ├── {"host": "http://broker2:5000"}
│── broker3/                # Broker 3 metadata
│   ├── {"host": "http://broker3:5000"}

/topics/                    
│── orders/                 # Topic: orders
│   ├── {"name": "orders", "partitions": {...}}
│   ├── partition_0/        # Partition 0 details
│   │   ├── {"leader": "broker1", "isr": ["...."], "followers": ["......"]}
│   ├── partition_1/
│   │   ├── {"leader": "broker2", "isr": ["...."], "followers": ["......"]}
│   ├── partition_2/
│   │   ├── {"leader": "broker3", "isr": ["....."], "followers": ["...."]}


'''
import logging
import json
from fastapi import FastAPI, HTTPException
from kazoo.exceptions import NoNodeError, KazooException, NodeExistsError
# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Change to INFO for less verbosity
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("broker.log")  # Log to file
    ]
)

# Get broker hostname as BROKER_ID inside the Docker container
BROKER_ID = os.getenv("HOSTNAME", "localhost")
BROKER_HOST = f"http://{BROKER_ID}:5000"
ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST", "zookeeper:2181")

zk = KazooClient(hosts=ZOOKEEPER_HOST)
zk.start()
app = FastAPI()
# Register broker in Zookeeper
zk.ensure_path("/brokers")
broker_path = f"/brokers/{BROKER_ID}"
brokers_set = set()

@app.post("/replicate/{topic_ID}/{partition_ID}")

def replicate_messages(topic_ID: str, partition_ID: int, messages: list):
    partition_path = f"./data/{topic_ID}/partition_{partition_ID}.log"
    os.makedirs(os.path.dirname(partition_path), exist_ok=True)

    with open(partition_path, "a") as f:
        for message in messages:
            f.write(json.dumps(message) + "\n")
    
    return {"status": "Replication successful", "broker": BROKER_ID}

def clear_partition_data(topic_ID, partition_ID):
    """
    Clears outdated messages before syncing to avoid duplicates.
    """
    partition_path = f"./data/{topic_ID}/partition_{partition_ID}.log"
    if os.path.exists(partition_path):
        os.remove(partition_path)
        
def sync_data_from_leader(topic_ID, partition_ID, leader_broker):
    """
    Synchronizes missing messages from the leader broker before rejoining ISR.
    """

    leader_url = f"http://{leader_broker}:5000/read/{topic_ID}/{partition_ID}"
    response = requests.get(leader_url)

    if response.status_code == 200:
        messages = response.json().get("messages", [])

        # Clear outdated messages before syncing (to avoid duplicates)
        clear_partition_data(topic_ID, partition_ID)

        # Write all new messages from the leader
        replicate_messages(topic_ID, partition_ID, messages)

        print(f"Broker {BROKER_ID} synchronized missing messages for {topic_ID} partition {partition_ID}")

        return {"status": "Synchronization complete", "broker": BROKER_ID}
    
    print(f"Failed to sync from leader {leader_broker} for {topic_ID} partition {partition_ID}")
    return {"error": "Failed to sync from leader", "broker": BROKER_ID}

def rejoinAsFollower():
    logging.info("Starting rejoinAsFollower process...")

    try:
        topics = zk.get_children("/topics")
    except KazooException as e:
        logging.error(f"Failed to retrieve topics: {str(e)}")
        return

    if not topics:
        logging.info("No topics found in ZooKeeper.")
        return

    for topic in topics:
        topic_path = f"/topics/{topic}"
        topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
        logging.info(f"Processing topic: {topic}")

        for partition, partition_data in topic_metadata["partitions"].items():
            logging.info(f"Processing partition {partition} for rejoinAsFollower...")

            # If a broker rejoins, check if it was previously in ISR and not the current leader
            if BROKER_ID not in partition_data["isr"] and BROKER_ID != partition_data["leader"]:
                if BROKER_ID in partition_data.get("previous_isr", []):
                    logging.info(f"Broker {BROKER_ID} was previously in ISR for {topic} partition {partition}. Attempting to rejoin.")

                    lock = zk.Lock(f"/locks/{topic}/{partition}")
                    acquired = False

                    for attempt in range(5):  # Retry with exponential backoff
                        try:
                            if lock.acquire(timeout=5):
                                logging.info(f"Acquired lock for /locks/{topic}/{partition} on attempt {attempt + 1}")
                                acquired = True
                                break
                        except KazooException as e:
                            logging.warning(f"Attempt {attempt + 1} failed to acquire lock for /locks/{topic}/{partition}: {e}")
                            time.sleep(2 ** attempt)

                    if not acquired:
                        logging.error(f"Failed to acquire lock for /locks/{topic}/{partition} after 5 attempts. Skipping rejoin.")
                        continue

                    try:
                        logging.info(f"Rejoining ISR for {topic} partition {partition}. Synchronizing data from leader...")
                        sync_data_from_leader(topic, partition, partition_data["leader"])

                        partition_data["isr"].append(BROKER_ID)
                        partition_data["followers"].append(BROKER_ID)

                        # Update metadata in ZooKeeper
                        zk.set(topic_path, json.dumps(topic_metadata).encode("utf-8"))
                        logging.info(f"Broker {BROKER_ID} successfully rejoined ISR for {topic} partition {partition}.")
                    except KazooException as e:
                        logging.error(f"Failed to update metadata for {topic} partition {partition}: {e}")
                    finally:
                        lock.release()
                        logging.info(f"Lock released for /locks/{topic}/{partition}")
                else:
                    logging.info(f"Broker {BROKER_ID} not in ISR for {topic} partition {partition}. Skipping rejoin.")
            else:
                logging.info(f"Broker {BROKER_ID} already in ISR or is the leader for {topic} partition {partition}.")

    


try:
    zk.create(broker_path, json.dumps({"host": BROKER_HOST}).encode("utf-8"), ephemeral=True)
    rejoinAsFollower()
except KazooException as e:
    print(f"Failed to register broker: {str(e)}")




@app.post("/create_topic/{topic_ID}/{num_partitions}")
def create_topic(topic_ID: str, num_partitions: int):
    if not topic_ID or not isinstance(topic_ID, str):
        raise HTTPException(status_code=400, detail="Invalid topic ID")
    
    if not isinstance(num_partitions, int) or num_partitions <= 0:
        raise HTTPException(status_code=400, detail="Number of partitions must be a positive integer")

    topic_parent_path = "/topics"
    topic_path = f"{topic_parent_path}/{topic_ID}"

    try:
        # Ensure the /topics and /leaders paths exist
        if not zk.exists(topic_parent_path):
            zk.create(topic_parent_path, b"{}")  # Create the /topics node if missing

        # Check if the topic already exists
        if zk.exists(topic_path):
            raise HTTPException(status_code=400, detail="Topic already exists")
        
        # Fetch available brokers
        brokers = zk.get_children("/brokers")
    except NoNodeError:
        raise HTTPException(status_code=500, detail="ZooKeeper node '/brokers' does not exist")
    except KazooException as e:
        raise HTTPException(status_code=500, detail=f"Error accessing ZooKeeper: {str(e)}")

    if not brokers:
        raise HTTPException(status_code=500, detail="No available brokers to assign partitions")

    topic_metadata = {"partitions": {}}

    try:
        for partition in range(num_partitions):
            leader = brokers[partition % len(brokers)]
            followers = [b for b in brokers if b != leader]
            isr =  followers  # Initialize ISR with  followers

            # Add partition metadata
            topic_metadata["partitions"][partition] = {
                "leader": leader,
                "isr": isr,
                "followers": followers,
                "previous_isr": isr + [leader]  # Keep track of previous ISR for rejoinAsFollower
            }

        # Create the topic node in ZooKeeper with the partition metadata
        zk.create(topic_path, json.dumps(topic_metadata).encode("utf-8"))
        return {"status": f"Topic {topic_ID} created with {num_partitions} partitions and leader paths"}
    
    except NodeExistsError:
        raise HTTPException(status_code=400, detail="Topic creation failed: Topic already exists")
    except KazooException as e:
        raise HTTPException(status_code=500, detail=f"Error creating topic in ZooKeeper: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
@app.get("/read/{topic_ID}/{partition_ID}")
def read_messages(topic_ID: str, partition_ID: int):
    partition_path = f"./data/{topic_ID}/partition_{partition_ID}.log"

    if not os.path.exists(partition_path):
        return {"error": "Partition not found"}

    with open(partition_path, "r") as f:
        messages = [json.loads(line.strip()) for line in f.readlines()]

    return {"messages": messages}

def handle_dead_broker(dead_broker, executor_broker):
    logging.info(f"Handling dead broker event for {dead_broker} by {executor_broker}")
    
    try:
        topics = zk.get_children("/topics")
    except KazooException as e:
        logging.error(f"Failed to retrieve topics: {str(e)}")
        return
    
    for topic in topics:
        topic_path = f"/topics/{topic}"
        topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))

        for partition, partition_data in topic_metadata["partitions"].items():

   
                # If the current leader fails, a new leader must be elected
            if partition_data["leader"] == dead_broker:
                logging.info(f"Leader for {topic} partition {partition} is missing. Electing a new leader...")
                partition_data["leader"] = partition_data["isr"][0]  # This broker is now leader
                logging.info(f"Broker {partition_data['isr'][0]} is now leader for {topic} partition {partition}")

            # Update ISR and followers, ensuring only available brokers remain
            partition_data["isr"] = [b for b in partition_data["isr"] if  b != partition_data["leader"]]
            partition_data["followers"] = [b for b in partition_data.get("followers", []) if b != partition_data["leader"]]
           
            try:
                zk.set(topic_path, json.dumps(topic_metadata).encode("utf-8"))
                logging.info(f"Metadata for {topic} updated successfully.")
            except KazooException as e:
                logging.error(f"Failed to update metadata for {topic}: {str(e)}")
    




def monitor_dead_broker(dead_broker: str):
    
    dead_broker_path = f"/events/dead_broker/{dead_broker}"
    
    # Ensure that dead broker event is registered
    try:
        zk.ensure_path(dead_broker_path)
    except KazooException as e:     
        logging.info(f"Path {dead_broker_path} already exists")
    
    def attempt_to_become_leader_executor_of_dead_event(dead_broker: str):
        logging.info(f"Broker {dead_broker} is dead. Handling event...")
        # Only one broker will achieve to be leader of the dead broker event
        executor_broker_path = dead_broker_path + '/master_executor'
        try:
            zk.create(executor_broker_path, value=bytes(BROKER_ID, 'utf-8'),  ephemeral=True, sequence=False, makepath=True)
            logging.info(f"Broker {BROKER_ID} is the executor of the dead broker event.")
            handle_dead_broker(dead_broker, BROKER_ID)
        except NodeExistsError as e:
            #Someone else is the executor
            logging.info(f"Broker {BROKER_ID} failed to be the executor of the dead broker event.")
            @zk.DataWatch(executor_broker_path)
            def watch_dead_broker_event(data, stat, event):
                logging.info(f"Registered in watch_dead_broker_event")
                if event and event.type == "DELETED":
                    logging.info(f"Call attempt_to_become_leader_executor_of_dead_event")
                    attempt_to_become_leader_executor_of_dead_event(dead_broker)
                  
    attempt_to_become_leader_executor_of_dead_event(dead_broker)
                
            
    

import time
@zk.ChildrenWatch("/brokers")
def watch_broker_changes(brokers):
    logging.info(f"Broker event detected: {brokers}")

    ''''
    /events/dead_broker/<HOSTNAME> is a new event that is created when a broker is dead.
    The first broker that creates the event is the executor of the event.
    The executor will handle the event and will be responsible to elect a new leader for the dead broker event.
    1. Broker is dead. Instead of handling death at each broker node and have mutliple locks
    is better to create an new event under /events/dead_broker (data : hostname of dead broker)

    '''
    global brokers_set
    current_brokers = set(brokers)
    dead_brokers = brokers_set - current_brokers
    brokers_set = current_brokers
    for broker in dead_brokers:
        monitor_dead_broker(broker)
        
        


# API: Write a message to a partition and replicate to followers
@app.post("/write/{topic_ID}/{partition_ID}")
def write_message(topic_ID: str, partition_ID: int, message: dict):
    partition_path = f"./data/{topic_ID}/partition_{partition_ID}.log"
    os.makedirs(os.path.dirname(partition_path), exist_ok=True)

    with open(partition_path, "a") as f:
        f.write(json.dumps(message) + "\n")
    
    # Get topic metadata to find followers
    topic_path = f"/topics/{topic_ID}"
    if not zk.exists(topic_path):
        return {"error": "Topic not found"}
    
    topic_metadata = json.loads(zk.get(topic_path)[0].decode("utf-8"))
    partition_data = topic_metadata["partitions"].get(str(partition_ID), {})
    followers = partition_data.get("followers", [])
    
    successful_followers = []
    
    # Replicate message to all followers
    for follower in followers:
        follower_url = f"http://{follower}:5000/replicate/{topic_ID}/{partition_ID}"
        response = requests.post(follower_url, json={"messages": [message]})
        
        if response.status_code == 200:
            successful_followers.append(follower)
    
    # Update ISR: Only followers that successfully replicated stay in ISR
    partition_data["isr"] = [partition_data["leader"]] + successful_followers
    zk.set(topic_path, json.dumps(topic_metadata).encode("utf-8"))
    
    return {"status": "Message written and replicated", "broker": BROKER_ID}
