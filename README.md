# Kafka-like Pub-Sub System (FastAPI + Zookeeper + Redis)

This system is a lightweight Kafka-style publish-subscribe architecture built using FastAPI, Zookeeper, and Redis.

---

## 🚀 API Endpoints & Usage

### 🔧 Topic Management

#### ➕ Create a Topic

**POST** `/create_topic/{topic_ID}/{num_partitions}`

Create a new topic with a specified number of partitions.

**Example:**
```bash
curl -X POST http://localhost:4000/create_topic/news/3
```

---

#### ❌ Delete a Topic

**DELETE** `/delete_topic/{topic_ID}`

Delete an existing topic and all of its partition metadata.

**Example:**
```bash
curl -X DELETE http://localhost:4000/delete_topic/news
```

---

### ✉️ Message Publishing

#### 📝 Write a Message to Topic (Key-based Partitioning)

**POST** `/write/{topic_ID}`

Write a message to a topic. The message must include a `key` to determine the partition.

**JSON Body Example:**
```json
{
  "key": "user123",
  "value": "Breaking news update"
}
```

**Example:**
```bash
curl -X POST http://localhost:4000/write/news \
     -H "Content-Type: application/json" \
     -d '{"key": "user123", "value": "Breaking news update"}'
```

---

### 👤 Consumer APIs

#### 📩 Subscribe to a Topic

**POST** `/subscribe/{email}/{topic_ID}`

Subscribe a user (by email) to a topic.

**Example:**
```bash
curl -X POST http://localhost:5000/subscribe/john@example.com/news
```

---

#### 📤 Unsubscribe from a Topic

**POST** `/unsubscribe/{email}/{topic_ID}`

Unsubscribe a user (by email) from a topic.

**Example:**
```bash
curl -X POST http://localhost:5000/unsubscribe/john@example.com/news
```

---

#### 📖 Read Messages from Topic by Key

**GET** `/read/{email}/{topic_ID}/{key}`

Read new messages from a topic by computing the partition using the provided key. Also updates the user's offset automatically.

**Example:**
```bash
curl http://localhost:5000/read/john@example.com/news/user123
```

---

## 🔧 Environment Variables

| Variable        | Description                   | Default            |
|-----------------|-------------------------------|--------------------|
| `ZOOKEEPER_HOST`| Host and port for Zookeeper   | `zookeeper:2181`   |
| `REDIS_HOST`    | Redis server host              | `redis`            |
| `REDIS_PORT`    | Redis server port              | `6379`             |

---

## 📦 Dependencies

- FastAPI
- Redis
- Kazoo (Zookeeper client)
- Requests
- hashlib
- json
- os
- random
---

## 🏁 Run the App

Run the application using docker:

```bash
docker-compose up --build 
```

---
===