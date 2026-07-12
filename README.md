# 📊 Cassandra Posts Analytics

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black)
![Apache Cassandra](https://img.shields.io/badge/Apache-Cassandra-1287B1)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED)

A real-time posts analytics platform built with **Apache Kafka**, **Apache Cassandra**, **Python**, and **Streamlit**. The system ingests events through Kafka, processes them asynchronously, stores analytics data in Cassandra, and visualizes insights through an interactive dashboard.

---

# ✨ Features

- 📩 Event Producer
- ⚡ Real-time Event Consumer
- 🗄️ Apache Cassandra Storage
- 📈 Interactive Streamlit Dashboard
- 🐳 Docker Compose Deployment
- 📊 Analytics Visualization
- 🔄 Event-Driven Architecture

---

# 🏗️ Architecture

```text
                +----------------------+
                |    Event Producer    |
                +----------+-----------+
                           |
                           ▼
                  +------------------+
                  |   Apache Kafka   |
                  +---------+--------+
                            |
                            ▼
                 +--------------------+
                 |  Event Consumer    |
                 +---------+----------+
                           |
                           ▼
                +----------------------+
                | Apache Cassandra DB  |
                +----------+-----------+
                           |
                           ▼
                +----------------------+
                | Streamlit Dashboard  |
                +----------------------+
```

---

# 📂 Project Structure

```
cassandra-posts-analytics/
├── app.py
├── docker-compose.yaml
├── requirements.txt
├── core/
│   ├── db_utils.py
│   ├── event_producer.py
│   └── event_consuming.py
└── .streamlit/
```

---

# 🛠️ Technologies

- Python
- Apache Kafka
- Apache Cassandra
- Streamlit
- Docker Compose

---

# 🚀 Getting Started

## Clone the repository

```bash
git clone https://github.com/medelafia/cassandra-posts-analytics.git

cd cassandra-posts-analytics
```

---

## Install dependencies

```bash
pip install -r requirements.txt
```

---

## Run the infrastructure

```bash
docker compose up
```

---

## Start the Streamlit application

```bash
streamlit run app.py
```

---

# 🔄 Workflow

```
Generate Event
      │
      ▼
Kafka Producer
      │
      ▼
Kafka Topic
      │
      ▼
Consumer
      │
      ▼
Apache Cassandra
      │
      ▼
Analytics Dashboard
```

---

# 📊 Analytics

The dashboard can be extended to display:

- Total posts
- Active users
- Trending hashtags
- Event frequency
- Time-series analytics
- User engagement metrics

---

# 📈 Future Improvements

- Kafka Streams
- Apache Spark Streaming
- Grafana dashboards
- Prometheus monitoring
- Kubernetes deployment
- REST API
- Authentication
- CI/CD with GitHub Actions
- Automated testing

---

# 🎯 Learning Objectives

This project demonstrates:

- Event-driven architecture
- Apache Kafka messaging
- NoSQL data modeling with Cassandra
- Real-time analytics
- Interactive dashboards
- Docker-based deployment

---

# 👨‍💻 Author

**Mohamed El Afia**

GitHub: https://github.com/medelafia

---

# ⭐ Support

If you found this project useful, consider giving it a ⭐ on GitHub.
