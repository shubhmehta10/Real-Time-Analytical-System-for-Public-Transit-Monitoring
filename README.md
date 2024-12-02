# Real-Time-Analytical-System-for-Public-Transit-Monitoring

# Minikube, Apache Kafka, Apache Pinot, and Python Data Pipeline Setup

This README provides instructions to set up and use Minikube, Apache Kafka, Apache Pinot, and Python to ingest data, process it with Kafka, store it in Pinot, and visualize the results.

## Prerequisites

Ensure the following are installed on your system:

- **Minikube**: Kubernetes on your local machine
- **Kubectl**: Kubernetes command-line tool
- **Helm**: Kubernetes package manager
- **Python**: Version 3.x
- **Python libraries**: `requests`, `confluent-kafka`, etc.

## Step 1: Install and Start Minikube

### 1.1 Install Minikube

Follow the [Minikube installation guide](https://minikube.sigs.k8s.io/docs/start/) for your operating system.

### 1.2 Start Minikube

Start Minikube using the following command:

```bash
minikube start
```

## Step 2: Deploy Zookeeper and Kafka

### 2.1 Deploy Zookeeper

Apply the Zookeeper YAML configuration:

```bash
kubectl apply -f zookeeper-setup.yaml
```

### 2.2 Deploy Kafka

Apply the Kafka setup configuration:

```bash
kubectl apply -f kafka-setup.yaml
```

## Step 3: Install Apache Pinot Using Helm

### 3.1 Add Pinot Helm Chart Repository

Add the Pinot Helm chart repository if you haven't already:

```bash
helm repo add pinot https://raw.githubusercontent.com/apache/pinot/master/kubernetes/helm
```

### 3.2 Install Pinot

Install Pinot in the default namespace using Helm:

```bash
helm install pinot pinot/pinot --namespace default
```

## Step 4: Port Forward Pinot Server and Upload Schema and Table

### 4.1 Port Forward Pinot Server

Port forward the Pinot server:

```bash
kubectl port-forward svc/pinot-server 9000:9000
```

### 4.2 Upload Schema

Upload the schema using the following curl command:

```bash
curl -X POST "http://localhost:9000/schemas" \
     -H "Content-Type: application/json" \
     -d @schema2.json
```

### 4.3 Upload Table

Upload the table using the following curl command:

```bash
curl -X POST "http://localhost:9000/tables" \
     -H "Content-Type: application/json" \
     -d @table2.json
```

## Step 5: Ingest Data Using Kafka

### 5.1 Port Forward Kafka Service

Port forward the Kafka service:

```bash
kubectl port-forward svc/kafka-service 9092:9092
```

### 5.2 Run the Kafka Data Script

Run the Python script `kafka_data.py` to fetch data from the API and produce it as Kafka topics:

```bash
python kafka_data.py
```

## Step 6: Query Pinot Data and Visualize

### 6.1 Port Forward Pinot Broker

Port forward the Pinot broker:

```bash
kubectl port-forward svc/pinot-broker 8099:8099
```

### 6.2 Run the Visualization Script

Run the Python script `transit-analysis.py` to query data from Pinot and visualize it:

```bash
python transit-analysis.py
```

## File Overview

- **`zookeeper-setup.yaml`**: Configuration file for deploying Zookeeper.
- **`kafka-setup.yaml`**: Configuration file for setting up Kafka.
- **`schema2.json`**: Pinot schema definition file.
- **`table2.json`**: Pinot table definition file.
- **`kafka_data.py`**: Python script to fetch data from an API and produce Kafka topics.
- **`transit-analysis.py`**: Python script to query data from Pinot and generate visualizations.

---

Feel free to reach out for any questions or issues related to the setup!

