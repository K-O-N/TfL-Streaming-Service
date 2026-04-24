# TfL-Streaming-Service
An end-to-end data engineering pipeline capturing real-time TfL (Transport for London) vehicle data. Features orchestration using Airflow, a streaming ingestion layer (Python/Kafka), Infrastructure as Code (Terraform), a GCP-based Data Lakehouse (GCS/BigQuery), and modular transformations with dbt

Problem Statement: real-time Transport for London (TfL) operational data is highly dynamic and fragmented across multiple sources, making it difficult to ingest, store, and analyse in a consistent and scalable way. This project addresses that challenge by building an end-to-end data engineering pipeline that captures streaming TfL data, reliably lands it in a cloud data lake, and transforms it into structured, analytics-ready datasets for reporting and decision-making.

# System Architecture
| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Infrastructure** | **Terraform** | IaC for GCS Buckets, BigQuery Datasets, and IAM roles. |
| **Data Ingestion** | **Kafka Connect (GCS Sink)** | Streaming raw JSON events into Google Cloud Storage. |
| | **uv** | Ultra-fast Python package and virtualenv management. |
| **Storage** | **GCS** | Raw Data Lake (Bronze Layer). |
| **Orchestration** | **Airflow** | Manage data flow from GCS to BigQuery and DAG triggering. |
| **Warehouse** | **BigQuery** | Centralized Cloud Data Warehouse. |
| **Modeling** | **dbt** | Data transformations and testing layer. |
| **BI Tool** | **Looker Studio** | Real-time visualization of bus arrivals and line status. |

# Architecture Diagram
![](architecture.jpeg)

## Key Capabilities
- Real-time ingestion of TfL bus data
- Scalable cloud data lake architecture
- Fully containerised local development environment
- Automated infrastructure provisioning (IaC)
- Modular dbt transformation pipeline
- Live analytics dashboarding

## Preequisites 
* Docker Installed locally
* Google cloud Account with billing enabled, and a service account and credentials 
* A Transport for London API Key

# Getting Started 
1. Infrastructure Setup (Terraform)
Provision your Google Cloud resources first.
```
cd Setup
terraform init
terraform apply 
```

2. Services Deployment (Docker)
This spins up Redpanda and the Kafka Connect cluster with the GCS Sink plugin pre-installed.
```
cd Stream-Ingestion
docker-compose up -d --build
```
3. Verify the GCS Connector is active: ``` curl http://localhost:8083/connector-plugins | grep GcsSinkConnector``` 
Start pushing data to the producer 
```
# Sync dependencies
uv sync

# Run the producer (Starts streaming Arrivals, StopPoints, and Line Status)
uv run producer.py
```
this streams the following endpoints 
- arrivals
- stoppoints
- line status
  
4. GCS Sink Configuration
Submit the connector configuration to start moving data from Redpanda to your GCS bucket:
```
curl -X POST -H "Content-Type: application/json" --data @gcs_sink.json http://localhost:8083/connectors
```
5. Start Airflow container and and trigger dags
```
cd Airflow
docker compose up -d 
```
## Project Structure

## Data Pipeline Details
Topics Ingested
- tfl-arrivals: Real-time predicted arrival times for 100+ bus lines.
- tfl-stoppoints: Metadata regarding bus stop locations and facilities.
- tfl-line-status: Current service disruptions and delays.

## Design Principles
* Decoupled architecture (ingestion / orchestration / transformation)
* ELT-first approach (load raw → transform later)
* Cloud-native stack (GCP)
* Fully containerised local development
* Modular and extensible pipeline design

## Transformation Logic (dbt)
- Staging: Casting JSON strings to appropriate BigQuery types (Timestamps, Integers).
- Intermediate: Joining Arrivals with StopPoints to get geographic coordinates.
- Marts: Calculating "Average Delay per Line" and "Station Congestion Indexes."

## dbt Transformation Dag
![](model_Dag.jpeg)

## Final Dashboard
Click the preview below to explore the live interactive dashboard:

[![TfL Real-Time Report](Transport_for_London_Report.jpeg)](https://datastudio.google.com/reporting/380c05f4-f1ed-47a7-bf4f-8a4acb9924bc)

[ View Live Looker Studio Report](https://datastudio.google.com/reporting/380c05f4-f1ed-47a7-bf4f-8a4acb9924bc)
