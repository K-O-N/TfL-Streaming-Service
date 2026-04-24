# TfL-Streaming-Service
About An end-to-end data engineering pipeline capturing real-time TfL (Transport for London) vehicle data. Features orchestration using Airflow, a streaming ingestion layer (Python/Kafka), Infrastructure as Code (Terraform), a GCP-based Data Lakehouse (GCS/BigQuery), and modular transformations with dbt

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
Verify the GCS Connector is active: ``` curl http://localhost:8083/connector-plugins | grep GcsSinkConnector``` 
3. Start pushing data to the producer 
```
# Sync dependencies
uv sync

# Run the producer (Starts streaming Arrivals, StopPoints, and Line Status)
uv run producer.py
```
4. GCS Sink Configuration
Submit the connector configuration to start moving data from Redpanda to your GCS bucket:
```
curl -X POST -H "Content-Type: application/json" --data @gcs_sink.json http://localhost:8083/connectors
```
## Project Structure

├── Airflow/                # DAGs and Airflow configuration
│   └── credentials/        # Service account JSON keys
├── Stream-Ingestion/       # Streaming logic
│   ├── Dockerfile          # Custom Kafka Connect image (GCS plugin)
│   ├── docker-compose.yaml # Redpanda & Connect stack
│   ├── producer.py         # Python Kafka Producer
│   └── gcs_sink.json       # Kafka Connect Sink configuration
├── terraform/              # Infrastructure as Code
│   ├── main.tf             # GCS & BigQuery definitions
│   └── variables.tf        
├── dbt_tfl/                # dbt Transformation project
│   ├── models/             # SQL Models (Staging -> Marts)
│   └── dbt_project.yml
└── README.md

## Data Pipeline Details
Topics Ingested
- tfl-arrivals: Real-time predicted arrival times for 100+ bus lines.
- tfl-stoppoints: Metadata regarding bus stop locations and facilities.
- tfl-line-status: Current service disruptions and delays.

## Transformation Logic (dbt)
- Staging: Casting JSON strings to appropriate BigQuery types (Timestamps, Integers).
- Intermediate: Joining Arrivals with StopPoints to get geographic coordinates.
- Marts: Calculating "Average Delay per Line" and "Station Congestion Indexes."

## dbt Transformation DAGS
![](model_Dag.jpeg)


## Final Dashboard
Click the preview below to explore the live interactive dashboard:

[![TfL Real-Time Report](Transport_for_London_Report.jpeg)](https://datastudio.google.com/reporting/380c05f4-f1ed-47a7-bf4f-8a4acb9924bc)

[ View Live Looker Studio Report](https://datastudio.google.com/reporting/380c05f4-f1ed-47a7-bf4f-8a4acb9924bc)
