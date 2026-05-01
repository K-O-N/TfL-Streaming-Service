# Streaming TfL Data with Kafka and Confluent
Transport for London (TfL) provides dynamic, real-time data feeds—such as vehicle locations and arrival predictions—that update every 30 to 60 seconds. 
To capture and process these ephemeral data points without data loss, a streaming architecture using Apache Kafka and Confluent is used.

In this setup, a lightweight producer script continuously polls the TfL REST API and immediately publishes the JSON data payload into a Kafka topic. Confluent functions as the centralized event backbone, ensuring that the high-volume stream of vehicle positions is handled reliably. Downstream consumers or data connectors can then safely validate and write the stream into your GCS data lake without overloading the source API.

## Getting started 
1. Install and initialise uv
```
 pip install uv 
 uv init
```

2. Use uv to add dependencies kakfa-python pyarrow pandas
```
uv add kakfa-python pyarrow pandas
```

4. create a docker compose file 
create using redpandas.. 

update docker-compose file with the connect service 
add a dockerfile 

rebuild and start container 

confirm the plugins exist 
curl http://localhost:8083/connector-plugins
i




curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "gcs-sink-tfl",
    "config": {
      "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
      "tasks.max": "1",
      "topics": "tfl-stoppoints, tfl-line-status, tfl-arrivals",
      "confluent.topic.bootstrap.servers": "redpanda:29092",
      "confluent.topic.replication.factor": "1",
      "confluent.license.topic.replication.factor": "1",
      "confluent.metadata.replication.factor": "1",
      "gcs.bucket.name": "tflsystem-bucket",
      "gcs.part.size": "5242880",
      "flush.size": "50",
      "rotate.interval.ms": "60000",
      "format.class": "io.confluent.connect.gcs.format.json.JsonFormat",
      "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }'


  curl http://localhost:8083/connectors/gcs-sink-tfl/status 
  curl http://localhost:8083/connectors/gcs-sink-tfl/status

  
{"name":"gcs-sink-tfl","connector":{"state":"RUNNING","worker_id":"connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"connect:8083"}],"type":"sink"}(stream-ingestion) 



  FROM confluentinc/cp-kafka-connect:latest

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-gcs:latest


