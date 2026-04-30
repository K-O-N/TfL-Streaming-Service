# Data Ingestion
install uv and initialise 
 pip install uv 
 uv init 

Use uv to add dependencies 
kakfa-python pyarrow pandas 

create a docker compose file 
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


