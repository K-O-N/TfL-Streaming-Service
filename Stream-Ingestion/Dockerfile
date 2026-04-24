FROM confluentinc/cp-kafka-connect:7.5.0

# Install the GCS Connector plugin
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-gcs:10.2.2

# The parent image already handles the /etc/confluent/docker/run entrypoint,
# so we don't need to specify a CMD or ENTRYPOINT here unless we are changing it.