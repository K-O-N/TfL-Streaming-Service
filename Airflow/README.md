# Data Ingestion and Orchestration

## Data Orchestration
For the orchestion of pipeline jobs, I will be using airflow. Airflow is used as the control plane of the pipeline, orchestrating batch ingestion from GCS into BigQuery. It does not handle streaming ingestion directly; instead, it coordinates downstream processing of data produced by Kafka Connect.

#### Airflow Setup
- Create a subdirectory in the data_ingestion folder, ```mkdir airflow```
- Download the official image docker-compose.yaml by running the following 
``` curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml'```
- Set the Airflow user, since this is a new setup, create directories to store my days, logs, plugins, and config
    ```
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
These directories ensure:

- dags/ → stores pipeline definitions
- logs/ → persists task execution logs
- plugins/ → custom operators/hooks (if needed later)
- config/ → Airflow configuration overrides

- Minimise the default docker-compose.yaml: the official image is quite heavy, hence, 
    - Disable Examples to keep the UI clean
    - Remove Services: airflow-worker, airflow-triggerer, flower, and redis
    - Switch to LocalExecutor from CeleryEXECUTOR in the yaml and comment out AIRFLOW_CELERY_* 

- Set up Google Cloud connection: building and running the official image with google cloud would run safely, however, would be unable to connect to/authenticate to our cloud warehouse/storage
    - Add the required google credentails path and also the credentails path to volumes
    ```
    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/credentials/google_credentials.json
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/opt/airflow/credentials/google_credentials.json'
    GCP_PROJECT_ID: 'xxx'
    GCP_GCS_BUCKET: 'xxx'
    ```
    - create a custom docker file using the airflow base image to install related google requirements - ref https://airflow.apache.org/docs/docker-stack/recipes.html
    ```
    ARG BASE_AIRFLOW_IMAGE
    FROM ${BASE_AIRFLOW_IMAGE}

    SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

    USER 0

    ARG CLOUD_SDK_VERSION=322.0.0
    ENV GCLOUD_HOME=/opt/google-cloud-sdk

    ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

    RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
        && TMP_DIR="$(mktemp -d)" \
        && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
        && mkdir -p "${GCLOUD_HOME}" \
        && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
        && "${GCLOUD_HOME}/install.sh" \
        --bash-completion=false \
        --path-update=false \
        --usage-reporting=false \
        --additional-components alpha beta kubectl \
        --quiet \
        && rm -rf "${TMP_DIR}" \
        && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
        && gcloud --version

    USER ${AIRFLOW_UID}
    ```
This enables:

running gcloud commands inside Airflow containerdebugging GCS/BigQuery issues directly from Airflow

- Create a requirements.txt file to install libraries via pip install (google specific client for airflow and pyarrow)
- Finally, uncomment the build: . under x-airflow-common. Comment out the image line (optional) or give a new name to the image


## How to Run
- Build your custom image: This will execute your Dockerfile, install vim, and set up the google-cloud-sdk
``` docker compose build``` 

- Initialize the Database with the new image: ``` docker compose up airflow-init ``` if you get an error or unhealthy database, it could be because of prexisting postgresdb data. Run ``` docker compose down --volumes --remove-orphans ``` to remove firstthen initialise the database with new image

- Start Airflow: ``` docker compose up``` once airflow is up, you can access the UI via localhost:8080

Data Handling Strategy

To ensure reliable and consistent data ingestion, files are processed using a three-stage workflow:

### Staging (Move Operation)
- Incoming files are first moved from the landing directory to a dedicated processing/ (staging) folder. This creates a stable snapshot of the data to be processed and isolates it from newly arriving files.
- Load to BigQuery
- Only the files in the staging folder are ingested into BigQuery. This guarantees that each batch represents a fixed set of data and avoids partial or inconsistent loads.
- Cleanup (Delete Operation): After a successful load, the processed files are deleted from the staging folder to prevent reprocessing in subsequent runs.


### Design Benefits
- Idempotency: If the Load task fails, the files are safe in the processing/ folder. You can fix the error and restart without losing data or creating duplicates.
- Zero Data Loss: By moving files first, you ensure that any file created by your Python producer while the DAG is running won't accidentally be deleted.
- Cleanliness: Your BigQuery External Table (if you still have it) will only show "pending" data that hasn't been moved to the Warehouse yet.
