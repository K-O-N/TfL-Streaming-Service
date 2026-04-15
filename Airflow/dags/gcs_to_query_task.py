from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from datetime import datetime

BUCKET = 'tflsystem-bucket'
LANDING_PATH = 'topics/tfl-arrivals/partition=0/' 
TOPICS = {
    'arrivals': {'landing': 'topics/tfl-arrivals/partition=0/', 'staging': 'processing/arrivals/', 'table': 'arrivals_native'},
    'status': {'landing': 'topics/tfl-line-status/partition=0/', 'staging': 'processing/status/', 'table': 'line_status_native'},
    'stops': {'landing': 'topics/tfl-stoppoints/partition=0/', 'staging': 'processing/stoppoints/', 'table': 'stoppoints_dim'}
}
STAGING_PATH = 'processing/tfl-arrivals/'

SCHEMAS = {
    'arrivals': [
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'operationType', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'vehicleId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'naptanId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stationName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'lineId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'lineName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'platformName', 'type': 'STRING', 'mode': 'NULLABLE'},     
        {'name': 'direction', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bearing', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'tripId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'baseVersion', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'destinationNaptanId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'destinationName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timeToStation', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'currentLocation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'towards', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'expectedArrival', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timeToLive', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'modeName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timing_source', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timing_insert', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timing_read', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timing_sent', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timing_received', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}

    ],
    'status': [
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'modeName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'statusSeverity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'statusSeverityDescription', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'reason', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    'stops': [
        {'name': 'naptanId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'commonName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'lat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'lon', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'placeType', 'type': 'STRING', 'mode': 'NULLABLE'}
        
    ]
}


with DAG(
    'tfl_safe_ingestion_pipeline',
    start_date=datetime(2026, 3, 27),
    schedule='0 */3 * * *',  # Every 3 hours
    catchup=False,
    max_active_runs=1
) as dag:

    for key, info in TOPICS.items():
        # 1. MOVE to processing (prevents loading files while they are being written)
        move_files = GCSToGCSOperator(
            task_id=f'move_{key}_to_processing',
            source_bucket=BUCKET,
            source_object=f"{info['landing']}*",
            destination_bucket=BUCKET,
            destination_object=info['staging'],
            move_object=True,
        )

        # 2. LOAD to BigQuery with Fixed Schema
        # Arrivals = APPEND (historical), Status/Stops = TRUNCATE (current state)
        write_mode = 'WRITE_APPEND' if key == 'arrivals' else 'WRITE_TRUNCATE'

        load_bq = GCSToBigQueryOperator(
            task_id=f'load_{key}_to_bq',
            bucket=BUCKET,
            source_objects=[f"{info['staging']}*"],
            destination_project_dataset_table=f"tfl-system.tfl_system_raw.{info['table']}",
            source_format='NEWLINE_DELIMITED_JSON', 
            write_disposition=write_mode,
            schema_fields=SCHEMAS[key], # FIXED DATATYPES
            autodetect=False,
        )

        # 3. CLEANUP
        cleanup = GCSDeleteObjectsOperator(
            task_id=f'cleanup_{key}_staging',
            bucket_name=BUCKET,
            prefix=info['staging'],
        )

        move_files >> load_bq >> cleanup






























    # # 1. MOVE: "Freeze" the current batch by moving to a processing folder
    # stage_files = GCSToGCSOperator(
    #     task_id='move_to_processing',
    #     source_bucket=BUCKET,
    #     source_object=f'{LANDING_PATH}*', # Correct: Recursive search
    #     destination_bucket=BUCKET,
    #     destination_object=STAGING_PATH,   # Correct: Clean destination
    #     move_object=True,
    # )

    # # 2. LOAD: Added recursive search for subfolders
    # load_to_bq = GCSToBigQueryOperator(
    #     task_id='load_staging_to_native',
    #     bucket=BUCKET,
    #     source_objects=[f'{STAGING_PATH}*'], 
    #     destination_project_dataset_table='tfl-system.tfl_system_raw.arrivals_native',
    #     source_format='NEWLINE_DELIMITED_JSON',
    #     write_disposition='WRITE_APPEND',
    #     autodetect=True,
    # )

    # # 3. PURGE: Clean up the processing folder
    # cleanup_staging = GCSDeleteObjectsOperator(
    #     task_id='cleanup_processing_folder',
    #     bucket_name=BUCKET,
    #     prefix=STAGING_PATH,
    # )

    # stage_files >> load_to_bq >> cleanup_staging