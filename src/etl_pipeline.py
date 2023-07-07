import shutil
import tempfile
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import os
import wget
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from rich import print
from google.cloud import storage, bigquery


SA_PATH = "./project-etl.json"


def months_to_str(month: list):
    month_range = '-'.join(str(month) for month in months)
    return month_range


def upload_to_gcs(parquet, months: list):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = storage.Client()
    bucket = client.get_bucket("mle-data-pipeline-project")

    file_name = f"{colour}_tripdata_2021-{months_to_str(months)}.parquet"

    bucket.blob(
        f'{colour}_taxi/{file_name}').upload_from_string(parquet, 'text/parquet')
    print("Successfully uploaded the data!")


@task(name="Extract Task",
      retries=3,
      retry_delay_seconds=60,
      log_prints=True)
def extract(url: str, months: list, url_template: str, gcs_destination: str) -> pd.DataFrame:
    """

    """

    temp_dir = tempfile.mkdtemp()

    try:
        df_taxi = pd.DataFrame()

        for month in months:
            url = url_template.replace('month', str(month).zfill(2))
            file_path = wget.download(url, out=temp_dir)

            monthly_df = pd.read_parquet(f'{file_path}')
            print(f'\n2021-{month} no of trips: {monthly_df.shape[0]}')

            df_taxi = pd.concat([df_taxi, monthly_df])
        print('\nUploading data to GCS...')
        parquet = df_taxi.to_parquet()

        upload_to_gcs(parquet, months)

    finally:
        # Clean up the temporary directory and its contents
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    return df_taxi


@task(name="Transform Task",
      retries=3,
      retry_delay_seconds=60,
      log_prints=True)
def transform(table_id: str):
    client = bigquery.Client()
    query = f"""
    SELECT 
        -- Reveneue grouping 
        PULocationID AS revenue_zone,
        EXTRACT(MONTH FROM lpep_pickup_datetime) as revenue_month,

        -- Revenue calculation 
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

        -- Additional calculations
        AVG(passenger_count) AS avg_montly_passenger_count,
        AVG(trip_distance) AS avg_montly_trip_distance
    FROM
        projectetl.{table_id}
    GROUP BY
        1, 2
    """

    view_ref = client.dataset('projectetl').table('revenue_report')

    view = bigquery.Table(view_ref)
    view.view_query = query

    try:
        client.get_table(view_ref)
        client.delete_table(view_ref)
    except:
        print('{table_ref} does not exist. Creating view')
    
    client.create_table(view)

    print("View created successfully.")


@task(name="Load Task",
      retries=3,
      retry_delay_seconds=60,
      log_prints=True)
def load(gcs_uri, table_id: str):
    """
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = bigquery.Client()

    table_ref = client.dataset('projectetl').table(table_id)

    try:
        client.get_table(table_ref)
        client.delete_table(table_ref)
    except:
        print('{table_ref} does not exist. Creating table')

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = True

    load_job = client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config)

    load_job.result()

    if load_job.state == "DONE":
        print("Table created successfully.")
    else:
        print("Table creation failed.")


@flow(name="Data Ingestion Flow")
def main_flow(colour: str, months: list):
    url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{colour}_tripdata_2021-month.parquet"
    gcs_destination = f"gs://mle-data-pipeline-project/green_taxi/green_tripdata_2021-{months_to_str(months)}.parquet"
    table_id = f"{colour}_taxi"
    extract(url_source, months, url_source, gcs_destination)
    load(gcs_destination, table_id)
    transform(table_id)


if __name__ == '__main__':
    colour = 'green'
    months = [1, 2, 3]
    main_flow(colour, months)
