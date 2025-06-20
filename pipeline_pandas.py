import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsSingleton
from google.cloud import storage, bigquery
import logging
import re
import os
import datetime
import pandas as pd
import numpy as np
import io

PROJECT_ID = 'proud-outpost-455911-s8'
BUCKET = 'etl_sap' 
REGION = 'us-central1'
BIGQUERY_DATASET = 'sap_etl'
PREFIX = 'input/'

def process_csv_to_bq():
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET)
    
    blobs = bucket.list_blobs(prefix = PREFIX)
    
    for blob in blobs:
        if not blob.name.endswith('.csv'):
            continue
        
        table_name = os.path.basename(blob.name).replace('.csv', '')
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        logging.info(f"Processing {blob.name} into {table_id}")
        
        data = blob.download_as_text()
        
        try:
            df = pd.read_csv(
                io.StringIO(data),
                delimiter = ';',
                na_values=['#', '']
            )
        except Exception as e:
            logging.error(f"Erro ao ler CSV: {e}")
            continue
        
        df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col).lower() for col in df.columns]
        
        if 'VALUE' in df.columns:
            df['VALUE'] = df['VALUE'].str.replace(',', '.').astype(float)
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], format='%Y%m%d', errors='coerce')
            
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
        )
        
        try:
            job = bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Aguarda conclusão
            logging.info(f"Carregados {len(df)} registros na tabela {table_id}")
        except Exception as e:
            logging.error(f"Erro no BigQuery: {e}")

if __name__ == '__main__':
    process_csv_to_bq()