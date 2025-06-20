import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsSingleton
from google.cloud import storage
import logging
import re
import os

# --- Configurações do Projeto ---
PROJECT_ID = 'proud-outpost-455911-s8'
BUCKET = 'etl_sap' 
REGION = 'us-central1'
BIGQUERY_DATASET = 'sap_etl'


def get_csv_files(bucket_name, prefix='input/'):
    """
    Lista os arquivos .csv em um prefixo do GCS e usa seus nomes como nomes de tabela.
    """
    table_names = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista os BLOBS (arquivos) sem o delimitador, para pegar os arquivos em vez de pastas
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        # Pega o nome do arquivo, remove o prefixo 'input/' e a extensão '.csv'
        file_path = blob.name
        if file_path.endswith('.csv'):
            table_name = os.path.basename(file_path).replace('.csv', '')
            table_names.append(table_name)
    
    logging.info(f"Tabelas descobertas no GCS: {table_names}")
    return table_names

class ParseWithHeader(beam.DoFn):
    def process(self, element, header):
        try:
            header_columns = [re.sub(r'[^a-zA-Z0-9_]', '', col).lower() for col in header.split(',')]
            data_columns = element.split(',')
            if len(header_columns) != len(data_columns):
                return
            record = dict(zip(header_columns, data_columns))
            yield record
        except Exception as e:
            logging.warning(f"Erro ao processar a linha {element}: {e}")
            pass

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        region=REGION,
        temp_location=f'gs://{BUCKET}/temp',
        staging_location=f'gs://{BUCKET}/staging',
        job_name='pipeline-dinamico-de-tabelas-final' # Mudei o nome para não confundir
    )
    
    table_names = get_csv_files(BUCKET, prefix='input/')

    if not table_names:
        logging.error("Nenhuma tabela .csv foi encontrada em gs://%s/input/. Verifique os arquivos.", BUCKET)
        return

    with beam.Pipeline(options=options) as p:
        for table_name in table_names:
            
            # <-- MUDANÇA 2: O CAMINHO AGORA APONTA PARA O ARQUIVO ESPECÍFICO ---
            gcs_input_path = f'gs://{BUCKET}/input/{table_name}.csv'
            
            lines = p | f'[{table_name}] Ler CSV' >> beam.io.ReadFromText(gcs_input_path)

            header = lines | f'[{table_name}] Extrair Cabeçalho' >> beam.combiners.Sample.FixedSizeGlobally(1) | f'[{table_name}] Formatar Cabeçalho' >> beam.Map(lambda x: x[0])

            data = lines | f'[{table_name}] Filtrar Cabeçalho' >> beam.Filter(lambda line, header: line != header, header=AsSingleton(header))

            parsed_data = data | f'[{table_name}] Parsear com Cabeçalho' >> beam.ParDo(ParseWithHeader(), header=AsSingleton(header))

            parsed_data | f'[{table_name}] Escrever no BigQuery' >> beam.io.WriteToBigQuery(
                table=f'{PROJECT_ID}:{BIGQUERY_DATASET}.{table_name}',
                schema='SCHEMA_AUTODETECT',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()