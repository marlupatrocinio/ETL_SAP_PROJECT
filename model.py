import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from google.cloud import storage
import os
import re
import csv
import json
from io import StringIO
import logging

class dataProcessingFunctions:
    @staticmethod
    def parse_csv_line(line: str, delimiter: str = ',') -> list:
        try:
            f = StringIO(line)
            reader = csv.reader(f, delimiter=delimiter)
            return next(reader)
        except StopIteration:
            logging.warning(f"Empty line encountered: '{line}'")
            return None
        except Exception as e:
            logging.error(f"Error parsing CSV line '{line}' with delimiter '{delimiter}': {e}")
            return None
        
    @staticmethod
    def apply_transformations(data: dict, table_config: dict) -> dict:
        if data is None:
            return None
        bq_schema_fields = table_config['bq_schema']
    transformed_data = {}
    for field in bq_schema_fields:
        field_name = field['name']
        transformed_data[field_name] = None
        try:
            if isinstance(data, list) and len(data) > i:
                current_value = data[i]
            else:
                logging.warning(f"Data for field '{field_name}' not found at index {i} for row: {data}")
                current_value = None

            if current_value is None or current_value == '':
                transformed_data[field_name] = None
            elif field['type'] == 'INTEGER':
                try:
                        transformed_data[field_name] = int(current_value)
                except ValueError:
                        logging.warning(f"Cannot convert '{current_value}' to INTEGER for field '{field_name}'. Setting to None.")
                        transformed_data[field_name] = None
            elif field['type'] in ['FLOAT', 'BIGNUMERIC']:
                    try:
                        transformed_data[field_name] = float(current_value)
                    except ValueError:
                        logging.warning(f"Cannot convert '{current_value}' to NUMERIC/FLOAT for field '{field_name}'. Setting to None.")
                        transformed_data[field_name] = None
            elif field['type'] == 'BOOLEAN':
                    transformed_data[field_name] = str(current_value).lower() in ['true', '1', 'yes']
            elif field['type'] == 'DATE':
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', str(current_value)):
                        transformed_data[field_name] = str(current_value)
                    else:
                        logging.warning(f"Date format for '{field_name}' ({current_value}) is not YYYY-MM-DD. Setting to None or trying parse.")
                        transformed_data[field_name] = None
            else:
                    transformed_data[field_name] = str(current_value)

        except IndexError:
                logging.error(f"Index out of range for field '{field_name}' at index {i}. Row: {data}")
                transformed_data[field_name] = None
        except Exception as e:
                logging.error(f"General error mapping field '{field_name}' (value: {data[i] if isinstance(data, list) and len(data) > i else 'N/A'}): {e}")
                transformed_data[field_name] = None

        transform_cfg = table_config.get('transformation_config', {})

        for field_name in transform_cng.get('string_to_uppercase', []):
             if field_name in transformed_data and transformed_data[field_name] is not None:
                transformed_data[field_name] = str(transformed_data[field_name]).upper().strip()
        for source_field, bool_map in transform_cfg.get('boolean_fields_map', {}).items():
            if source_field in transformed_data and transformed_data[source_field] is not None:
                source_value = str(transformed_data[source_field]).strip()
                for key_value, target_bool_field in bool_map.items():
                    transformed_data[target_bool_field] = (source_value == key_value)
        return transformed_data
    
def run():
    # 1. Configurar Argumentos da Linha de Comando
    parser = argparse.ArgumentParser(description="Dataflow pipeline for Bronze to Silver layer transformation.")
    parser.add_argument('--table_key', required=True,
                        help='Key of the table to process (e.g., "sales_orders") from table_configs.json.')
    parser.add_argument('--config_file_path', default='config/table_configs.json',
                        help='Path to the JSON configuration file.')
    parser.add_argument('--input_gcs_bucket', required=True,
                        help='Name of the GCS bucket for raw data (Bronze Layer).')
    parser.add_argument('--raw_data_prefix', default='raw_sap_data/',
                        help='Prefix (folder) within the GCS bucket where raw SAP data is stored.')
    parser.add_argument('--project', required=True,
                        help='Your Google Cloud project ID.')
    parser.add_argument('--region', default='us-central1',
                        help='The GCP region to run the Dataflow job.')
    parser.add_argument('--temp_location', required=True,
                        help='GCS path for Dataflow temporary files.')
    parser.add_argument('--staging_location', required=True,
                        help='GCS path for Dataflow staging files.')
    parser.add_argument('--runner', default='DataflowRunner',
                        help='The Beam runner to use (e.g., DirectRunner, DataflowRunner).')
    
    # Adicionar uma opção para sobrescrever ou anexar dados no BigQuery
    parser.add_argument('--bq_write_disposition', default='WRITE_APPEND',
                        choices=['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'],
                        help='BigQuery write disposition. Default is WRITE_APPEND.')

    args, beam_args = parser.parse_known_args()

    # 2. Configurar Opções do Pipeline Apache Beam
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).runner = args.runner
    pipeline_options.view_as(GoogleCloudOptions).project = args.project
    pipeline_options.view_as(GoogleCloudOptions).region = args.region
    pipeline_options.view_as(StandardOptions).temp_location = args.temp_location
    pipeline_options.view_as(StandardOptions).staging_location = args.staging_location

    # 3. Carregar Configuração da Tabela Específica
    try:
        with open(args.config_file_path, 'r') as f:
            all_table_configs = json.load(f)
        
        # Encontrar a configuração para a table_key fornecida
        current_table_config = None
        for config in all_table_configs:
            if config.get('table_key') == args.table_key:
                current_table_config = config
                break

        if not current_table_config:
            raise ValueError(f"Table key '{args.table_key}' not found in configuration file '{args.config_file_path}'.")

        logging.info(f"Loaded configuration for table: {args.table_key}")
        # logging.debug(f"Config details: {json.dumps(current_table_config, indent=2)}") # Descomente para depurar

    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {args.config_file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from config file: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred loading table config: {e}")
        raise

    # 4. Listar Arquivos no Cloud Storage para a Tabela Atual
    source_gcs_path = (
        f"gs://{args.input_gcs_bucket}/"
        f"{args.raw_data_prefix.strip('/')}/"
        f"{current_table_config['source_gcs_folder'].strip('/')}/*.csv"
    )
    # A listagem dinâmica de arquivos é feita internamente pelo ReadFromText quando você usa curingas.
    # No caso de um job batch, o Dataflow irá enumerar os arquivos no Worker.
    # Se você precisar de lógica de listagem mais complexa (ex: apenas arquivos de hoje),
    # você pode usar a `google.cloud.storage` API no Driver antes de passar a lista exata para ReadFromText.
    # Para este exemplo, manteremos o padrão curinga simples que é mais comum.

    logging.info(f"Source GCS path for {args.table_key}: {source_gcs_path}")

    # 5. Definir o Caminho de Saída do BigQuery
    target_bq_full_table_id = (
        f"{args.project}:"
        f"{current_table_config['target_bq_dataset']}."
        f"{current_table_config['target_bq_table']}"
    )
    logging.info(f"Target BigQuery table for {args.table_key}: {target_bq_full_table_id}")

    # 6. Construir o Pipeline Dataflow
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | f'Read_{args.table_key}_CSV' >> beam.io.ReadFromText(source_gcs_path, skip_header_lines=1) # Assume cabeçalho
         | f'Parse_{args.table_key}_Line' >> beam.Map(
             DataProcessingFunctions.parse_csv_line_generic,
             delimiter=current_table_config['file_delimiter']
           )
         | f'Filter_None_Parsed_{args.table_key}' >> beam.Filter(lambda row_list: row_list is not None)
         | f'Map_And_Transform_{args.table_key}' >> beam.Map(
             DataProcessingFunctions.apply_generic_transformations,
             table_config=current_table_config # Passa a configuração da tabela para a transformação
           )
         | f'Filter_None_Transformed_{args.table_key}' >> beam.Filter(lambda row_dict: row_dict is not None)
         | f'Write_{args.table_key}_To_BigQuery' >> beam.io.WriteToBigQuery(
             table=target_bq_full_table_id,
             schema={'fields': current_table_config['bq_schema']}, # Use o esquema da configuração
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=getattr(beam.io.BigQueryDisposition, args.bq_write_disposition), # Use o argumento de linha de comando
             insert_method='STREAMING_INSERTS' # Para maior eficiência em tempo real/quase real
         ))

if __name__ == '__main__':
    logging.info("Starting Dataflow pipeline construction...")
    run()
    logging.info("Dataflow pipeline construction complete. Job will be submitted if runner is DataflowRunner.")