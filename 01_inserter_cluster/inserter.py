#!/usr/bin/env python3

from pathlib import Path
import pyarrow.parquet as pq
import clickhouse_connect
from minio import Minio
import os, yaml, logging

# Configuration ###################################################

ymlfile = """
scraper:
  target_page: "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
  retry_on_page: 3
storage:
  minio_url: "s3.pub1.infomaniak.cloud"
  minio_access_key: ""
  minio_secret_key: password
  minio_container: "taxi-nyc"
clickhouse:
  user: ""
  password: ""
  url: ""
  database: ""
log_level: INFO
"""

local_path = os.path.abspath(os.path.dirname(__file__))

if os.path.isfile(os.path.join(local_path, "config.yml")):

  with open(os.path.join(local_path, "config.yml"), "r") as ymlfile:
    yaml_settings = yaml.load(ymlfile, Loader=yaml.FullLoader)

  minio_url     = yaml_settings['storage']['minio_url']
  minio_access_key    = yaml_settings['storage']['minio_access_key']
  minio_secret_key     = yaml_settings['storage']['minio_secret_key']
  minio_container     = yaml_settings['storage']['minio_container']
  clickhouse_user     = yaml_settings['clickhouse']['user']
  clickhouse_password     = yaml_settings['clickhouse']['password']
  clickhouse_url     = yaml_settings['clickhouse']['url']
  clickhouse_database     = yaml_settings['clickhouse']['database']
  log_level         = yaml_settings['log_level']


logging.basicConfig(level=log_level)
root = logging.getLogger()
hdlr = root.handlers[0]
json_format = logging.Formatter('{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
hdlr.setFormatter(json_format)

## Functions ####################################################################

def get_client_clickhouse():
  client_clickhouse = clickhouse_connect.get_client(
      host=clickhouse_url,
      database=clickhouse_database, 
      username=clickhouse_user, 
      password=clickhouse_password)
  return client_clickhouse

def get_client_minio():
  client_minio = Minio(
    minio_url,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
  )
  return client_minio

def insertion(file_to_insert):

  logging.info(f'{file_to_insert} - start')
  try:
    logging.info(f'{file_to_insert} - connexion to clickhouse')
    client_clickhouse = get_client_clickhouse()
  except:
    logging.error(f'{file_to_insert} - impossible connexion to clickhouse')

  logging.info(f'{file_to_insert} - check if the file was already processed')
  parameters = { file_to_insert }
  already_exists = client_clickhouse.command('SELECT count(*) FROM taxis.state_inserter where file_work_in_progress = %s',parameters=parameters)
  
  
  logging.info(f'{file_to_insert} - check if the file was already processed')

  if already_exists == 0:
    logging.info(f'{file_to_insert} - add it to state_inserter')
    row = [[file_to_insert]]
    client_clickhouse.insert('taxis.state_inserter', row, column_names=['file_work_in_progress'])
    logging.info(f'{file_to_insert} - pull the file')
    tmp_file = Path(tmp_dir,file_to_insert, exist_ok=False)

    try:
      logging.info(f'{file_to_insert} - connexion to minio')
      client_minio = get_client_minio()
    except:
      logging.error(f'{file_to_insert} - impossible connexion to minio')

    logging.info(f'{file_to_insert} - get the file')
    client_minio.fget_object(minio_container,file_to_insert,tmp_file)

    logging.info(f'{file_to_insert} - open parquet file')
    parquet_file = pq.ParquetFile(tmp_file)
    logging.info(f'{file_to_insert} - insertion begin')
    for i in parquet_file.iter_batches(batch_size=100000):
      chunk_df = i.to_pandas()
      try:
        client_clickhouse.insert_df('taxis.trips', chunk_df)
      except:
        pass
    logging.info(f'{file_to_insert} - insertion ended')
    tmp_file.unlink()

    logging.info(f'{file_to_insert} - temp file cleaned')
  else:
    logging.info(f'{file_to_insert} - already processed')

  logging.info(f'{file_to_insert} - ended')


## Main ######################""

if __name__ == '__main__':

  logging.info(f'prepare temp directory')

  tmp_dir = Path("/tmp/inserter/")
  tmp_dir.mkdir(parents=True, exist_ok=True)

  try:
    logging.info(f'connexion to clickhouse')
    client_clickhouse = get_client_clickhouse()
  except:
    logging.error(f'impossible connexion to clickhouse')

  try:
    logging.info(f'create state_inserter table if needed')
    client_clickhouse.command("CREATE TABLE IF NOT EXISTS taxis.state_inserter ON CLUSTER xavki_cluster (file_work_in_progress String) Engine=ReplicatedMergeTree('/clickhouse/data/tables/state/{shard}', '{replica}') ORDER BY tuple()")
  except:
    logging.error(f'impossible connexion to create table state_inserter')

  try:
    logging.info(f'connexion to minio')
    client_minio = get_client_minio()
  except:
    logging.error(f'impossible connexion to minio')

  try:
    logging.info(f'list files already exist')
    s3_list_files = client_minio.list_objects(minio_container, recursive=True)
  except:
    logging.error(f'impossible to list files')

  list_to_process = []
  for obj in s3_list_files:
    list_to_process.append(obj.object_name)

  try:
    for file in list_to_process:
      logging.info(f'processing {file}')
      insertion(file)
  except:
    logging.info(f'error processing {file}')

