#!/usr/bin/env python3

import os, io, uuid, yaml, logging
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from minio import Minio
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pathlib import Path

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
log_level: INFO
"""

local_path = os.path.abspath(os.path.dirname(__file__))

if os.path.isfile(os.path.join(local_path, "config.yml")):

  with open(os.path.join(local_path, "config.yml"), "r") as ymlfile:
    yaml_settings = yaml.load(ymlfile, Loader=yaml.FullLoader)

  target_page     = yaml_settings['scraper']['target_page']
  retry_on_page     = yaml_settings['scraper']['retry_on_page']
  minio_url     = yaml_settings['storage']['minio_url']
  minio_access_key    = yaml_settings['storage']['minio_access_key']
  minio_secret_key     = yaml_settings['storage']['minio_secret_key']
  minio_container     = yaml_settings['storage']['minio_container']
  log_level         = yaml_settings['log_level']

logging.basicConfig(level=log_level)
root = logging.getLogger()
hdlr = root.handlers[0]
json_format = logging.Formatter('{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
hdlr.setFormatter(json_format)

## Functions ####################################################################

def get_client():

  client = Minio(
    minio_url,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
  )

  return client

def process_clean_norm(file_url, client):

  filename = os.path.basename(file_url)
  tmp_file_name = "tmp_" + filename

  logging.info(f'prepare temp directory')
  dest_tmp_dir = Path("/tmp/uploader/")
  dest_tmp_dir.mkdir(parents=True, exist_ok=True)

  dest_file_path = Path(dest_tmp_dir,tmp_file_name)
  final_file = Path(dest_tmp_dir,filename)

  logging.info(f'{filename} - started')
  
  retries = Retry(total=retry_on_page, backoff_factor=1)
  adapter = HTTPAdapter(max_retries=retries)
  session = requests.Session()
  session.mount('https://', adapter)

  logging.info(f'{filename} - create temporary file')  
  file_content = session.get(file_url)
  open(dest_file_path, 'wb').write(file_content.content)

  logging.info(f'{filename} - read parquet file')  
  df = pd.read_parquet(dest_file_path)
  
  # general fields
  logging.info(f'{filename} - generate general fields') 
  df['file'] = filename
  df['uuid'] = df.apply(lambda _: str(uuid.uuid4()), axis=1)

  ## PassengerCount
  logging.info(f'{filename} - process passenger_count') 
  if 'Passenger_Count' in df.columns:
    df.rename(columns = {'Passenger_Count':'passenger_count'},inplace=True)
  df.loc[df['passenger_count'] == "",['passenger_count']] = 100
  df['passenger_count'] = df['passenger_count'].fillna(100)
  df['passenger_count'] = df['passenger_count'].astype('Int64')  

  ## TripDistance
  logging.info(f'{filename} - process trip_distance') 
  if 'Trip_Distance' in df.columns:
    df.rename(columns = {'Trip_Distance':'trip_distance'},inplace=True)
  
  ## PaymentType
  logging.info(f'{filename} - process payment_type') 
  if 'Payment_Type' in df.columns:
    df.loc[(df['Payment_Type'] == "Credit") | (df['Payment_Type'] == "CREDIT"),['payment_type']] = 1
    df.loc[(df['Payment_Type'] == "Cash") | (df['Payment_Type'] == "CASH"),['payment_type']] = 2
    df.loc[df['Payment_Type'] == "No Charge",['payment_type']] = 3
    df.loc[df['Payment_Type'] == "Dispute",['payment_type']] = 4
    df['payment_type'] = df['payment_type'].astype('Int64')
    df.drop(columns=['Payment_Type'],inplace=True)

  # Remove
  logging.info(f'{filename} - remove congestion_surcharge') 
  if 'congestion_surcharge' in df.columns:
    df.drop(columns=['congestion_surcharge'],inplace=True)
  if 'improvement_surcharge' in df.columns:
    df.drop(columns=['improvement_surcharge'],inplace=True)
  if 'airport_fee' in df.columns:
    df.drop(columns=['airport_fee'],inplace=True)


  ## FareAmt
  logging.info(f'{filename} - process fare_amount') 
  if 'Fare_Amt' in df.columns:
    df.rename(columns = {'Fare_Amt':'fare_amount'},inplace=True)

  ## TipAmt
  logging.info(f'{filename} - process tip_amount') 
  if 'Tip_Amt' in df.columns:
    df.rename(columns = {'Tip_Amt':'tip_amount'},inplace=True)

  ## TollsAmnt
  logging.info(f'{filename} - process tolls_amount') 
  if 'Tolls_Amt' in df.columns:
    df.rename(columns = {'Tolls_Amt':'tolls_amount'},inplace=True)

  ## TotalAmt
  logging.info(f'{filename} - process total_amount') 
  if 'Total_Amt' in df.columns:
    df.rename(columns = {'Total_Amt':'total_amount'},inplace=True)

  ## Airport
  logging.info(f'{filename} - process airport_fee') 
  if 'Airport_Fee' in df.columns:
    df.rename(columns = {'Airport_Fee':'airport_fee'},inplace=True)

  ## RateCode
  logging.info(f'{filename} - process rate_code') 
  if 'RatecodeID' in df.columns:
    df.rename(columns = {'RatecodeID':'rate_code'},inplace=True)
  if 'Rate_Code' in df.columns:
    df.drop(columns=['Rate_Code'],inplace=True)
  if 'rate_code' not in df.columns:
    df['rate_code'] = 1000
  df.loc[df['rate_code'] == "",['rate_code']] = 1000
  df['rate_code'] = df['rate_code'].fillna(1000)
  df['rate_code'] = df['rate_code'].astype('Int64')  

  ## VendorId
  logging.info(f'{filename} - process vendor_id') 
  if 'VendorID' in df.columns:
    df.rename(columns = {'VendorID':'vendor_id'},inplace=True)
  if 'vendor_name' in df.columns:
    df.loc[df['vendor_name'] == "VTS",['vendor_name']] = 2
    df.loc[df['vendor_name'] == "CMT",['vendor_name']] = 1
    df.loc[df['vendor_name'] == "DDS",['vendor_name']] = 0
    df['vendor_name'] = df['vendor_name'].astype('Int64')
    df.rename(columns = {'vendor_name':'vendor_id'},inplace=True)

  ## TripPickUp
  logging.info(f'{filename} - process tpep_pickup_datetime') 
  if 'Trip_Pickup_DateTime' in df.columns:
    df.rename(columns = {'Trip_Pickup_DateTime':'tpep_pickup_datetime'},inplace=True)
  if 'pickup_datetime' in df.columns:
    df = df.rename(columns = {'pickup_datetime':'tpep_pickup_datetime'},inplace=True)
  df.loc[df['tpep_pickup_datetime'] == "",['tpep_pickup_datetime']] = np.nan
  df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])  

  ## TripDropOff
  logging.info(f'{filename} - process tpep_dropoff_datetime') 
  if 'Trip_Dropoff_DateTime' in df.columns:
    df.rename(columns = {'Trip_Dropoff_DateTime':'tpep_dropoff_datetime'},inplace=True)
  if 'dropoff_datetime' in df.columns:
    df.rename(columns = {'dropoff_datetime':'tpep_dropoff_datetime'},inplace=True)
  df.loc[df['tpep_dropoff_datetime'] == "",['tpep_dropoff_datetime']] = np.nan
  df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])  

  ## PickupLong
  logging.info(f'{filename} - process pickup_longitude') 
  if 'Start_Lon' in df.columns:
    df.rename(columns = {'Start_Lon':'pickup_longitude'},inplace=True)
  if 'pickup_longitude' not in df.columns:
    df['pickup_longitude'] = np.nan

  ## PickupLat
  logging.info(f'{filename} - process pickup_latitude') 
  if 'Start_Lat' in df.columns:
    df.rename(columns = {'Start_Lat':'pickup_latitude'},inplace=True)
  if 'pickup_latitude' not in df.columns:
    df['pickup_latitude'] = np.nan    

  ## DropOffLong
  logging.info(f'{filename} - process dropoff_longitude') 
  if 'End_Lon' in df.columns:
    df.rename(columns = {'End_Lon':'dropoff_longitude'},inplace=True)
  if 'dropoff_longitude' not in df.columns:
    df['dropoff_longitude'] = np.nan

  ## DropOffLat
  logging.info(f'{filename} - process dropoff_latitude') 
  if 'End_Lat' in df.columns:
    df.rename(columns = {'End_Lat':'dropoff_latitude'},inplace=True)
  if 'dropoff_latitude' not in df.columns:
    df['dropoff_latitude'] = np.nan

  ## StoreFwd
  logging.info(f'{filename} - process store_and_forward') 
  if 'store_and_fwd_flag' in df.columns:
    df.rename(columns = {'store_and_fwd_flag':'store_and_forward'},inplace=True)
  df['store_and_forward'] = df['store_and_forward'].astype('str')

  logging.info(f'{filename} - process PULocationID') 
  if 'PULocationID' not in df.columns:
    df['PULocationID'] = np.nan_to_num(np.nan)

  logging.info(f'{filename} - process DOLocationID') 
  if 'DOLocationID' not in df.columns:
    df['DOLocationID'] = ""
    df['DOLocationID'] = np.nan_to_num(np.nan)

  ## surcharge
  logging.info(f'{filename} - process surcharge') 
  if 'extra' in df.columns:
    df.rename(columns = {'extra':'surcharge'},inplace=True)

  logging.info(f'{filename} - generate final file') 
  df.to_parquet(final_file)
  del df

    # if needed to store it in memory
    #bytes_data = df.to_parquet()
    #buffer = io.BytesIO(bytes_data)
    #raw_bytes = buffer.getbuffer().nbytes
    #client.put_object("taxi-nyc", f, buffer, raw_bytes)

  logging.info(f'{filename} - push the file to minio') 
  client.fput_object(minio_container, filename ,final_file)

  logging.info(f'{filename} - clean files') 
  os.remove(dest_file_path)
  os.remove(final_file)
  logging.info(f'{filename} - ended')


### Main ##########################################################

if __name__ == '__main__':

  logging.info(f'Get the page')
  r = requests.get(target_page)
  logging.info(f'Get the soup')
  soup = bs(r.text, "lxml")

  list_process_files = []
  list_s3_files = []
  
  logging.info(f'Connect to S3 bucket')
  try:
    client = get_client()
  except:
    logging.error(f'impossible connexion to minio')

  try:
    s3_list_files = client.list_objects(minio_container, recursive=True)
    s3_list_files = [obj.object_name for obj in s3_list_files]
  except:
    logging.error(f'impossible to collect object files')

  logging.info(f'Create the file list')
  for a in soup.findAll('a', href=True):
    file_url = a['href']
    if file_url.endswith('.parquet') and "yellow" in file_url :
      if os.path.basename(file_url) not in s3_list_files:
        list_process_files.append(file_url)

  number_lines = len(list_process_files)

  logging.info(f'{number_lines} file need to be processed')

  for f in list_process_files:
    process_clean_norm(f,client)