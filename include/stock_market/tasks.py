from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio
import json
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def _get_stock_prices(url, symbol):
    import requests
    import json
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'] )

    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    
    client = _get_minio_client()
    
    if not client.bucket_exists(bucket_name=BUCKET_NAME):
        client.make_bucket(bucket_name=BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f"{objw.bucket_name}/{symbol}"

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)

    for obj in objects:
        if obj.object_name and obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('THe csv file does not exist')

def _load_csv_to_postgres(obj_name):

    import pandas as pd
    
    client = _get_minio_client()
    obj = client.get_object(bucket_name=BUCKET_NAME, object_name=obj_name)
    df = pd.read_csv(BytesIO(obj.read()))

    pg_hook = PostgresHook('postgres')
    engine = pg_hook.get_sqlalchemy_engine() 

    df.to_sql('stock_market', engine, schema="public", if_exists="replace", index=False)

    
    





