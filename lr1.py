import pandas as pd
import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from elasticsearch import Elasticsearch


def read_file(**kwargs):
    folder_path = kwargs['folder_path']
    dataframes = []
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]

    for file_path in csv_files:
        data = pd.read_csv(file_path)
        dataframes.append(data)

    if dataframes:
        combined_data = pd.concat(dataframes, ignore_index=True)
    else:
        combined_data = pd.DataFrame()

    return combined_data


def filter_rows(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_file')

    filtered_data = data.dropna(subset=['designation', 'region_1'])
    return filtered_data


def fill_price_null(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='filter_rows')

    data['price'].fillna(0.0, inplace=True)
    return data


def save_to_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fill_price_null')

    output_path = kwargs['output_path']
    data.to_csv(output_path, index=False)


def save_to_elasticsearch(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fill_price_null')
    es = Elasticsearch("http://elasticsearch-kibana:9200")

    for _, row in data.iterrows():
        es.index(index="wine_airflow", body=row.to_dict())


with DAG(
    'lr1_dag',
    start_date=datetime(2024, 10, 24),
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='read_file',
        python_callable=read_file,
        op_kwargs={'folder_path': './data/input/'},
    )

    t2 = PythonOperator(
        task_id='filter_rows',
        python_callable=filter_rows,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='fill_price_null',
        python_callable=fill_price_null,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        op_kwargs={'output_path': './data/output/output.csv'},
        provide_context=True
    )

    t5 = PythonOperator(
        task_id='save_to_elasticsearch',
        python_callable=save_to_elasticsearch,
        provide_context=True
    )

    t1 >> t2 >> t3 >> [t4, t5]
