import time
from datetime import datetime, timedelta
import logging
import csv
from tempfile import NamedTemporaryFile # to flush file from saving in working directory
from airflow import DAG
from airflow import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
import pandas as pd
from sqlalchemy import create_engine


default_args={
    'owner':'Nnamdi',
    'retries':5,
    'retry_delay':timedelta(minutes=5)}

def extract_src_tables():
      hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
      conn = hook.get_conn()
      cursor = conn.cursor()
      sql=""" select relname as table_name
            from pg_catalog.pg_class
            where relkind = 'r'
            and relname in ('orders', 'categories', 'customers')
            and relnamespace = (
                select oid
                from pg_catalog.pg_namespace
                where nspname = 'public'
            )"""
      df  = hook.get_pandas_df(sql)
      cursor.close()
      conn.close()
      tbl_dict = df.to_dict('dict')
      logging.info(" Extration is done")
      return tbl_dict

def load_src_data(tbl_dict: dict):
    hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    all_tbl_name = []
    start_time = time.time()
    # access the table_name element in dictionaries
    #for k, v in tbl_dict['table_name'].items():
    for table_name, table_data in tbl_dict['table_name'].items():   
        
        all_tbl_name.append(table_data)
        rows_imported = 0
        sql= f'select * from {table_data}'
        cursor.execute(sql)
        column_name = [i[0] for i in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_name)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {table_data} ')
        df.to_sql(f'src_{table_data}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
    cursor.close()
    conn.close()
    logging.info(f" Data imported successfuly", {str(round(time.time() - start_time, 2))})
    return all_tbl_name  

def transform_src_orders():
    hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql=""" select * from src_orders """
    pdf = pd.read_sql_query(sql, conn)
    df = pdf.copy(deep=True)
    df.shipregion.fillna(value='999', inplace=True)
    df.shippeddate.fillna(value='999', inplace=True)
    df.shippostalcode.fillna(value='999', inplace=True)
    df = df.rename(columns={"shipregion": "shipRegionNew"})
    df.to_sql('stg_orders', engine, if_exists='replace', index=False)
    conn.close()
    cursor.close()
    logging.info("transfomed successfully")

def transform_src_categories():
    hook = PostgresHook(postgres_conn_id="postgres_localhost_northwinddb")
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql="""
        select * from src_categories
    """
    pdf = pd.read_sql_query(sql, conn)
    df = pdf.copy(deep=True)
    df = df.drop(columns='categorynaame', axis=1)
    df.to_sql('stg_categories', engine, if_exists="replace", index=False)
    conn.close()
    cursor.close()
    logging.info("transformed successfuly")

def transform_src_customers():
    hook = PostgresHook(postgres_conn_id="postgres_localhost_northwinddb")   
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql="""
        select * from src_customers
     """ 
    pdf = pd.read_sql_query(sql, conn)
    df = pdf.copy(deep=True)
    df.region.fillna(value='999', inplace=True)
    df.fillna({'fax':'999'}, inplace=True)
    print(df)
    df.to_sql('stg_customers', engine, if_exists='replace', index=False)
    conn.close()
    cursor.close()
    logging.info("transfomed successfully")

# Load
def postgres_load_s3(data_interval_start, data_interval_end):
    # querry data from postgres db and save in a text file
    hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from stg_orders where orderdate >=%s and orderdate <%s",
                        (data_interval_start, data_interval_end))
    with NamedTemporaryFile(mode='w',suffix=f"{data_interval_start}" ) as f:
       csv_writer = csv.writer(f)
       csv_writer.writerow([i[0] for i in cursor.description])
       csv_writer.writerows(cursor)
       f.flush()
       cursor.close()
       conn.close()
       #logging.info("save stg_orders date to text file successfuly: %s", f"dags/get_orders{data_interval_start}.txt")
       print("save stg_orders date to text file successfuly: %s", f"dags/get_orders{data_interval_start}.txt")


       #load text file into s3 bucket
       s3_hook = S3Hook(aws_conn_id="minio_S3_conn")
       s3_hook.load_file(
           filename = f.name,
           key=f"orders/{data_interval_start}.txt",
           bucket_name ="airflow",
           replace= True,
           #encrypy= True
       )
       logging.info("stg_orders file %s has been pushed to s3", f.name)

def customer_order_model():
     hook = PostgresHook(postgres_conn_id= 'postgres_localhost_northwinddb')
     engine = hook.get_sqlalchemy_engine()
     conn = hook.get_conn()
     cursor = conn.cursor()
     tb_order = pd.read_sql_query("select * from stg_orders", conn)
     tb_custom = pd.read_sql_query(" select * from stg_customers", conn)
     #join stg_orders and stg_customers
     merged = tb_order.merge(tb_custom, on='customerid')
     merged.to_sql('customer_order_model', engine, if_exists='replace', index=False)
     conn.close()
     cursor.close()
     logging.info(" table successfuly joined")

with DAG(
    dag_id='dag_with_automate_etl_v31',
    default_args=default_args,
    start_date=datetime(2023, 5, 25),
    end_date=datetime(2023, 5, 27),
    schedule_interval='@daily'

) as dag:
    task1 = PythonOperator(
        task_id = 'load_src_data',
        python_callable=load_src_data,
        op_kwargs={'tbl_dict': extract_src_tables()}
    )
    task2 = PythonOperator(
        task_id = 'transform_src_orders',
        python_callable= transform_src_orders
    )
    task3 = PythonOperator( 
        task_id = 'transform_src_categories',
        python_callable= transform_src_categories
        )
    task4 = PythonOperator(
        task_id = 'transform_src_customers',
        python_callable= transform_src_customers
    )
    task5 = PythonOperator(
        task_id = 'postgres_load_s3',
        python_callable= postgres_load_s3,
        op_kwargs={
            'data_interval_start': '{{ dag.start_date.strftime("%Y-%m-%d") }}',
            'data_interval_end': '{{dag.start_date.strftime("%Y-%m-%d") }}'
        }
    )
    task6 = PythonOperator(
        task_id ='customer_order_model',
        python_callable= customer_order_model
    )
    task1 >> [task2, task3, task4]
    [task2, task4]>>task6
    task2 >> task5
