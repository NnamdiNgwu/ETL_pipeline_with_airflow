import time
from datetime import datetime, timedelta
import logging
import csv
from tempfile import NamedTemporaryFile # to flush file from saving in working directory
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd


default_args={
    'owner':'Nnamdi',
    'retries':5,
    'retry_delay':timedelta(minutes=5)}

@task()
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

@task()
def load_src_data(tbl_dict: dict):
    hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    all_tbl_name = []
    start_time = time.time()
    # access the table_name element in dictionaries
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

@task()
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
    # df = df.rename(columns={"shipregion": "shipRegionNew"})
    df.to_sql('stg_orders', engine, if_exists='replace', index=False)
    conn.close()
    cursor.close()
    logging.info("transfomed successfully")

@task()
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

@task()
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
    #df.fax.fillna(value='999', inplace=True)
    df.fillna({"region": "999"}, inplace=True)
    print(df)
    df.to_sql('stg_customers', engine, if_exists='replace', index=False)
    conn.close()
    cursor.close()
    logging.info("transfomed successfully")

# Load
@task()
def postgres_load_s3(data_interval_start=None, data_interval_end=None):
    # querry data from postgres db and save in a text file
    if not data_interval_start or not data_interval_end:
        hook = PostgresHook(postgres_conn_id='postgres_localhost_northwinddb')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(" select min(orderdate), max(orderdate) from stg_orders" )
        min_date, max_date = cursor.fetchone()

        # Set the data interval based on the min and max dates
        if min_date and max_date:
            data_interval_start = min_date
            data_interval_end = max_date
        else:
            logging.info("No data available in stg_orders table ")    

    cursor.execute("select * from stg_orders where orderdate >= %s and orderdate < %s",
                   (data_interval_start, data_interval_end))

    with NamedTemporaryFile(mode='w',suffix=f"{data_interval_start}" ) as f:
       csv_writer = csv.writer(f)
       csv_writer.writerow([i[0] for i in cursor.description])
       csv_writer.writerows(cursor)
       f.flush()
       cursor.close()
       conn.close()
       logging.info("save stg_orders date to text file successfuly: %s", f"dags/get_orders{data_interval_start}.txt")

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

@task()
def customer_order_model():
     hook = PostgresHook(postgres_conn_id= 'postgres_localhost_northwinddb')
     engine = hook.get_sqlalchemy_engine()
     conn = hook.get_conn()
     cursor = conn.cursor()
     tb_order = pd.read_sql_query("select * from stg_orders", conn)
     tb_custom = pd.read_sql_query(" select * from stg_customers", conn)
     #join stg_orders and stg_customers
     merged = tb_order.merge(tb_custom, on="customerid")
     merged.to_sql('customer_order_model', engine, if_exists='replace', index=False)
     conn.close()
     cursor.close()
     logging.info(" table successfuly joined")

with DAG(
    dag_id='automated_ETL_with_taskflowAPI_v04',
    default_args=default_args,
    start_date=datetime(2023, 5, 26),
    end_date=datetime(2023, 5, 28),
    schedule_interval='0 8 * * *',
    # tags=['table_model']

) as dag:
    
    with TaskGroup("extract_table_load", tooltip=" Extract and load source data") as  extract_load_src:
        src_table = extract_src_tables()
        load_table_data = load_src_data(src_table)
        # task order
        src_table >> load_table_data

    with TaskGroup("transform_src_tables", tooltip=" Transform and stage data ", ) as transform_src_tabe:
        transform_src_order = transform_src_orders()
        transform_src_category = transform_src_categories()
        transform_src_customer = transform_src_customers()
        # task order
        [transform_src_order, transform_src_category ,transform_src_customer]

    with TaskGroup("postgres_load_s3", tooltip="Extract staged data and load to S3 bucket ") as load_postgres_to_s3:
        postgres_to_s3 = postgres_load_s3()
        transform_src_order = transform_src_orders()
        # define task order
        transform_src_order >> postgres_to_s3

    with TaskGroup("load_customer_order_model", tooltip="final product model") as  load_customer_order_model:
        custo_order_model = customer_order_model()
        # task order
        custo_order_model

    # Define the overall DAG structure
    extract_load_src >> transform_src_tabe >> [load_customer_order_model, load_postgres_to_s3 ] 
