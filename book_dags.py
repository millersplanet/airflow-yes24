from datetime import datetime
import json
from airflow import DAG
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from crawl_books import get_books
from to_postgres import save_to_postgres

def complete():
    print("yes24_top_200_수집 완료")

default_args = {
    "start_date" : datetime(2024, 1, 1)
}

with DAG(
    dag_id="yes24_book_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    tags=['book','yes24'],
    catchup=False,
) as dag:
    
    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="airflow-superset",
        sql='''
        CREATE TABLE IF NOT EXISTS yes_book(
            rank integer,
            title TEXT,
            author text,
            price integer,
            discount_rate integer,
            publishing_house text,
            publication_date text,
            rate numeric,
            review_count integer,
            link text
            )
            '''
    )
    
    get_data_result = PythonOperator(
        task_id="get_books",
        python_callable=get_books,
        provide_context=True,
        dag=dag
    )
    
    save_postgres = PythonOperator(
        task_id="save_postgres",
        python_callable=save_to_postgres,
        provide_context=True,
        dag=dag
    )
    
    print_complete = PythonOperator(
            task_id="print_complete",
            python_callable=complete
    )
    
    
    
creating_table >> get_data_result >> save_postgres >> print_complete
