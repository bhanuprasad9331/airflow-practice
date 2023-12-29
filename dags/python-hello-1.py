from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define a Python function that will be executed by the PythonOperator
def print_hello():
    print("Hello, Airflow!")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG with its ID, default arguments, schedule interval, and description
dag = DAG('hello_airflow', default_args=default_args, schedule_interval='@once', description='A simple DAG that prints Hello, Airflow!')

# Define the PythonOperator that will run the print_hello function
hello_operator = PythonOperator(
    task_id='print_hello_task',  # Unique task ID
    python_callable=print_hello,  # Function to be executed
    dag=dag,  # Assign the DAG to the operator
)

# Set the task dependencies
hello_operator
