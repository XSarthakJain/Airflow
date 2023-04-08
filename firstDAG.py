from datetime import timedelta
#DAG Object
from airflow import DAG

#Operators
from airflow.operators.dummy_aperator import Dummy_Operator
from airflow.operatots.python_operator import Python_Operator

#Initialize the default Arguments
default_args = {
    'owner' : "sarthak JAIN",
    'start_date': datetime(2023,4,8),
    'retries' : 3,
    'retry_delay' : timedelta(minutes = 5)
}

#Instantiate a DAG objects
hello_world_dag = DAG('hello_world_dag',default_args=default_args,description='Hello World DAG',schedule_interval='* * * * *',cathup=False,tags = ['example','hello World'])

#Pyhton Callable Function
def print_hello():
    return 'Hello World'

#Creating First Task
first_task = DummyOperator(task_id='start_task',dag=hello_world_dag)

#Creating Second Task
second_task = PythonOperator(task_id = 'second_task',python_callable=print_hello,dag=hello_world_dag)

#third_task
third_task = BashOperator(task_id='third_task',bash_command = 'echo 1',dag=hello_world_dag)
#SET Order Of Tasks
first_task >> second_task >> third_task