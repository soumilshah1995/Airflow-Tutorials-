try:
    import os
    import sys

    from datetime import timedelta,datetime
    from airflow import DAG

    # Operators
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator
    from airflow.utils.trigger_rule import TriggerRule

    from airflow.utils.task_group import TaskGroup

    import pandas as pd

    print("All Dag modules are ok ......")

except Exception as e:
    print("Error  {} ".format(e))


# ===============================================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'email': ['shahsoumil519@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}
dag  = DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False)
# ================================================


def read_file(**context):
    path = os.path.join(os.getcwd(), "dags/common/netflix_titles.csv")
    df = pd.read_csv(path)
    print(df.columns)
    context['ti'].xcom_push(key='df', value=df)


def process_type(**context):

    df = context.get("ti").xcom_pull(key="df")
    # df = pd.DataFrame(data=df)

    df["type"]  = df['type'].apply(lambda x:  'ok')
    context['ti'].xcom_push(key='type', value=df["type"].to_list())
    return df

def process_director(**context):
    df = context.get("ti").xcom_pull(key="df")
    # df = pd.DataFrame(data=df)
    df["director"]  = df['director'].apply(lambda x:  'ok')
    context['ti'].xcom_push(key='director', value= df["director"].to_list())
    return df

def process_title(**context):

    df = context.get("ti").xcom_pull(key="df")
    # df = pd.DataFrame(data=df)
    df["title"]  = df['title'].apply(lambda x:  'ok')
    context['ti'].xcom_push(key='title', value=df["title"].to_list())
    return df


def complete_task(**context):

    df = context.get("ti").xcom_pull(key="df")

    df["type"] = context.get("ti").xcom_pull(key="type")
    df["title"] = context.get("ti").xcom_pull(key="title")
    df["director"] = context.get("ti").xcom_pull(key="director")

    path = os.path.join(os.getcwd(), "dags/common/process.csv")
    df.to_csv(path)


with DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    read_file = PythonOperator(task_id="read_file",python_callable=read_file,provide_context=True,)

    with TaskGroup("processing_tasks") as processing_tasks:
        process_title = PythonOperator(task_id="process_title",python_callable=process_title,provide_context=True,)
        process_director = PythonOperator(task_id="process_director",python_callable=process_director,provide_context=True,)
        process_type = PythonOperator(task_id="process_type",python_callable=process_type,provide_context=True,)

    complete_task = PythonOperator(task_id="complete_task",python_callable=complete_task,provide_context=True,)


read_file >> processing_tasks
processing_tasks >> complete_task
