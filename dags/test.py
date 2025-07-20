from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="example_kubernetes_executor",
    default_args=default_args,
    catchup=False,
    tags=["example", "kubernetes"],
) as dag:

    @task
    def task1():
        print("This is task 1")

    @task
    def task2():
        print("This is task 2")

    @task.kubernetes(
        image="python:3.12-slim",
        namespace="radio-check-dev",
        in_cluster=True,
    )
    def print_pattern():
        print("This is task 3 with custom image")
        n = 5
        for i in range(n):
            for _ in range(i + 1):
                print("* ", end="")
            print("\r")
    
    @task.virtualenv(
        task_id="task_virtualenv",
        python_version="3.12",
        requirements=["fastf1"],
        system_site_packages=False,
    )
    def task_virtualenv():
        import fastf1
        print("This is a virtualenv task using FastF1")
        print(fastf1.__version__)

    task1() >> task2() >> print_pattern() >> task_virtualenv()
