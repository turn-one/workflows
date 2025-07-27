from airflow.decorators import dag, task
from pyspark.sql import SparkSession


@dag(
    dag_id="seasons_schedule",
)
def seasons_schedule():
    @task
    def print_season_schedule():
        print("This is the season schedule task")
        # Here you can add logic to fetch and print the season schedule

    @task.pyspark(
        conn_id="spark-kubernetes",
    )
    def pyspark_task(spark: SparkSession):
        print(spark.sparkContext.getConf().getAll())

    print_season_schedule()


seasons_schedule()
