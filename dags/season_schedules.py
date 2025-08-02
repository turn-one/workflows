from datetime import timedelta
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from airflow.models import Variable
import fastf1
import pandas as pd


@dag(
    dag_id="seasons_schedule",
)
def seasons_schedule():
    last_processed_season = int(
        Variable.get(
            "last_processed_season",
            default_var=1950,
        )
    )
    season = last_processed_season + 1

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        task_id="get_season_schedule",
    )
    def get_season_schedule(season: int):
        with fastf1.Cache.disabled():
            schedule = pd.DataFrame(
                fastf1.get_event_schedule(
                    season,
                )
            )
        return schedule.to_json(
            orient="records",
        )

    @task.pyspark(
        conn_id="spark-kubernetes",
    )
    def load_into_bronze_catalog(spark: SparkSession, schedule: str):
        print(schedule)
        spark.sql("SHOW CATALOGS").show()

    schedule = get_season_schedule(season)
    load_into_bronze_catalog(schedule)


seasons_schedule()
