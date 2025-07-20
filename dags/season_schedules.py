from airflow.decorators import dag, task

@dag(dag_id="seasons_schedule",)
def seasons_schedule():
    @task
    def print_season_schedule():
        print("This is the season schedule task")
        # Here you can add logic to fetch and print the season schedule

    print_season_schedule()

seasons_schedule()