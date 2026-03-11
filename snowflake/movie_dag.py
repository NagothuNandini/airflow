from datetime import datetime
from airflow.decorators import dag
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Global Variable for Connection ID
CONN_ID = "snowflake"

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['movie'],
)
def movie_pipeline():

    # 1. Load Movie Task (Directly from your screenshot logic)
    load_movie_to_snowflake = aql.load_file(
        task_id='load_movie_to_snowflake',
        input_file=File(
            path='https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv'
        ),
        output_table=Table(
            name='imdb_movies',
            conn_id=CONN_ID,
        ),
    )


# Call the DAG function
movie_pipeline()
