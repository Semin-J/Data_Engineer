from datetime import datetime, timedelta
import os
import create_tables
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 2, 12),
    'retries': 3
    'retry_delay': timedelta(minutes=2),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

# Additional task to create tables in redshift
create_table = PostgresOperator(
    task_id = "Create_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = create_tables.create_all_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json_path = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/B/C/TRABCEI128F424C983.json",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplay",
    sql = "songplay_table_insert"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql = "user_table_insert",
    truncate_insert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    sql = "song_table_insert",
    truncate_insert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    sql = "artist_table_insert",
    truncate_insert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql = "time_table_insert",
    truncate_insert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    target_table="songs",
    sql="""
        SELECT COUNT(*)
        FROM {}
        WHERE {} IS NULL;
        """.format("songs","title"),
    expected_result = 0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task Lineage
start_operator >> create_table
create_table >> stage_events_to_redshift
create_table >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
