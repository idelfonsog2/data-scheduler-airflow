from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Idelfonso',
    'depends_on_past': False,
    'email': ['idelfonsog2@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2018, 11, 1)
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="data-pipelines-idelfonso",
    s3_key="log_data",
    target_table="staging_events",
    json_file_path="s3://data-pipelines-idelfonso/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="data-pipelines-idelfonso",
    s3_key="song_data",
    target_table="staging_songs",
    json_file_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table="songplays",
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table="users",
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table="songs",
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table="artists",
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table="time",
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_check= [
        { 'check_sql': 'SELECT COUNT(*) FROM artists WHERE (name IS NULL);', 'expected_result': '0' },
        { 'check_sql': 'SELECT COUNT(*) FROM users WHERE (level IS NULL);', 'expected_result': '0' },
        { 'check_sql': 'SELECT COUNT(*) FROM songs WHERE (title IS NULL);', 'expected_result': '0' },
        { 'check_sql': 'SELECT COUNT(*) FROM time WHERE (weekday IS NULL);', 'expected_result': '0' },
        { 'check_sql': 'SELECT COUNT(*) FROM songplays WHERE (user_agent IS NULL);', 'expected_result': '0' }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator


