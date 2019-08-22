from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

#Default paramaters for DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    
}

# Instantiate DAG
dag = DAG('udac_example_dag',
          catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# Dummy Task - no functionality
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Copy log files to staging table in Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id ="aws_credentials",
    table='staging_events',
    s3_bucket="udacity-dend",
    s3_key ="log_data/"
)

# Copy song files to staging table in Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id ="aws_credentials",
    table='staging_songs',
    s3_bucket="udacity-dend",
    s3_key = "song_data/A/A/A/TRAAAAK128F9318786.json"
)

""" Use staging tables to populate fact table"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="staging_events", 
    destination_table="songplays",
    load_type = 'insert',
    sql = SqlQueries.songplay_table_insert
)

""" Use staging tables to populate user table"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="staging_events",
    destination_table="user_table",
    load_type = "insert",
    sql = SqlQueries.user_table_insert
)

""" Use staging tables to populate song_table table"""
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="staging_songs",
    destination_table="song_table",
    load_type = "insert",
    sql = SqlQueries.song_table_insert
)

""" Use staging tables to populate artist table"""
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="staging_songs",
    destination_table="artist_table",
    load_type = "insert",
    sql = SqlQueries.artist_table_insert
)

""" Use Songplay table to populate Time table"""
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="songplays",
    destination_table="time_table",
    load_type = "insert",
    sql = SqlQueries.time_table_insert
)

""" Data check operator to ensure that all tables are populated """
Run_data_quality_checks =  DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table>> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table>> Run_data_quality_checks
load_user_dimension_table >> Run_data_quality_checks
load_artist_dimension_table >> Run_data_quality_checks
load_time_dimension_table >> Run_data_quality_checks
Run_data_quality_checks >> end_operator