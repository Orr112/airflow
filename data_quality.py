import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
       

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        check_sql = """
        Select count(*) from {}
    """
        tables = ['staging_events','artists','songs','staging_songs','time','users','songplays']
        for table in tables:
            formatted_sql = check_sql.format(
            table)
            count = redshift_hook.get_records(formatted_sql)
            self.log.info(count)
            if count is None or count == 0:
                print(redshift_hook.run(formatted_sql))
                print(count)
                raise ValueError(f"{table} is an empty table")
            else:
               self.log.info(f"Data quality check: {table} has a count of {count}records") 
