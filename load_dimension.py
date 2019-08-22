from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

""" Operator to load data into dimensions table"""
class LoadDimensionOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql')
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 load_type="",
                 sql ="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.load_type = load_type
        self.sql = sql

    def execute(self, context):
        
        #Connect to Postgress Instance
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        """Assign selected query to variable based on insert or upsert selection"""
        if self.load_type == 'insert':
            insert_sql = "Insert into " + self.destination_table + self.sql
            redshift.run(insert_sql)
        elif self.load_type == 'append':
            append_sql = "Upsert into " + self.destination_table + self.sql
            redshift.run(append_sql)
       
