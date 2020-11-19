from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
For example one test could be a SQL statement that checks if certain column contains NULL values 
by counting all the rows that have NULL in the column. We do not want to have any NULLs 
so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')