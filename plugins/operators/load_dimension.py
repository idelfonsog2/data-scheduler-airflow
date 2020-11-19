from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Take as input a SQL statement and target database on which to run the query against
Also a target table name must be pass. It will contain the result of the transformation

Dimension loads are often done with the truncate-insert pattern where the target table 
is emptied before the load. Thus, you could also have a parameter that allows switching 
between insert modes when loading dimensions. Fact tables are usually so massive that 
they should only allow append type functionality.
"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
