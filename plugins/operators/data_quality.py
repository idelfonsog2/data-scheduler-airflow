from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.check = check

    def execute(self, context):
        self.log.info('DataQualityOperator')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(check['check_sql'])[0]
         
        if records[0] != check['expected_result']:
            ValueError(f"Data quality check failed. {check['table']} contains null in id column, got {records[0]} instead")
   