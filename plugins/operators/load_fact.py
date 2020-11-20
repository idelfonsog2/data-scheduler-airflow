from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 target_table="",
                 redshift_conn_id="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table= target_table
        self.redshift_conn_id= redshift_conn_id
        self.sql_statement= sql_statement

    def execute(self, context):
        self.log.info('Start of LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Inserting into table {self.target_table} ...')
        redshift.run("INSERT INTO {target_table} {sql_statement}".format(target_table=self.target_table, sql_statement=self.sql_statement))

        redshift.run(self.sql_statement)
        self.log.info(f'End of LoadFactOperator {self.target_table} ...')
