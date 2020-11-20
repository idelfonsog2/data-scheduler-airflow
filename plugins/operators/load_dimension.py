from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 target_table="",
                 redshift_conn_id="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.target_table= target_table
        self.redshift_conn_id= redshift_conn_id
        self.sql_statement= sql_statement
        self.append_data = append_data

    def execute(self, context):
        self.log.info('LoadDimensionOperator')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            self.log.info(f'Inserting into table {self.target_table} ...')
            redshift.run("INSERT INTO {target_table} {sql_statement}".format(target_table=self.target_table, sql_statement=self.sql_statement))
        else:
            self.log.info(f'Truncating from table {self.target_table} ...')
            redshift.run("TRUNCATE {target_table}".format(target_table=self.target_table))

            self.log.info(f'Inserting into table {self.target_table} ...')
            redshift.run("INSERT INTO {target_table} {sql_statement}".format(target_table=self.target_table, sql_statement=self.sql_statement))

        self.log.info(f'End of LoadFactOperator {self.target_table} ...')