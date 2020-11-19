from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


"""
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. 
The operator creates and runs a SQL COPY statement based on the parameters provided. 
The operator's parameters should specify where in S3 the file is loaded and what is the target table.

it must contain a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills
"""
class StageToRedshiftOperator(BaseOperator):
    # airflow will use context variables to render this template
    # before it gets pass into the operator
    template_fields = ("s3_key",) 
    ui_color = '#358140'
    copy_sql = """
        COPY {target_table}
        FROM '{s3_location}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        REGION 'us-east-2'
        COMPUPDATE OFF
        JSON '{json_file_path}';
        """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 target_table="",
                 json_file_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.json_file_path = json_file_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        logging.info(f'Deleting from table {self.target_table} ...')
        redshift.run("DELETE FROM {}".format(self.target_table))

        rendered_key = self.s3_key.format(**context)
        logging.info(f'Rendered key {rendered_key} ...')

        s3_location = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        logging.info(f'S3 Path {s3_location} ...')

        sql_formatted = StageToRedshiftOperator.copy_sql.format(
            target_table=self.target_table,
            s3_location=s3_location,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            json_file_path=self.json_file_path)

        redshift.run(sql_formatted)





