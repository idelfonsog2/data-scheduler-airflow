from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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
        COPY '{target_table}'
        FROM '{s3_path}'
        CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}'
        REGION 'us-west-2'
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

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        redshift.run("DELETE FROM {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(s3_bucket, rendered_key)
        sql_formatted = StageToRedshiftOperator.copy_sql.format(
            target_table=self.target_table,
            s3_location=s3_path,
            access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            json_file_path=self.json_file_path)

        redshift.run(sql)





