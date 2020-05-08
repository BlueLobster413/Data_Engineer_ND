from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """  COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    JSON '{}' """

    @apply_defaults
    def __init__(self,
                    redshift_conn_id = "",
                    aws_conn_id = "",
                    table = "",
                    s3_bucket="",
                    s3_key="",
                    s3_json_path = "",                    
                    *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.table = table
        self.s3_json_path = s3_json_path
        
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        credentials = aws_hook.get_credentials()
        
#         create_events_sql = """ CREATE TABLE IF NOT EXISTS staging_events (artist varchar(256),
#                                                                             auth varchar(256),
#                                                                             firstname varchar(256),
#                                                                             gender varchar(256),
#                                                                             iteminsession int4,
#                                                                             lastname varchar(256),
#                                                                             length numeric(18,0),
#                                                                             "level" varchar(256),
#                                                                             location varchar(256),
#                                                                             "method" varchar(256),
#                                                                             page varchar(256),
#                                                                             registration numeric(18,0),
#                                                                             sessionid int4,
#                                                                             song varchar(256),
#                                                                             status int4,
#                                                                             ts int8,
#                                                                             useragent varchar(256),
#                                                                             userid int4 ); """
#         create_songs_sql = """ CREATE TABLE IF NOT EXISTS staging_songs (num_songs int4,
#                                                                         artist_id varchar(256),
#                                                                         artist_name varchar(256),
#                                                                         artist_latitude numeric(18,0),
#                                                                         artist_longitude numeric(18,0),
#                                                                         artist_location varchar(256),
#                                                                         song_id varchar(256),
#                                                                         title varchar(256),
#                                                                         duration numeric(18,0),
#                                                                         "year" int4 );"""
        
        #postgres_hook.run("DELETE FROM {}".format(self.table))
        self.log.info(f"Creating {self.table} table")
#         if self.table == "staging_events":
#             postgres_hook.run(create_events_sql)
#         elif self.table == "staging_songs":
#             postgres_hook.run(create_songs_sql)
        
        self.log.info(f"Copying {self.table} data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table, 
                                                                s3_path,
                                                                credentials.access_key,
                                                                credentials.secret_key,
                                                                self.s3_json_path)

        postgres_hook.run(formatted_sql)
        
        
        