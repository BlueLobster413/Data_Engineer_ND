from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = "INSERT INTO {} {}; COMMIT;"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "",
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        postgres_hook.run("DELETE FROM {}".format(self.table))
        self.log.info(f"{self.table} is cleared with old data")
        
        format_insert_sql = LoadFactOperator.insert_sql.format(self.table, sql)
        
        postgres_hook.run(format_insert_sql)
        self.log.info(f"{self.table} is created and loaded")
