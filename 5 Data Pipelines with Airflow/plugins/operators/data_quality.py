from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    check_sql = "SELECT COUNT(*) FROM {};"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            format_sql = DataQualityOperator.check_sql.format(table)
            number = postgres_hook.run(format_sql)
            if number > 0:
                self.log.info(f"{table} table is loaded with {number} rows and checked successfully.")
            else:
                self.log.info(f"{table} table has failed quality check")