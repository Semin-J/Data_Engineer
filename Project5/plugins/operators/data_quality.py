from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 target_table = "",
                 sql = "",
                 expected_result = 0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        self.target_table = target_table,
        self.sql = sql,
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql)
        
        if len(records[0]) < 1 or len(records):
            raise ValueError(f"Data Quality check failed. {self.taret_table} no results")
        
        res = records[0][0]
        
        if res != self.expected_result:
            raise ValueError(f"Data Quality check failed. Results are not matched")
        logging.info(f"{self.target_table} data quality check is successful!")