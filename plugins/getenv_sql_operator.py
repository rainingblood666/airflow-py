from airflow.sdk import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

class GetEnvSQLOperator(BaseOperator):
    def __init__(self,  mssql_conn_id: str,  **kwargs) -> None:
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
   
    def execute(self, context):
        mssql_conn_id = self.mssql_conn_id
        hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        sql = "SELECT DEFAULT_DOMAIN() AS env"
        env = hook.get_pandas_df (sql=sql).iloc[0, 0]
        print("env:")
        print(env)
        return env