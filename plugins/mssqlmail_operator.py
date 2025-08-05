from airflow.sdk import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import io
from ftplib import FTP

class MsSQLMailOperator(BaseOperator):
    def __init__(self, env: str, mssql_conn_id: str, table: str,columns: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.env = env
        self.mssql_conn_id = mssql_conn_id
        self.table = table
        self.columns = columns


    def execute(self, context):
            def upload_df_to_ftp(df, ftp_host, ftp_user, ftp_pass, remote_path):
                # Convert DataFrame to CSV in memory
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                csv_buffer.seek(0)
                # Connect and upload
                with FTP(ftp_host) as ftp:
                    ftp.login(user=ftp_user, passwd=ftp_pass)
                    with io.BytesIO(csv_buffer.getvalue().encode('utf-8')) as bio:
                        ftp.storbinary(f'STOR {remote_path}', bio)
            env = self.env
            if env == 'prod':
               ftp_host='ftp.corp.com'
            else:
               ftp_host='host.docker.internal'
            
            table = self.table
            columns = self.columns
            mssql_conn_id = self.mssql_conn_id
            print("env="+env)
            hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
            sql = f"select count(*) qty from {table}(nolock)"
            qty = hook.get_first(sql=sql)[0]
            sql = f"select  {columns} from {table}(nolock)"
            print(str(qty))
            if qty < 500:
                df = hook.get_pandas_df (sql=sql)
                tableHTML=df.to_html(index=False, justify='left', border=1, classes='table table-striped table-bordered')
            else:
                tableHTML=f"<body>report for {table} is placed on ftp:// {ftp_host}</body>"
               
                df_generator = hook.get_pandas_df_by_chunks(sql=sql, chunksize=5000)
                i=0 
                for df in df_generator:
                    i += 1
                    df = df.convert_dtypes()
                    print(" Save to FTP:"+ftp_host)
                    upload_df_to_ftp(
                        df,
                        ftp_host=ftp_host,
                        ftp_user='anonymous',      # <-- replace with your FTP username
                        ftp_pass='anonymous',  # <-- replace with your FTP password
                        remote_path='for_csv/report_'+str(i)+'.csv'         )
            print("tableHTML:")
            print(tableHTML)
            queryBase = " EXEC Msdb.dbo.sp_send_dbmail "
            queryBase += " @profile_name ='stas' ,"
            queryBase +="@recipients = 'stan.mischuk@gmail.com',"
            queryBase +=f"@body = '{tableHTML}',"
            queryBase += "@body_format = 'HTML',"
            queryBase +=f"@subject = '{table}'"
            dbconn =  hook.get_conn()
            hook.set_autocommit(dbconn, True)
            cur = dbconn.cursor()      
            cur.execute(queryBase)  # Test connection
            cur.close()
       
