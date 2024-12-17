import glob
import math
import os

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from common.helper import download_file_drive
from common.hook import hook
from common.variables import FOLDER_ID_BANHANG, TEMP_PATH

FOLDER_NAME = 'banhang'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "ban hang"],
    max_active_runs=1,
)
def Misa_ban_hang():
    @task
    def remove_files():
        folder = os.path.join(TEMP_PATH, FOLDER_NAME)
        files = glob.glob(os.path.join(folder, '*'))
        for i in files:
            try:
                os.remove(i)
                print(f"Deleted file: {i}")
            except Exception as e:
                print(f"Error deleting file {i} : {e}")
    ######################################### API ################################################

    def download_latest_file() -> list:
        return download_file_drive(folder_name=FOLDER_NAME, folder_id=FOLDER_ID_BANHANG)

    def find_row(file_path: str):
        temp_df = pd.read_excel(file_path, index_col=None, engine='openpyxl')
        header_row = 0
        for i, row in temp_df.iterrows():
            if row.astype(str).str.contains("STT").any():
                header_row = i
                break
        return header_row
    ######################################### INSERT DATA ################################################

    @task
    def insert_ban_hang() -> None:
        list_file_local = download_latest_file()
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql = """
                    INSERT INTO [dbo].[3rd_misa_ban_hang](
                        [STT]
                        ,[Ngay_hach_toan]
                        ,[So_chung_tu]
                        ,[Khach_hang]
                        ,[Dia_chi]
                        ,[Dien_giai]
                        ,[Tong_tien_thanh_toan]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, getdate())
                """
        if len(list_file_local) > 0:
            for file_local in list_file_local:
                print("Insert data of file: ", file_local.split('/')[-1])
                header_row = 0
                try:
                    header_row = find_row(file_path=file_local) + 1
                except Exception as e:
                    print("Error at: ", e)
                    header_row = header_row + 1
                df = pd.read_excel(file_local, skiprows=header_row,
                                   index_col=None, engine='openpyxl', skipfooter=1)
                data = tuple([x for x in df['Số chứng từ'].tolist() if not (
                    isinstance(x, float) and math.isnan(x))])
                sql_del = f"delete from [dbo].[3rd_misa_ban_hang] where So_chung_tu in {data};"
                print(sql_del)
                cursor.execute(sql_del)
                sql_ban = """select So_chung_tu from [dbo].[3rd_misa_ban_hang]"""
                df_sql = pd.read_sql(sql_ban, sql_conn)
                df = df[~df['Số chứng từ'].isin(df_sql['So_chung_tu'])]
                values = []
                if not df.empty:
                    for _index, row in df.iterrows():
                        if 'nan' in str(row[2]):
                            break
                        value = (
                            str(row[0]),
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            str(row[4]),
                            str(row[5]),
                            str(row[6]),
                        )
                        values.append(value)
                    cursor.executemany(sql, values)

                print(
                    f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
                sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    insert_ban_hang() >> remove_files()


dag = Misa_ban_hang()
