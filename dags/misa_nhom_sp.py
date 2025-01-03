import glob
import os

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.helper import download_file_drive
from common.hook import hook
from common.variables import FOLDER_ID_SANPHAM_NHOM, TEMP_PATH

FOLDER_NAME = 'nhomsanpham'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="50 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "group product", "nhóm sản phẩm"],
    max_active_runs=1,
)
def Misa_group_products():
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

    def download_latest_file() -> str:
        return download_file_drive(folder_name=FOLDER_NAME, folder_id=FOLDER_ID_SANPHAM_NHOM)

    ######################################### INSERT DATA ################################################
    @task
    def insert_group_products() -> None:
        list_local_file = download_latest_file()
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        if not list_local_file:
            print("No data available to process")
            sql_conn.close()
            return

        for local_file in list_local_file:
            df = pd.read_excel(local_file, index_col=None, engine='openpyxl')
            if not df.empty:
                sql_del = f"delete from [dbo].[3rd_misa_group_products] where [ma_hang] in {tuple([str(i) for i in df.iloc[:, 0].tolist()])};"
                print(sql_del)
                cursor.execute(sql_del)
                values = []

                sql = """
                        INSERT INTO [dbo].[3rd_misa_group_products](
                            [ma_hang],[ten_hang],[nhan_hang],[nhom_nhan_hang],[dtm_creation_date])
                        VALUES(%s, %s, %s, %s, getdate())
                    """
                for _index, row in df.iterrows():
                    if 'nan' in str(row[0]):
                        break
                    value = (str(row[0]), str(row[1]), str(row[2]), str(row[3]),
                             )
                    values.append(value)
                cursor.executemany(sql, values)

                print(
                    f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
                sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    insert_group_products() >> remove_files()


dag = Misa_group_products()
