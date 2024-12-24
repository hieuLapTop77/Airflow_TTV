import glob
import os

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.helper import download_file_drive
from common.hook import hook
from common.variables import FOLDER_ID_KHACHHANG, TEMP_PATH

FOLDER_NAME = 'khachhang'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="20 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "khach hang"],
    max_active_runs=1,
)
def Misa_khach_hang():
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
        return download_file_drive(folder_name=FOLDER_NAME, folder_id=FOLDER_ID_KHACHHANG)

    ######################################### INSERT DATA ################################################
    @task
    def insert_khach_hang() -> None:
        list_file_local = download_latest_file()
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql = """
                    INSERT INTO [dbo].[3rd_misa_khachhang](
                        [Ma_khach_hang],[Ten_mo_hinh_kinh_doanh],[Ten_khach_hang],[Dia_chi],[Ten_kenh_phan_phoi],[Code_kenh_phan_phoi],
                        [Ten_tinh],[Ten_quan_huyen],[Code_vi_tri],[Nhan_vien_kinh_doanh],[Don_vi_phu_trach],[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
                """
        if not list_file_local:
            print("No data available to process")
            sql_conn.close()
            return

        for file_local in list_file_local:
            df = pd.read_excel(
                file_local, index_col=None, engine='openpyxl')
            if not df.empty:
                df["Mã Khách hàng"] = df["Mã Khách hàng"].fillna(0)
                df["Mã Khách hàng"] = df["Mã Khách hàng"].astype(np.int64)
                df["Mã Khách hàng"] = df["Mã Khách hàng"].astype(str)
                ma_khach_hang_list = tuple(df["Mã Khách hàng"].tolist())
                for i in range(0, len(ma_khach_hang_list), 200):
                    sql_del = f"delete from [dbo].[3rd_misa_khachhang] where Ma_khach_hang in {ma_khach_hang_list[i: i+200]};"
                    print(sql_del)
                    cursor.execute(sql_del)
                values = []
                print(df.columns.tolist())

                for _index, row in df.iterrows():
                    value = (
                        str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(
                            row[5]), str(row[6]), str(row[7]), str(row[8]), str(row[9]), str(row[10])
                    )
                    values.append(value)
                print(values[0])
                cursor.executemany(sql, values)

                print(
                    f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
                sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    insert_khach_hang() >> remove_files()


dag = Misa_khach_hang()
