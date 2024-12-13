import json

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.hook import hook
from common.variables import MISA_API_TONKHO, MISA_APP_ID, MISA_TOKEN

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="5 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "Ton Kho", "Inventory"],
    max_active_runs=1,
)
def Misa_API_TonKho():
    ######################################### API ################################################
    @task
    def call_api_get_tonkho():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{MISA_TOKEN}"
        }
        body = {
            "app_id": f"{MISA_APP_ID}",
            "stock_id": "",
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "last_sync_time": None
        }
        while True:
            response = requests.post(
                MISA_API_TONKHO, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_ton_kho(data)
                body["skip"] += 1000
            else:
                print("Error please check api with error: ", response.json())
                break
    ######################################### INSERT DATA ################################################

    def insert_ton_kho(data: list) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
                    INSERT INTO [dbo].[3rd_misa_api_tonkho](
                        [inventory_item_id]
                        ,[inventory_item_code]
                        ,[inventory_item_name]
                        ,[stock_id]
                        ,[stock_code]
                        ,[stock_name]
                        ,[organization_unit_id]
                        ,[organization_unit_code]
                        ,[organization_unit_name]
                        ,[quantity_balance]
                        ,[amount_balance]
                        ,[unit_price]
                        ,[expiry_date]
                        ,[lot_no]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, getdate())
                """
        sql_select = "truncate table [3rd_misa_api_tonkho]"
        cursor.execute(sql_select)
        sql_conn.commit()
        df = pd.DataFrame(data)
        cols = ['inventory_item_id', 'inventory_item_code', 'inventory_item_name', 'stock_id', 'stock_code', 'stock_name',
                'organization_unit_id', 'organization_unit_code', 'organization_unit_name', 'quantity_balance', 'amount_balance',
                'unit_price', 'expiry_date', 'lot_no']
        df = df[cols]
        if not df.empty:
            values = [tuple(str(row[col]) if row[col] is not None else 'NULL' for col in cols)
                      for _, row in df.iterrows()]

            try:
                cursor.executemany(sql, values)
                print(
                    f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
                sql_conn.commit()
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                sql_conn.close()

    ############ DAG FLOW ############
    call_api_get_tonkho()


dag = Misa_API_TonKho()
