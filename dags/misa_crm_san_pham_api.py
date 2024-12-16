import json
from typing import Union

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.hook import hook
from common.variables import (
    MISA_CRM_CLIENT_ID,
    MISA_CRM_CLIENT_SECRET,
    MISA_CRM_GET_TOKEN,
    MISA_CRM_SAN_PHAM_API,
)

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
def Misa_CRM_SanPham_API():
    ######################################### API ################################################
    def call_api_get_token() -> Union[str, None]:
        body = {
            "client_id": MISA_CRM_CLIENT_ID,
            "client_secret": MISA_CRM_CLIENT_SECRET
        }
        response = requests.post(
            MISA_CRM_GET_TOKEN, json=body, timeout=None)
        if response.status_code == 200:
            print(response.json().get("data"))
            return response.json().get("data")
        else:
            print("Error please check api with error: ", response.json())
            return None

    @task
    def call_api_get_san_pham():
        bearer_token = call_api_get_token()
        if not bearer_token:
            print("No found bearer token for request")
            return None
        body = {
            "client_id": MISA_CRM_CLIENT_ID
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}"
        }
        params = {
            "page": 0,
            "pageSize": 100
        }
        while True:
            response = requests.get(
                MISA_CRM_SAN_PHAM_API, json=body, params=params, headers=headers, timeout=None)
            if response.status_code == 200:
                data = response.json().get("data")
                if not data:
                    break
                insert_data_don_hang(data)
                params["page"] += 1
            else:
                print("Error please check api with error: ", response.json())
                break
    ######################################### INSERT DATA ################################################

    def insert_data_don_hang(data: list) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
                    INSERT INTO [dbo].[3rd_misa_crm_san_pham_api]
                            ([id]
                            ,[owner_name]
                            ,[product_code]
                            ,[product_category]
                            ,[usage_unit]
                            ,[product_name]
                            ,[inactive]
                            ,[unit_price]
                            ,[tax]
                            ,[description]
                            ,[created_date]
                            ,[created_by]
                            ,[modified_date]
                            ,[modified_by]
                            ,[tag]
                            ,[purchased_price]
                            ,[price_after_tax]
                            ,[is_public]
                            ,[form_layout]
                            ,[organization_unit_name]
                            ,[is_follow_serial_number]
                            ,[unit_price1]
                            ,[unit_price2]
                            ,[unit_price_fixed]
                            ,[is_use_tax]
                            ,[unit_cost]
                            ,[sale_description]
                            ,[quantity_formula]
                            ,[product_properties]
                            ,[default_stock]
                            ,[warranty_period]
                            ,[source]
                            ,[is_set_product]
                            ,[warranty_description]
                            ,[avatar]
                            ,[dtm_creation_date])
                        VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, getdate())
                """
        sql_select = 'select distinct product_code from [3rd_misa_crm_san_pham_api]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        cols = [
            'id',
            'owner_name',
            'product_code',
            'product_category',
            'usage_unit',
            'product_name',
            'inactive',
            'unit_price',
            'tax',
            'description',
            'created_date',
            'created_by',
            'modified_date',
            'modified_by',
            'tag',
            'purchased_price',
            'price_after_tax',
            'is_public',
            'form_layout',
            'organization_unit_name',
            'is_follow_serial_number',
            'unit_price1',
            'unit_price2',
            'unit_price_fixed',
            'is_use_tax',
            'unit_cost',
            'sale_description',
            'quantity_formula',
            'product_properties',
            'default_stock',
            'warranty_period',
            'source',
            'is_set_product',
            'warranty_description',
            'avatar'
        ]
        df = df[cols]
        df = df[~df['product_code'].isin(df_sql['product_code'])]
        df.loc[(df['purchased_price'].isnull()) | (
            df['purchased_price'] == None), 'purchased_price'] = 0.000
        if not df.empty:
            values = [tuple(str(row[col]) if row[col] is not None and pd.notna(row[col]) else 'NULL' for col in cols)
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
    call_api_get_san_pham()


dag = Misa_CRM_SanPham_API()
