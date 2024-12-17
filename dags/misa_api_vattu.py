import json

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from common.hook import hook
from common.variables import MISA_API_DANHMUC, MISA_APP_ID, MISA_TOKEN

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
    tags=["Misa", "vật tư", "hàng hóa"],
    max_active_runs=1,
)
def Misa_API_VatTu():
    ######################################### API ################################################
    @task
    def get_data_misa():
        body = {
            "data_type": 2,
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{MISA_TOKEN}"
        }
        while True:
            response = requests.post(
                MISA_API_DANHMUC, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_vattu(data)
                body["skip"] += 1000
            else:
                print("Error please check api with error: ", response.json())
                break

    ######################################### INSERT DATA ################################################
    def insert_vattu(data: list) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
                    INSERT INTO [dbo].[3rd_misa_api_vattu_hanghoa](
                        [dictionary_type],[inventory_item_id],[inventory_item_name],[inventory_item_code],[inventory_item_type],[unit_id],
                        [minimum_stock],[inventory_item_category_code_list],[inventory_item_category_name_list],[inventory_item_category_id_list],
                        [inventory_item_category_misa_code_list],[branch_id],[default_stock_id],[discount_type],[cost_method],[inventory_item_cost_method],
                        [base_on_formula],[is_unit_price_after_tax],[is_system],[inactive],[is_follow_serial_number],[is_allow_duplicate_serial_number],
                        [purchase_discount_rate],[unit_price],[sale_price1],[sale_price2],[sale_price3],[fixed_sale_price],[import_tax_rate],[export_tax_rate],
                        [fixed_unit_price],[description],[specificity],[purchase_description],[sale_description],[inventory_account],[cogs_account],
                        [sale_account],[unit_list],[unit_name],[is_temp_from_sync],[reftype],[reftype_category],[purchase_fixed_unit_price_list],
                        [purchase_last_unit_price_list],[quantityBarCode],[allocation_type],[allocation_time],[allocation_account],[tax_reduction_type],
                        [purchase_last_unit_price],[is_specific_inventory_item],[has_delete_fixed_unit_price],[has_delete_unit_price],[has_delete_discount],
                        [has_delete_unit_convert],[has_delete_norm],[has_delete_serial_type],[is_edit_multiple],[is_not_sync_crm],[isUpdateRebundant],
                        [is_special_inv],[isCustomPrimaryKey],[isFromProcessBalance],[is_drug],[status_sync_medicine_national],[is_sync_corp],[convert_rate],
                        [is_update_main_unit],[is_image_duplicate],[is_group],[discount_value],[is_set_discount],[index_unit_convert],[excel_row_index],
                        [is_valid],[created_date],[created_by],[modified_date],[modified_by],[auto_refno],[pass_edit_version],[state],[discount_account],
                        [sale_off_account],[return_account],[tax_rate],[guaranty_period],[inventory_item_source],[image],[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , getdate())
                """
        sql_select = 'select distinct inventory_item_id from [3rd_misa_api_vattu_hanghoa]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        cols = ['dictionary_type', 'inventory_item_id', 'inventory_item_name', 'inventory_item_code', 'inventory_item_type',
                'unit_id', 'minimum_stock', 'inventory_item_category_code_list', 'inventory_item_category_name_list',
                'inventory_item_category_id_list', 'inventory_item_category_misa_code_list', 'branch_id', 'default_stock_id',
                'discount_type', 'cost_method', 'inventory_item_cost_method', 'base_on_formula', 'is_unit_price_after_tax', 'is_system',
                'inactive', 'is_follow_serial_number', 'is_allow_duplicate_serial_number', 'purchase_discount_rate', 'unit_price',
                'sale_price1', 'sale_price2', 'sale_price3', 'fixed_sale_price', 'import_tax_rate', 'export_tax_rate', 'fixed_unit_price',
                'description', 'specificity', 'purchase_description', 'sale_description', 'inventory_account', 'cogs_account', 'sale_account',
                'unit_list', 'unit_name', 'is_temp_from_sync', 'reftype', 'reftype_category', 'purchase_fixed_unit_price_list',
                'purchase_last_unit_price_list', 'quantityBarCode', 'allocation_type', 'allocation_time', 'allocation_account',
                'tax_reduction_type', 'purchase_last_unit_price', 'is_specific_inventory_item', 'has_delete_fixed_unit_price',
                'has_delete_unit_price', 'has_delete_discount', 'has_delete_unit_convert', 'has_delete_norm', 'has_delete_serial_type',
                'is_edit_multiple', 'is_not_sync_crm', 'isUpdateRebundant', 'is_special_inv', 'isCustomPrimaryKey', 'isFromProcessBalance',
                'is_drug', 'status_sync_medicine_national', 'is_sync_corp', 'convert_rate', 'is_update_main_unit', 'is_image_duplicate', 'is_group',
                'discount_value', 'is_set_discount', 'index_unit_convert', 'excel_row_index', 'is_valid', 'created_date', 'created_by', 'modified_date',
                'modified_by', 'auto_refno', 'pass_edit_version', 'state', 'discount_account', 'sale_off_account', 'return_account', 'tax_rate', 'guaranty_period',
                'inventory_item_source', 'image'
                ]
        df = df[cols]
        df = df[~df['inventory_item_id'].isin(df_sql['inventory_item_id'])]
        # df = pd.DataFrame(data)
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
    get_data_misa()


dag = Misa_API_VatTu()
