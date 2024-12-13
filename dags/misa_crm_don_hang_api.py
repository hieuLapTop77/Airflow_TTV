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
    MISA_CRM_DON_HANG_API,
    MISA_CRM_GET_TOKEN,
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
def Misa_CRM_DonHang_API():
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
    def call_api_get_don_hang():
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
                MISA_CRM_DON_HANG_API, json=body, params=params, headers=headers, timeout=None)
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
                    INSERT INTO [dbo].[3rd_misa_crm_don_hang_api]
                            ([id]
                            ,[sale_order_no]
                            ,[campaign_name]
                            ,[book_date]
                            ,[sale_order_amount]
                            ,[deadline_date]
                            ,[quote_name]
                            ,[status]
                            ,[exchange_rate]
                            ,[sale_order_name]
                            ,[sale_order_date]
                            ,[account_name]
                            ,[contact_name]
                            ,[due_date]
                            ,[delivery_status]
                            ,[sale_order_type]
                            ,[opportunity_name]
                            ,[revenue_status]
                            ,[currency_type]
                            ,[description]
                            ,[balance_receipt_amount]
                            ,[pay_status]
                            ,[invoiced_amount]
                            ,[total_receipted_amount]
                            ,[is_invoiced]
                            ,[un_invoiced_amount]
                            ,[paid_date]
                            ,[delivery_date]
                            ,[un_subcrible]
                            ,[billing_account]
                            ,[billing_country]
                            ,[billing_district]
                            ,[billing_street]
                            ,[billing_address]
                            ,[billing_contact]
                            ,[billing_province]
                            ,[billing_ward]
                            ,[billing_code]
                            ,[shipping_country]
                            ,[shipping_district]
                            ,[shipping_street]
                            ,[shipping_address]
                            ,[phone]
                            ,[shipping_province]
                            ,[shipping_ward]
                            ,[shipping_code]
                            ,[organization_unit_name]
                            ,[created_by]
                            ,[modified_by]
                            ,[form_layout]
                            ,[owner_name]
                            ,[created_date]
                            ,[modified_date]
                            ,[is_public]
                            ,[tag]
                            ,[total_summary]
                            ,[tax_summary]
                            ,[discount_summary]
                            ,[to_currency_summary]
                            ,[shipping_amount_summary]
                            ,[sale_order_process_cost]
                            ,[recorded_sale_organization_unit_name]
                            ,[recorded_sale_users_name]
                            ,[parent_name]
                            ,[is_parent_sale_order]
                            ,[liquidate_amount]
                            ,[invoice_date]
                            ,[to_currency_summary_oc]
                            ,[discount_summary_oc]
                            ,[tax_summary_oc]
                            ,[total_summary_oc]
                            ,[liquidate_amount_oc]
                            ,[sale_order_amount_oc]
                            ,[total_receipted_amount_oc]
                            ,[balance_receipt_amount_oc]
                            ,[invoiced_amount_oc]
                            ,[is_use_currency]
                            ,[discount_overall]
                            ,[discount_overall_oc]
                            ,[tax_overall]
                            ,[tax_overall_oc]
                            ,[total_overall]
                            ,[total_overall_oc]
                            ,[discount_percent_overall]
                            ,[tax_percent_overall]
                            ,[to_currency_after_discount_summary]
                            ,[to_currency_oc_after_discount_summary]
                            ,[shipping_contact_name]
                            ,[is_correct_route]
                            ,[is_visited]
                            ,[amount_summary]
                            ,[usage_unit_amount_summary]
                            ,[number_of_days_owed]
                            ,[list_product_category]
                            ,[list_product]
                            ,[approved_status]
                            ,[approver]
                            ,[approved_date]
                            ,[other_sys_order_code]
                            ,[is_sync_price_after_discount]
                            ,[related_users]
                            ,[ticket]
                            ,[promotion_applied]
                            ,[sale_order_product_mappings]
                            ,[dtm_creation_date])
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, getdate())
                """
        sql_select = 'select distinct sale_order_no from [3rd_misa_crm_don_hang_api]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        cols = ['id',
                'sale_order_no', 'campaign_name', 'book_date', 'sale_order_amount', 'deadline_date', 'quote_name', 'status', 'exchange_rate', 'sale_order_name', 'sale_order_date',
                'account_name', 'contact_name', 'due_date', 'delivery_status', 'sale_order_type', 'opportunity_name', 'revenue_status', 'currency_type', 'description', 'balance_receipt_amount', 'pay_status',
                'invoiced_amount', 'total_receipted_amount', 'is_invoiced', 'un_invoiced_amount',
                'paid_date', 'delivery_date', 'un_subcrible', 'billing_account', 'billing_country', 'billing_district', 'billing_street', 'billing_address', 'billing_contact', 'billing_province', 'billing_ward',
                'billing_code', 'shipping_country', 'shipping_district', 'shipping_street', 'shipping_address', 'phone', 'shipping_province', 'shipping_ward', 'shipping_code', 'organization_unit_name',
                'created_by', 'modified_by', 'form_layout', 'owner_name', 'created_date', 'modified_date', 'is_public', 'tag', 'total_summary', 'tax_summary', 'discount_summary', 'to_currency_summary',
                'shipping_amount_summary', 'sale_order_process_cost', 'recorded_sale_organization_unit_name', 'recorded_sale_users_name', 'parent_name', 'is_parent_sale_order', 'liquidate_amount',
                'invoice_date', 'to_currency_summary_oc', 'discount_summary_oc', 'tax_summary_oc', 'total_summary_oc', 'liquidate_amount_oc', 'sale_order_amount_oc', 'total_receipted_amount_oc',
                'balance_receipt_amount_oc', 'invoiced_amount_oc', 'is_use_currency', 'discount_overall', 'discount_overall_oc', 'tax_overall', 'tax_overall_oc', 'total_overall', 'total_overall_oc', 'discount_percent_overall',
                'tax_percent_overall', 'to_currency_after_discount_summary', 'to_currency_oc_after_discount_summary', 'shipping_contact_name', 'is_correct_route', 'is_visited', 'amount_summary',
                'usage_unit_amount_summary', 'number_of_days_owed', 'list_product_category', 'list_product', 'approved_status', 'approver', 'approved_date', 'other_sys_order_code', 'is_sync_price_after_discount',
                'related_users', 'ticket', 'promotion_applied', 'sale_order_product_mappings'
                ]
        df = df[cols]
        df = df[~df['sale_order_no'].isin(df_sql['sale_order_no'])]
        if not df.empty:
            json_columns = ["sale_order_product_mappings"]
            for column in json_columns:
                if column in df.columns:
                    df[column] = df[column].apply(
                        lambda x: json.dumps(x) if x is not None else 'NULL')
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
    call_api_get_don_hang()


dag = Misa_CRM_DonHang_API()
