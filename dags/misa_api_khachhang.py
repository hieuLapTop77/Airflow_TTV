import json

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.operators.python import task
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
    tags=["Misa", "khach hang", "customer"],
    max_active_runs=1,
)
def Misa_API_Khachhang():
    ######################################### API ################################################
    @task
    def call_api_get_khach_hang():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{MISA_TOKEN}"
        }
        body = {
            "data_type": 1,
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_del = "truncate table [3rd_misa_api_account_objects]"
        cursor.execute(sql_del)
        sql_conn.commit()
        while True:
            response = requests.post(
                MISA_API_DANHMUC, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_khach_hang(data)
                body["skip"] += 1000
            else:
                print("Error please check api with error: ", response.json())
                break
    ######################################### INSERT DATA ################################################

    def insert_khach_hang(data: list) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
            INSERT INTO [dbo].[3rd_misa_api_account_objects](
                [dictionary_type],[account_object_id],[account_object_type],[is_vendor],[is_local_object],[is_customer],
                [is_employee],[inactive],[maximize_debt_amount],[receiptable_debt_amount],[account_object_code],[account_object_name],
                [address],[district],[ward_or_commune],[country],[province_or_city],[company_tax_code],[is_same_address],[closing_amount],[reftype],
                [reftype_category],[branch_id],[is_convert],[is_group],[is_sync_corp],[is_remind_debt],[isUpdateRebundant],[list_object_type],
                [database_id],[isCustomPrimaryKey],[excel_row_index],[is_valid],[created_date],[created_by],[modified_date],[modified_by],
                [auto_refno],[pass_edit_version],[state],[gender],[due_time],[agreement_salary],[salary_coefficient],[insurance_salary],
                [employee_contract_type],[contact_address],[contact_title],[email_address],[fax],[employee_id],[number_of_dependent],
                [account_object_group_id_list],[account_object_group_code_list],[account_object_group_name_list],[account_object_group_misa_code_list],
                [employee_tax_code],[einvoice_contact_name],[einvoice_contact_mobile],[contact_name],[contact_mobile],[tel],[description],
                [website],[mobile],[contact_fixed_tel],[bank_branch_name],[other_contact_mobile],[pay_account],[shipping_address],[receive_account],
                [organization_unit_name],[bank_account],[bank_name],[prefix],[account_object_shipping_address],[bank_province_or_city],
                [contact_email],[legal_representative],[organization_unit_id],[account_object_bank_account],[payment_term_id],[einvoice_contact_email],
                [issue_date],[issue_by],[identification_number],[amis_platform_id],[birth_date],[crm_id],[crm_group_id],[dtm_creation_date])
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
        """

        df = pd.DataFrame(data)
        list_col = ['contact_fixed_tel', 'bank_branch_name', 'other_contact_mobile', 'pay_account',
                    'shipping_address', 'receive_account', 'organization_unit_name', 'bank_account', 'bank_name', 'prefix',
                    'account_object_shipping_address', 'bank_province_or_city', 'contact_email', 'legal_representative',
                    'organization_unit_id', 'account_object_bank_account', 'payment_term_id', 'einvoice_contact_email', 'issue_date',
                    'issue_by', 'identification_number', 'amis_platform_id', 'birth_date', 'crm_id', 'crm_group_id']

        for i in list_col:
            if i not in df.columns.tolist():
                df[f'{i}'] = None

        cols = ['dictionary_type', 'account_object_id', 'account_object_type', 'is_vendor',
                'is_local_object', 'is_customer', 'is_employee', 'inactive', 'maximize_debt_amount',
                'receiptable_debt_amount', 'account_object_code', 'account_object_name', 'address', 'district',
                'ward_or_commune', 'country', 'province_or_city', 'company_tax_code', 'is_same_address', 'closing_amount',
                'reftype', 'reftype_category', 'branch_id', 'is_convert', 'is_group', 'is_sync_corp', 'is_remind_debt',
                'isUpdateRebundant', 'list_object_type', 'database_id', 'isCustomPrimaryKey', 'excel_row_index', 'is_valid',
                'created_date', 'created_by', 'modified_date', 'modified_by', 'auto_refno', 'pass_edit_version', 'state', 'gender',
                'due_time', 'agreement_salary', 'salary_coefficient', 'insurance_salary', 'employee_contract_type', 'contact_address',
                'contact_title', 'email_address', 'fax', 'employee_id', 'number_of_dependent', 'account_object_group_id_list',
                'account_object_group_code_list', 'account_object_group_name_list', 'account_object_group_misa_code_list',
                'employee_tax_code', 'einvoice_contact_name', 'einvoice_contact_mobile', 'contact_name', 'contact_mobile',
                'tel', 'description', 'website', 'mobile', 'contact_fixed_tel', 'bank_branch_name', 'other_contact_mobile',
                'pay_account', 'shipping_address', 'receive_account', 'organization_unit_name', 'bank_account', 'bank_name',
                'prefix', 'account_object_shipping_address', 'bank_province_or_city', 'contact_email', 'legal_representative',
                'organization_unit_id', 'account_object_bank_account', 'payment_term_id', 'einvoice_contact_email', 'issue_date',
                'issue_by', 'identification_number', 'amis_platform_id', 'birth_date', 'crm_id', 'crm_group_id']
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
    call_api_get_khach_hang()


dag = Misa_API_Khachhang()
