import concurrent.futures
import copy
import json
import math
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.helper import call_query_sql
from common.hook import hook
from common.variables import API_TOKEN, CLICKUP_CREATE_TASK

# Variables
LIST_ID_TASK_DESTINATION = 901803709629

CLICKUP_STATUS = "cần xử lý"

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}
BODY_TEMPLATE = {
    "name": "",
    "text_content": "",
    "description": "",
    "status": None,
    "date_created": "",
    "date_updated": None,
    "date_closed": None,
    "date_done": None,
    "archived": False,
    "assignees": [],
    "group_assignees": [],
    "checklists": [],
    "tags": [],
    "parent": None,
    "priority": None,
    "due_date": None,
    "start_date": None,
    "points": None,
    "time_estimate": None,
    "time_spent": 0,
    "custom_fields": [
        {
            "id": "ab325cd5-b5ad-4088-a51f-f4557911c1aa",
            "name": "Billing address"
        },
        {
            "id": "b1669a36-1781-4a4d-bbbb-195ee904a19d",
            "name": "Billing contact"
        },
        {
            "id": "b71e8c91-bd4f-490c-a20c-161890c40630",
            "name": "Booking date"
        },
        {
            "id": "8ab977da-44bf-44c1-b306-e19151675dd7",
            "name": "Contact name"
        },
        {
            "id": "bab5a15a-9298-46cb-9566-ecfa8f374145",
            "name": "Deadline date"
        },
        {
            "id": "e530feab-c5e1-4f88-8012-488212ee5764",
            "name": "Description"
        },
        {
            "id": "10a0c720-b367-47f0-93f6-79e7779ac78f",
            "name": "Due date"
        },
        {
            "id": "c8ce64b4-057c-4f11-8bbd-10e048458cd0",
            "name": "Mã đơn hàng"
        },
        {
            "id": "8d551da4-6f8c-4417-acb5-7c7db129a155",
            "name": "Organization unit name"
        },
        {
            "id": "da8d1c2c-b2be-4cf0-a69e-c72845b10041",
            "name": "Owner name"
        },
        {
            "id": "ac9c7a72-a642-4f2a-8acd-dd1b8009d3fa",
            "name": "Phone"
        },
        {
            "id": "c8f99f40-da7b-4b82-834d-cb0297156d46",
            "name": "Revenue status"
        },
        {
            "id": "a62f097c-3266-40c5-817a-48007252a64d",
            "name": "Sale order amount"
        },
        {
            "id": "930afd0a-feb9-4647-88a0-36457e1534ed",
            "name": "Sale order date"
        },
        {
            "id": "c53f3141-47d9-4ff1-bdd9-3cdf32762e5c",
            "name": "Shipping address"
        },
        {
            "id": "cc9e116e-e503-4e76-9144-1a3293d6162b",
            "name": "Shipping contact name"
        },
        {
            "id": "432b2d14-0176-4a65-a37c-1373cdf92991",
            "name": "Sản phẩm cần ship"
        },
        {
            "id": "71b3f818-0a62-4e34-810c-4f8e225a6b18",
            "name": "Số lượng"
        },
        {
            "id": "53c07947-5b56-4ef0-b89e-90b3fd55e91a",
            "name": "Trạng thái giao hàng"
        },
        {
            "id": "5251762d-ac06-4800-911b-749368f4e479",
            "name": "Trạng thái thanh toán"
        },
        {
            "id": "07479210-e162-415d-8934-4de0bc3a4c38",
            "name": "Trạng thái đơn hàng"
        },
        {
            "id": "6d727c33-3020-4918-8e92-e1b7fcec7376",
            "name": "Tên đơn hàng"
        },
        {
            "id": "a0643663-e0a3-4b47-9949-ae43846725a3",
            "name": "sale_order_product_mappings"
        }
    ],
    "attachments": [],
}


@dag(
    default_args=default_args,
    schedule_interval="5 */3 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup", "Đơn hàng", "CRM"],
    max_active_runs=1,
)
def Clickup_post_don_hang_crm():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }

    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['sale_order_no']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = CLICKUP_STATUS
        body["description"] = df_row["list_info_product"]
        for field in body["custom_fields"]:
            if field["id"] == "ab325cd5-b5ad-4088-a51f-f4557911c1aa":  # "Billing address"
                field["value"] = df_row['billing_address'] if df_row['billing_address'] else None
            elif field["id"] == "b1669a36-1781-4a4d-bbbb-195ee904a19d":  # "Billing contact"
                field["value"] = df_row['billing_contact'] if df_row['billing_contact'] else None
            elif field["id"] == "b71e8c91-bd4f-490c-a20c-161890c40630":  # "Booking date"
                field["value"] = df_row['book_date'] if df_row['book_date'] else None
            elif field["id"] == "8ab977da-44bf-44c1-b306-e19151675dd7":  # "Contact name"
                field["value"] = df_row['contact_name'] if df_row['contact_name'] else None
            elif field["id"] == "bab5a15a-9298-46cb-9566-ecfa8f374145":  # "Deadline date"
                field["value"] = df_row['deadline_date'] if df_row['deadline_date'] else "NULL"
            elif field["id"] == "e530feab-c5e1-4f88-8012-488212ee5764":  # "Description"
                field["value"] = df_row['description'] if df_row['description'] else None
            elif field["id"] == "10a0c720-b367-47f0-93f6-79e7779ac78f":  # "Due date"
                field["value"] = df_row['due_date'] if df_row['due_date'] else None
            # elif field["id"] == "c8ce64b4-057c-4f11-8bbd-10e048458cd0":  # "Mã đơn hàng"
            #     field["value"] = df_row['created_by'] if df_row['created_by'] else None
            elif field["id"] == "8d551da4-6f8c-4417-acb5-7c7db129a155":  # "Organization unit name"
                field["value"] = df_row['organization_unit_name'] if df_row['organization_unit_name'] else None
            elif field["id"] == "da8d1c2c-b2be-4cf0-a69e-c72845b10041":  # "Owner name"
                field["value"] = df_row['owner_name'] if df_row['owner_name'] else None
            elif field["id"] == "ac9c7a72-a642-4f2a-8acd-dd1b8009d3fa":  # "Phone"
                field["value"] = df_row['phone'] if df_row['phone'] else None
            elif field["id"] == "c8f99f40-da7b-4b82-834d-cb0297156d46":  # "Revenue status"
                field["value"] = df_row['revenue_status'] if df_row['revenue_status'] else None
            elif field["id"] == "a62f097c-3266-40c5-817a-48007252a64d":  # "Sale order amount"
                field["value"] = df_row['sale_order_amount'] if df_row['sale_order_amount'] else None
            elif field["id"] == "930afd0a-feb9-4647-88a0-36457e1534ed":  # "Sale order date"
                field["value"] = df_row['sale_order_date'] if df_row['sale_order_date'] else None
            elif field["id"] == "c53f3141-47d9-4ff1-bdd9-3cdf32762e5c":  # "Shipping address"
                field["value"] = df_row['shipping_address'] if df_row['shipping_address'] else None
            elif field["id"] == "cc9e116e-e503-4e76-9144-1a3293d6162b":  # "Shipping contact name"
                field["value"] = df_row['shipping_contact_name'] if df_row['shipping_contact_name'] else None
            # elif field["id"] == "432b2d14-0176-4a65-a37c-1373cdf92991":  # "Sản phẩm cần ship"
            #     field["value"] = df_row['created_by'] if df_row['created_by'] else None
            elif field["id"] == "53c07947-5b56-4ef0-b89e-90b3fd55e91a":  # "Trạng thái giao hàng"
                field["value"] = df_row['delivery_status'] if df_row['delivery_status'] else None
            elif field["id"] == "5251762d-ac06-4800-911b-749368f4e479":  # "Trạng thái thanh toán"
                field["value"] = 1 if df_row['pay_status'] == "Đã thanh toán" else 0
            elif field["id"] == "07479210-e162-415d-8934-4de0bc3a4c38":  # "Trạng thái đơn hàng"
                num = None
                if df_row['status'] == "Đang thực hiện":
                    num = 1
                elif df_row['status'] == "Đã thực hiện":
                    num = 2
                else:
                    num = 0
                field["value"] = num
            elif field["id"] == "6d727c33-3020-4918-8e92-e1b7fcec7376":  # "Tên đơn hàng"
                field["value"] = df_row['sale_order_name'] if df_row['sale_order_name'] else None

            # "sale_order_product_mappings"
            # elif field["id"] == "a0643663-e0a3-4b47-9949-ae43846725a3":
            #     field["value"] = df_row['list_info_product'] if df_row['list_info_product'] else None
        return json.loads(json.dumps(body, ensure_ascii=False))

    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        for field in main_task_payload['custom_fields']:
            if (field['name'] == 'Deadline date' or field['name'] == 'Due date') and (pd.isna(field['value']) or math.isnan(field['value'])):
                field['value'] = None
        print("Body json: ", main_task_payload)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            LIST_ID_TASK_DESTINATION), json=main_task_payload, headers=headers, timeout=None)
        print(res.status_code)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created task: {task_id}")
            id_don_hang = df_row['sale_order_no']
            # Update status clickup
            query = f"""
                INSERT INTO [dbo].[log_clickup_post_don_hang_crm]
                    ([Ma_don_hang]
                    ,[dtm_creation_date])
                VALUES('{id_don_hang}',getdate())
            """
            call_query_sql(query=query)
            print(
                f"UPDATE: Update status clickup với mã đơn hàng: {id_don_hang} successfully")
        else:
            print("create task fail: ", res.status_code, "Error: ", res.json())

    @task
    def create_don_hang_clickup():
        sql_conn = hook.get_conn()
        sql = """
            select * from vw_Clickup_Post_Don_Hang_CRM where [sale_order_no] not in
            (select Ma_don_hang from [dbo].[log_clickup_post_don_hang_crm]);
        """
        print("Connection established")
        df = pd.read_sql(sql, sql_conn)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])
        sql_conn.close()

    ############ DAG FLOW ############
    create_don_hang_clickup()


dag = Clickup_post_don_hang_crm()
