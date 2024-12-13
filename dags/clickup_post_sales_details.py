import concurrent.futures
import copy
import json
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from common.helper import call_query_sql
from common.hook import hook
from common.variables import API_TOKEN, CLICKUP_CREATE_TASK

# Variables
ID_LIST_DEFAULT = 901802479598

HEADERS = {
    "Authorization": f"{API_TOKEN}",
    "Content-Type": "application/json",
}

BODY_TEMPLATE = {
    "name": "Đơn hàng (mã đơn hàng)",
    "text_content": "",
    "description": "",
    "status": "cần giao",
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
            "id": "ea6dc2f1-f35e-48ef-802b-f99d6fa9c546",
            "name": "Chiết khấu",
            "value": None
        },
        {
            "id": "9e2c4b36-cd4e-442e-a82c-ebd35e74a6a6",
            "name": "Giá trị trả lại",
            "value": None
        },
        {
            "id": "5c52dc4f-a8d9-4b5f-bbb2-3eb7e9fa36a1",
            "name": "Mã hàng",
            "value": None
        },
        {
            "id": "438c5839-7a80-4e92-9cda-61652672be30",
            "name": "Nhân Viên Kinh Doanh",
            "value": None
        },
        {
            "id": "b7332e3b-52e4-40b4-8841-d73efa2d22d2",
            "name": "Khách hàng misa kế toán",
            "value": None
        },
        {
            "id": "cd3ec20a-9461-4543-bb05-c97854932714",
            "name": "Số lượng",
            "value": None
        },
        {
            "id": "3bcd3129-efd6-41b6-b1bc-30cafcaf1c59",
            "name": "TT Thanh toán",
            "value": None
        },
        {
            "id": "bbbbc74f-57d2-4f2a-a2a4-5a00afb6d427",
            "name": "Tên kho",
            "value": None
        },
        {
            "id": "24dd568f-bf21-4166-8be0-32effc22dc4c",
            "name": "Tên kênh phân phối",
            "value": None
        },
        {
            "id": "4ca5bb89-97dc-426a-8a23-d85017576d06",
            "name": "Tổng số lượng trả lại",
            "value": None
        },
        {
            "id": "1dd93b23-700a-41b1-88f8-bbb027c311df",
            "name": "Tổng tiền thanh toán",
            "value": None
        },
        {
            "id": "40c4e8d2-7ea2-4d3a-9ecf-6ed491ea19c8",
            "name": "Đơn vị phụ trách",
            "value": None
        },
        {
            "id": "890ff1aa-40c1-4a58-ab3d-4ccf240fa77c",
            "name": "Diễn giải",
            "value": None
        },
        {
            "id": "eb20eefa-8bf0-40f4-adf1-37a11bac25cd",
            "name": "Loại dữ liệu",
            "value": None
        },
        {
            "id": "b60989ab-2d22-433d-a5e5-8b7d3491f2ea",
            "name": "Ngày hạch toán",
            "value": None
        },
        {
            "id": "15deedd8-4c19-4696-92cc-3db3c2713eb2",
            "name": "Số chứng từ",
            "value": None
        },
        {
            "id": "0da482d7-5b1c-4851-a8b3-d0b0c4ef7786",
            "name": "Tên khách hàng",
            "value": None
        },
        {
            "id": "26866878-a950-4210-a069-66bd95b24530",
            "name": "Tổng thanh toán NT",
            "value": None
        },
        {
            "id": "776b2498-e134-4c3f-aa56-95ffa4ab2e73",
            "name": "Địa điểm chính xác",
            "value": None
        }
    ],
    "attachments": [],
}

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="30 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "clickup", 'Bán hàng'],
    max_active_runs=1,
)
def Clickup_post_sales_details():
    ######################################### API ################################################
    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['So_chung_tu']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        for field in body["custom_fields"]:

            if field["id"] == "b60989ab-2d22-433d-a5e5-8b7d3491f2ea":  # "Ngày hạch toán"
                field["value"] = df_row['Ngay_hach_toan'] if df_row['Ngay_hach_toan'] != 0 else None

            elif field["id"] == "15deedd8-4c19-4696-92cc-3db3c2713eb2":  # "Số chứng từ"
                field["value"] = df_row['So_chung_tu'] if df_row['So_chung_tu'] is not None else None

            elif field["id"] == "26866878-a950-4210-a069-66bd95b24530":  # "Tổng thanh toán NT"
                field["value"] = df_row['Tong_thanh_toan_NT'] if df_row['Tong_thanh_toan_NT'] is not None else None

            elif field["id"] == "890ff1aa-40c1-4a58-ab3d-4ccf240fa77c":  # "Diễn giải"
                field["value"] = df_row['Dien_giai_chung'] if df_row['Dien_giai_chung'] is not None else None

            elif field["id"] == "eb20eefa-8bf0-40f4-adf1-37a11bac25cd":
                field["value"] = 0

            elif field["id"] == "0da482d7-5b1c-4851-a8b3-d0b0c4ef7786":
                field["value"] = df_row['Ten_khach_hang']

            elif field["id"] == "776b2498-e134-4c3f-aa56-95ffa4ab2e73":  # "Địa điểm chính xác"
                field["value"] = df_row['Dia_chi']
        return json.loads(json.dumps(body, ensure_ascii=False))

    def create_chitietbanhang_clickup_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            ID_LIST_DEFAULT), json=main_task_payload, headers=HEADERS, timeout=None)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            so_chung_tu = df_row['So_chung_tu']
            query = f"""
                INSERT INTO [dbo].[log_clickup_sales_details_ban_hang]
                    ([So_chung_tu]
                    ,[dtm_creation_date])
                VALUES('{so_chung_tu}',getdate())
            """
            call_query_sql(query=query)
        else:
            raise RuntimeError(
                f"Task creation failed with status code {res.status_code}: {res.text}")

    @task
    def create_order_clickup():
        sql_conn = hook.get_conn()
        sql = """
            select  a.Ngay_hach_toan
                    ,a.So_chung_tu
                    ,a.Ten_khach_hang 
                    ,a.Dia_chi
                    ,a.Dien_giai_chung
                    ,a.Tong_thanh_toan_NT
            from vw_Clickup_sales_details_ban_hang a
            where cast(So_chung_tu as varchar) not in 
                (
                    select cast(So_chung_tu as varchar) 
                    from [dbo].[log_clickup_sales_details_ban_hang])
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_chitietbanhang_clickup_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    create_order_clickup()


dag = Clickup_post_sales_details()
