import concurrent.futures
import copy
import json
from datetime import datetime, timedelta
from typing import Dict, Union

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

from common.helper import (
    call_api_get_list,
    call_api_mutiple_pages,
    call_multiple_thread,
    handle_df,
)

# Variables
TEMP_PATH = Variable.get("temp_path")
CLICKUP_DELETE_TASK = Variable.get("clickup_delete_task")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")
HOOK_MSSQL = Variable.get("mssql_connection")
API_TOKEN = Variable.get("api_token")
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
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
            "id": "9abf18d5-d6df-47ca-83df-3ab88f29f9e6",
            "name": "Khách hàng",
        },
        {
            "id": "5c52dc4f-a8d9-4b5f-bbb2-3eb7e9fa36a1",
            "name": "Mã hàng",
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
            "id": "cd3ec20a-9461-4543-bb05-c97854932714",
            "name": "Số lượng",
            "value": None
        },
        # {
        #     "id": "3625c536-9ff5-4aec-8972-0550d92b5315",
        #     "name": "TT Lập hóa đơn",
        #     "value": None
        # },
        # {
        #     "id": "3bcd3129-efd6-41b6-b1bc-30cafcaf1c59",
        #     "name": "TT Thanh toán",
        #     "value": None
        # },
        # {
        #     "id": "00be66a6-a6f7-464d-9ab6-513f9abfb2f4",
        #     "name": "TT Xuất hàng",
        #     "value": None
        # },
        {
            "id": "16e8ce7f-6ad3-4013-9462-22b8aaf5e70e",
            "name": "Tên nhân viên bán hàng",
            "value": None
        },
        {
            "id": "26866878-a950-4210-a069-66bd95b24530",
            "name": "Tổng thanh toán NT",
            "value": None
        },
        {
            "id": "1dd93b23-700a-41b1-88f8-bbb027c311df",
            "name": "Tổng tiền thanh toán",
            "value": None
        },
        {
            "id": "890ff1aa-40c1-4a58-ab3d-4ccf240fa77c",
            "name": "Diễn giải",
            "value": None
        },
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
            "id": "eb20eefa-8bf0-40f4-adf1-37a11bac25cd",
            "name": "Loại dữ liệu",
            "value": None
        },
        {
            "id": "4ca5bb89-97dc-426a-8a23-d85017576d06",
            "name": "Tổng số lượng trả lại",
            "value": None
        },
        {
            "id": "438c5839-7a80-4e92-9cda-61652672be30",
            "name": "Nhân Viên Kinh Doanh",
            "value": None
        },
        {
            "id": "24dd568f-bf21-4166-8be0-32effc22dc4c",
            "name": "Tên kênh phân phối",
            "value": None
        },
        {
            "id": "bbbbc74f-57d2-4f2a-a2a4-5a00afb6d427",
            "name": "Tên sản phẩm",
            "value": None
        },
        {
            "id": "40c4e8d2-7ea2-4d3a-9ecf-6ed491ea19c8",
            "name": "Đơn vị phụ trách",
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
    schedule_interval="34 */4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "clickup", 'ban hang'],
    max_active_runs=1,
)
def Banhang_Clickup():
    ######################################### API ################################################
    def call_api_delete(task_id) -> None:
        response = requests.delete(CLICKUP_DELETE_TASK.format(
            task_id), timeout=None, headers=HEADERS)
        print(CLICKUP_DELETE_TASK.format(task_id))
        if response.status_code in [200, 204]:
            print('Delete successful task: ', task_id)
        else:
            print("Error please check api")

    def init_date() -> Dict[str, str]:
        current_time = datetime.now()
        date_to = int(current_time.timestamp()*1000)
        time_minus_24_hours = current_time - timedelta(hours=24)
        date_from = int(time_minus_24_hours.timestamp() * 1000)
        return {"date_from": date_from, "date_to": date_to}

    def call_api_get_tasks(list_id):
        # date = init_date()
        params = {
            "page": 0,
            # "date_created_gt": date["date_from"],
            # "date_created_lt": date["date_to"],
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
        }
        name_url = 'CLICKUP_GET_TASKS'
        print(list_id)
        return call_api_mutiple_pages(headers=HEADERS, params=params, name_url=name_url, url=CLICKUP_GET_TASKS, task_id=list_id)

    def call_mutiple_thread_tasks():
        list_tasks: list = call_api_get_tasks(901802479598)
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(
            item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [item for sublist in filtered_data for item in sublist["tasks"]]
        return pd.DataFrame(data).to_json()

    def update_misa(So_chung_tu: str):
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_update = f"""
                update [dbo].[3rd_misa_ban_hang]
                set status_clickup = 'true'
                where So_chung_tu = '{So_chung_tu}'
                """
        print(sql_update)
        cursor.execute(sql_update)
        sql_conn.commit()
        print(f"updated misa ban hang id: {So_chung_tu} successfully")
        sql_conn.close()

    def create_task_payload(df_row, is_child=False, parent_id=None):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['So_chung_tu']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        if is_child:
            body['parent'] = parent_id
            body['name'] = df_row['Ten_hang']
        for field in body["custom_fields"]:
            if field["id"] == "9abf18d5-d6df-47ca-83df-3ab88f29f9e6":  # "Khách hàng"
                field["value"] = df_row['Khach_hang'] if df_row['Khach_hang'] is not None else None

            elif field["id"] == "5c52dc4f-a8d9-4b5f-bbb2-3eb7e9fa36a1":  # "Mã hàng"
                field["value"] = df_row['Ma_Hang'] if df_row['Ma_Hang'] is not None else None

            elif field["id"] == "b60989ab-2d22-433d-a5e5-8b7d3491f2ea":  # "Ngày hạch toán"
                if df_row['Ngay_hach_toan'] is not None:
                    date_object = datetime.strptime(
                        df_row['Ngay_hach_toan'], '%Y-%m-%d %H:%M:%S')
                    timestamp_milliseconds = int(
                        date_object.timestamp() * 1000)
                    field["value"] = timestamp_milliseconds

            elif field["id"] == "15deedd8-4c19-4696-92cc-3db3c2713eb2":  # "Số chứng từ"
                field["value"] = df_row['So_chung_tu'] if df_row['So_chung_tu'] is not None else None

            elif field["id"] == "cd3ec20a-9461-4543-bb05-c97854932714":  # "Số lượng"
                field["value"] = df_row['So_Luong_Ban'] if df_row['So_Luong_Ban'] is not None else None

            # elif field["id"] == "3625c536-9ff5-4aec-8972-0550d92b5315": #"TT Lập hóa đơn"
            #     field["value"] = df_row['TT_lap_hoa_don'] if df_row['TT_lap_hoa_don'] is not None else None

            # elif field["id"] == "3bcd3129-efd6-41b6-b1bc-30cafcaf1c59": #"TT Thanh toán"
            #     field["value"] = df_row['TT_thanh_toan'] if df_row['TT_thanh_toan'] is not None else None

            # elif field["id"] == "00be66a6-a6f7-464d-9ab6-513f9abfb2f4": #"TT xuất hàng"
            #     field["value"] = df_row['TT_xuat_hang'] if df_row['TT_xuat_hang'] is not None else None

            elif field["id"] == "16e8ce7f-6ad3-4013-9462-22b8aaf5e70e":  # "Tên nhân viên bán hàng"
                field["value"] = df_row['Ten_Nhan_Vien_Ban_Hang'] if df_row['Ten_Nhan_Vien_Ban_Hang'] is not None else None

            elif field["id"] == "26866878-a950-4210-a069-66bd95b24530":  # "Tổng thanh toán NT"
                field["value"] = df_row['Tong_Thanh_Toan_NT'] if df_row['Tong_Thanh_Toan_NT'] is not None else None

            elif field["id"] == "1dd93b23-700a-41b1-88f8-bbb027c311df":  # "Tổng tiền thanh toán"
                field["value"] = df_row['Tong_tien_thanh_toan'] if df_row['Tong_tien_thanh_toan'] is not None else None

            elif field["id"] == "890ff1aa-40c1-4a58-ab3d-4ccf240fa77c":  # "Diễn giải"
                field["value"] = df_row['Dien_Giai_Chung'] if df_row['Dien_Giai_Chung'] is not None else None

            elif field["id"] == "eb20eefa-8bf0-40f4-adf1-37a11bac25cd" and not is_child:
                field["value"] = 0

            elif field["id"] == "eb20eefa-8bf0-40f4-adf1-37a11bac25cd" and is_child:
                field["value"] = 1

            elif field["id"] == "bbbbc74f-57d2-4f2a-a2a4-5a00afb6d427" and is_child:
                field["value"] = df_row['Ten_hang']

            elif field["id"] == "ea6dc2f1-f35e-48ef-802b-f99d6fa9c546" and is_child:  # "Chiết khấu"
                field["value"] = df_row['Chiet_Khau']

            elif field["id"] == "9e2c4b36-cd4e-442e-a82c-ebd35e74a6a6" and is_child:  # "Giá trị trả lại"
                field["value"] = df_row['Gia_Tri_Tra_Lai']

            # "Tổng số lượng trả lại"
            elif field["id"] == "4ca5bb89-97dc-426a-8a23-d85017576d06" and is_child:
                field["value"] = df_row['Tong_So_Luong_Tra_Lai']

            # "Nhân Viên Kinh Doanh"
            elif field["id"] == "438c5839-7a80-4e92-9cda-61652672be30" and is_child:
                field["value"] = df_row['Nhan_vien_kinh_doanh']

            # "Tên kênh phân phối"
            elif field["id"] == "24dd568f-bf21-4166-8be0-32effc22dc4c" and is_child:
                field["value"] = df_row['Ten_kenh_phan_phoi']

            elif field["id"] == "40c4e8d2-7ea2-4d3a-9ecf-6ed491ea19c8" and is_child:  # "Đơn vị phụ trách"
                field["value"] = df_row['orderindex'] if df_row['orderindex'] is not None else None

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
            update_misa(so_chung_tu)
            hook = mssql.MsSqlHook(HOOK_MSSQL)
            sql_conn = hook.get_conn()
            sql = f"""select * from Misa_data_ban_hang where So_chung_tu = '{so_chung_tu}'"""
            df = pd.read_sql(sql, sql_conn)

            for _, row in df.iterrows():
                child_task_payload = create_task_payload(
                    row, is_child=True, parent_id=task_id)
                res = requests.post(CLICKUP_CREATE_TASK.format(
                    ID_LIST_DEFAULT), json=child_task_payload, headers=HEADERS, timeout=None)
                print(res.status_code)
                if res.status_code == 200:
                    print(
                        f"Created child task for product: {row['Ten_hang']} under parent task: {task_id}")
                else:
                    print(
                        f"Failed to create child task for product: {row['Ten_hang']}")

    def extract_so_chung_tu(custom_fields):
        for custom_field in custom_fields:
            if custom_field.get('id') == '15deedd8-4c19-4696-92cc-3db3c2713eb2':
                return custom_field.get('value')
        return None

    @task
    def delete_tasks() -> None:
        df_tasks = call_mutiple_thread_tasks()
        df_tasks = handle_df(df_tasks)
        if df_tasks.empty:
            return
        df_tasks['so_chung_tu'] = df_tasks['custom_fields'].apply(
            extract_so_chung_tu)
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select So_chung_tu from [dbo].[3rd_misa_ban_hang] """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()

        task_ids_to_delete = [df_tasks['id'][i] for i in range(
            len(df_tasks)) if df_tasks['so_chung_tu'][i] in df["So_chung_tu"].tolist()]
        print(task_ids_to_delete)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(call_api_delete, task_ids_to_delete)

    @task
    def create_order_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select * from [3rd_misa_ban_hang] where status_clickup = 'false'; """
        df = pd.read_sql(sql, sql_conn)
        df["Ma_Hang"] = None
        df["Ten_hang"] = None
        df['So_Luong_Ban'] = None
        df['Ten_Nhan_Vien_Ban_Hang'] = None
        df['Tong_Thanh_Toan_NT'] = None
        df['Dien_Giai_Chung'] = None
        df['Chiet_Khau'] = None
        df['Tong_So_Luong_Tra_Lai'] = None
        df['Gia_Tri_Tra_Lai'] = None
        df['Ten_kenh_phan_phoi'] = None
        df['Don_vi_phu_trach'] = None
        df['Nhan_vien_kinh_doanh'] = None
        df['orderindex'] = None
        # df["Dia_chi"] = None
        sql_conn.close()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_chitietbanhang_clickup_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    # task_check = check_tasks_clickup()
    # delete_tasks() >> create_order_clickup()
    # create_order_clickup()
    delete_tasks()
    # delete_task >> task_create


dag = Banhang_Clickup()
