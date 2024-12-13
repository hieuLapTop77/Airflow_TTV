import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Callable, Dict, List

import billiard as multiprocessing
import pandas as pd
import requests
from common.hook import hook
from common.variables import (
    API_KEY,
    TEMP_PATH,
    URL_DRIVER_DOWNLOAD,
    URL_DRIVER_REQUESTS,
)


def download_file_lastest_drive(folder_name: str, folder_id: str) -> str:
    """
        Download a file from the lastest from google drive

        returns a path saved in the lastest file
    """
    url = URL_DRIVER_REQUESTS.format(folder_id, API_KEY)
    response = requests.get(url, timeout=None)
    response.raise_for_status()
    files = response.json().get('files', [])
    if not files:
        print('No files found.')
        return
    latest_file = files[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
    download_response = requests.get(download_url, timeout=None)
    download_response.raise_for_status()
    file_local = os.path.join(TEMP_PATH, folder_name, file_name)
    os.makedirs(os.path.dirname(file_local), exist_ok=True)
    with open(file_local, 'wb') as f:
        f.write(download_response.content)
    print(f"Downloaded {file_name}")
    return file_local


def download_file_drive(folder_name: str, folder_id: str) -> list:
    """
        Download all files 30 hours ago from google drive

        returns list path file saved
    """
    downloaded_files = []
    time_limit = datetime.utcnow() - timedelta(hours=30)
    url = URL_DRIVER_REQUESTS.format(folder_id, API_KEY)
    print(url)
    response = requests.get(url, timeout=None)
    response.raise_for_status()
    files = response.json().get('files', [])
    if not files:
        print('No files found.')
        return []
    recent_files = [f for f in files if 'modifiedTime' in f and datetime.strptime(
        f['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ") > time_limit]
    if not recent_files:
        print('No files updated in the last 30 hours.')
        return []

    for file in recent_files:
        file_id = file['id']
        file_name = file['name']

        download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
        download_response = requests.get(download_url, timeout=None)
        try:
            download_response.raise_for_status()
        except:
            continue

        file_local = os.path.join(TEMP_PATH, folder_name, file_name)
        os.makedirs(os.path.dirname(file_local), exist_ok=True)

        with open(file_local, 'wb') as f:
            f.write(download_response.content)

        print(f"Downloaded {file_name}")
        downloaded_files.append(file_local)

    return downloaded_files


def call_api_get_list(sql: str, url: str, headers=None, params=None) -> list:
    """
        Call api get points lists from clickup
        return list of responses
    """
    list_results = []
    sql_conn = hook.get_conn()
    df = pd.read_sql(sql, sql_conn)
    if len(df["id"]) > 0:
        for i in df["id"]:
            response = requests.get(
                url.format(i),
                headers=headers,
                params=params,
                timeout=None,
            )
            if response.status_code == 200:
                list_results.append(response.json())
            else:
                print("Error please check at api: ", url.format(
                    i), " response: ", response.json())
    sql_conn.close()
    return list_results


def call_api_mutiple_pages(headers, params, name_url: str, url: str, task_id) -> list:
    """
        Call mutiple pages with api get tasks 
        return list response tasks
    """
    list_results = []
    task_ids = task_id
    print(f"Calling api {url.format(task_ids)} at page: ", params["page"])
    while True:
        print(
            f"Calling api {name_url} for task_id {task_ids} at page: ", params["page"])
        print(f"Calling api {url.format(task_ids)} at page: ", params["page"])
        response = requests.get(
            url.format(task_ids), headers=headers, params=params, timeout=None
        )
        if response.status_code == 200:
            data = response.json()
            list_results.append(data)
            if data.get("last_page", True):
                break
            params["page"] += 1
        else:
            print("Error please check api: ", url.format(task_ids))
            break

    return list_results


def call_multiple_thread(sql: str, function: Callable[[int], any], function_name: str, range_from: int = -1, range_to: int = -1) -> List[any]:
    """
        Call multiple threads with api get tasks
        return list response of tasks
    """
    list_results = []
    sql_conn = hook.get_conn()
    df = pd.read_sql(sql, sql_conn)
    sql_conn.close()

    if not df.empty and "id" in df.columns:
        num_cpus = multiprocessing.cpu_count()
        max_workers = num_cpus * 5

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            if range_from != -1 and range_to != -1:
                futures = [executor.submit(function, space_id)
                           for space_id in df["id"][range_from:range_to]]
            else:
                futures = [executor.submit(function, space_id)
                           for space_id in df["id"]]

            for future in as_completed(futures):
                try:
                    list_results.append(future.result())
                except Exception as e:
                    print(f"Error at function {function_name}: with error {e}")

    return list_results


def handle_df(df: str) -> pd.DataFrame:
    """
        Convert a string to dataframe
        return DataFrame
    """
    data = json.loads(df)
    df = pd.DataFrame.from_dict(data, orient='columns')
    return df


def insert_tasks(list_tasks: list, table_name: str, sql=None) -> None:
    """
        Insert a list of tasks into a table 
        return None
    """
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    if not list_tasks:
        sql_conn.close()
        print("No data to insert")
        return
    print("---------------------------------------------INSERT DATA----------------------------------------------------------")
    filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(
        item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
    data = [item for sublist in filtered_data for t in sublist for item in t["tasks"]]
    if not data:
        sql_conn.close()
        print("No data to insert")
        return
    df = pd.DataFrame(data)

    if sql:
        cursor.execute(sql)
        sql_conn.commit()
    else:
        sql_select = f"SELECT DISTINCT id FROM {table_name}"
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = df[~df['id'].isin(df_sql['id'])]

    if df.empty:
        sql_conn.close()
        print("No new data to insert.")
        return

    # Chuyển các cột JSON thành chuỗi JSON
    columns_to_json = [
        "status", "creator", "assignees", "group_assignees", "watchers",
        "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
        "locations", "sharing", "list", "project", "folder", "space"
    ]

    for column in columns_to_json:
        if column in df.columns:
            df[column] = df[column].apply(json.dumps)

    if 'subtasks' in df.columns:
        df['subtasks'] = df['subtasks'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else '[]')
    else:
        df['subtasks'] = '[]'

    df["status_nikko"] = 'no'
    df['orginal_id'] = ''
    df['order_nikko'] = ''

    # Tạo câu lệnh SQL để chèn dữ liệu
    sql = f"""
        INSERT INTO {table_name} (
            id, custom_id, custom_item_id, name, text_content, description, status, orderindex, date_created, date_updated,
            date_closed, date_done, archived, creator, assignees, group_assignees, watchers, checklists, tags, parent,
            priority, due_date, start_date, points, time_estimate, custom_fields, dependencies, linked_tasks, locations,
            team_id, url, sharing, permission_level, list, project, folder, space, subtasks, status_nikko, orginal_id,
            order_nikko, [dtm_Creation_Date]
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
        )
    """
    values = []
    for _index, row in df.iterrows():
        value = (
            str(row['id']), str(row['custom_id']), str(
                row['custom_item_id']), str(row['name']),
            str(row['text_content']), str(
                row['description']), str(row['status']),
            str(row['orderindex']), str(
                row['date_created']), str(row['date_updated']),
            str(row['date_closed']), str(
                row['date_done']), str(row['archived']),
            str(row['creator']), str(row['assignees']), str(
                row['group_assignees']),
            str(row['watchers']), str(row['checklists']), str(
                row['tags']), str(row['parent']),
            str(row['priority']), str(row['due_date']), str(row['start_date']),
            str(row['points']), str(row['time_estimate']), str(
                row['custom_fields']),
            str(row['dependencies']), str(
                row['linked_tasks']), str(row['locations']),
            str(row['team_id']), str(row['url']), str(row['sharing']),
            str(row['permission_level']), str(
                row['list']), str(row['project']),
            str(row['folder']), str(row['space']), str(row['subtasks']), str(
                row["status_nikko"]), str(row['orginal_id']), str(row['order_nikko'])
        )
        values.append(value)

    cursor.executemany(sql, values)

    print(
        f"Inserted {len(values)} rows into {table_name} out of {df.shape[0]} rows.")

    sql_conn.commit()
    sql_conn.close()


def insert_task_details(list_task_details: list, table_name: str) -> None:
    """
        Insert a task details into the table
        return none
    """
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    if not list_task_details:
        sql_conn.close()
        print("No data to insert.")
        return

    print("---------------------------------------------INSERT DATA----------------------------------------------------------")
    list_data = [item for sublist in list_task_details for item in sublist]
    if not list_data:
        sql_conn.close()
        print("No data to insert.")
        return

    df = pd.DataFrame(list_data)

    sql_select = f"SELECT DISTINCT id FROM {table_name}"
    df_sql = pd.read_sql(sql_select, sql_conn)
    df = df[~df['id'].isin(df_sql['id'])]

    if df.empty:
        sql_conn.close()
        print("No new data to insert.")
        return

    columns_to_json = [
        "status", "creator", "assignees", "group_assignees", "watchers",
        "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
        "locations", "sharing", "list", "project", "folder", "space", "attachments"
    ]
    for column in columns_to_json:
        if column in df.columns:
            df[column] = df[column].apply(json.dumps)

    if 'subtasks' in df.columns:
        df['subtasks'] = df['subtasks'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else '[]')
    else:
        df['subtasks'] = '[]'

    if 'time_sent' in df.columns:
        df['time_sent'] = df['time_sent'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else '')
    else:
        df['time_sent'] = ''
    df["status_nikko"] = 'no'
    df['orginal_id'] = ''
    df['order_nikko'] = ''

    new_col_order = [
        'id', 'custom_id', 'custom_item_id', 'name', 'text_content', 'description', 'status', 'orderindex',
        'date_created', 'date_updated', 'date_closed', 'date_done', 'archived', 'creator', 'assignees',
        'group_assignees', 'watchers', 'checklists', 'tags', 'parent', 'priority', 'due_date', 'start_date',
        'points', 'time_estimate', 'time_sent', 'custom_fields', 'dependencies', 'linked_tasks', 'locations',
        'team_id', 'url', 'sharing', 'permission_level', 'list', 'project', 'folder', 'space', 'subtasks',
        'attachments', 'status_nikko', 'orginal_id', 'order_nikko'
    ]
    df = df[new_col_order]
    sql = f"""
        INSERT INTO {table_name} (
            id, custom_id, custom_item_id, name, text_content, description, status, orderindex, date_created,
            date_updated, date_closed, date_done, archived, creator, assignees, group_assignees, watchers,
            checklists, tags, parent, priority, due_date, start_date, points, time_estimate, time_sent, custom_fields,
            dependencies, linked_tasks, locations, team_id, url, sharing, permission_level, list, project, folder,
            space, subtasks, attachments, status_nikko, orginal_id, order_nikko, [dtm_Creation_Date]
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
        )
    """

    values = [
        tuple(str(row[col]) for col in new_col_order)
        for _, row in df.iterrows()
    ]

    cursor.executemany(sql, values)

    print(
        f"Inserted {len(values)} rows into database with {df.shape[0]} rows.")

    sql_conn.commit()
    sql_conn.close()


def call_query_sql(query: str) -> None:
    """
        Run a query against the database
        return None
    """
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    print("--------------------------Query-----------------------")
    print(query)
    cursor.execute(query)
    sql_conn.commit()
    print("--------------------------Run query successfully-----------------------")
    sql_conn.close()


def download_single_file(url_download: str, temp_path: str, folder_name: str) -> str:
    file_name = url_download.split('/')[-1]
    response = requests.get(url_download, timeout=None)
    local_path = temp_path + folder_name + file_name
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f"DOWNLOAD: File downloaded {file_name} successfully")
    else:
        print(
            f"DOWNLOAD: Failed to download file. Status code: {response.status_code} on file name {file_name} response: {response.json()}")
    return local_path


def init_date() -> Dict[str, str]:
    current_time = datetime.now()
    date_to = int(current_time.timestamp()*1000)
    time_minus_24_hours = current_time - timedelta(hours=24)
    date_from = int(time_minus_24_hours.timestamp() * 1000)
    return {"date_from": date_from, "date_to": date_to}
