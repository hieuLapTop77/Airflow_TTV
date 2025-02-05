import os
from datetime import datetime, timedelta, timezone

import msal
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.hook import hook
from common.variables import (
    CLIENT_ID,
    CLIENT_SECRET,
    FILE_ID,
    TEMP_PATH,
    TENANT_ID,
    USER_ID,
)

FOLDER_NAME = 'kehoach'
FOLDER_BACKUP = "kehoach_ttv_backup"

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["TTV", "Kế hoạch", "vận hành"],
    max_active_runs=1,
)
def Ke_hoach_TTV():
    @task
    def move_files():
        source_file = get_url()
        if not source_file:
            return

        file_name = "File_ke_hoach.xlsx"
        file_name_save = file_name.split(".")[0] + "_" + (datetime.now(timezone.utc) + timedelta(
            hours=7) - timedelta(hours=3)).strftime('%Y%m%d%H%M%S') + "." + file_name.split(".")[1]
        destination_folder = os.path.join(TEMP_PATH, FOLDER_BACKUP)
        destination_file = os.path.join(destination_folder, file_name_save)
        os.rename(source_file, destination_file)
        print(f"Đã di chuyển {source_file} tới {destination_file}.")

    ######################################### API ################################################

    def get_token(client_id: str, tenant_id: str, client_secret: str) -> str:
        token = ""
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scopes = ["https://graph.microsoft.com/.default"]
        app = msal.ConfidentialClientApplication(
            client_id, authority=authority, client_credential=client_secret
        )
        token = app.acquire_token_for_client(scopes=scopes)
        if "access_token" in token:
            print("Access token created")
        else:
            print("Failed to get token:", token.get("error_description"))
        return token["access_token"]

    @task
    def download_latest_file() -> str:
        token = get_token(client_id=CLIENT_ID,
                          tenant_id=TENANT_ID, client_secret=CLIENT_SECRET)
        headers = {
            'Authorization': f'Bearer {token}'
        }
        response = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/items/{FILE_ID}",
            headers=headers, timeout=None
        )
        if response.status_code == 200:
            file_info = response.json()
            last_modified_str = file_info['lastModifiedDateTime']
            last_modified_time = datetime.strptime(
                last_modified_str, '%Y-%m-%dT%H:%M:%SZ')
            current_time = datetime.utcnow()
            desired_time = current_time - timedelta(hours=3)
            if last_modified_time > desired_time:
                print(
                    f"File đã được chỉnh sửa sau thời gian {desired_time}, tiến hành download...")
                response_download = requests.get(
                    f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/items/{FILE_ID}/content",
                    headers=headers, timeout=None
                )
                if response_download.status_code == 200:
                    path_file = os.path.join(
                        TEMP_PATH, FOLDER_NAME, "File_ke_hoach.xlsx")
                    with open(path_file, "wb") as f:
                        f.write(response_download.content)
                    print("Download thành công!")
                else:
                    print(
                        f"Lỗi khi download file: {response_download.status_code}, {response_download.json()}")
            else:
                print(
                    f"File không được chỉnh sửa sau thời gian {desired_time}, không cần download.")
        else:
            print(
                f"Lỗi khi lấy thông tin file: {response.status_code}, {response.json()}")

    def get_url() -> str:
        folder = os.path.join(TEMP_PATH, FOLDER_NAME)
        file_name = 'File_ke_hoach.xlsx'
        file_path = os.path.join(folder, file_name)
        if os.path.isfile(file_path):
            return file_path
        else:
            return ""

    ######################################### Function ############################################

    def insert_data(path_file: str, query: str, table_name: str, sheet_name: str, skip_rows=None) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        if not path_file:
            print("No data available to process")
            sql_conn.close()
            return

        df = pd.read_excel(
            path_file, index_col=None, engine='openpyxl', sheet_name=sheet_name, skiprows=skip_rows)
        if not df.empty:
            sql_del = f"truncate table {table_name};"
            cursor.execute(sql_del)
            values = [
                tuple(str(row[col]) for col in df.columns.tolist())
                for _, row in df.iterrows()
            ]
            cursor.executemany(query, values)

            print(
                f"Inserted sheet name '{sheet_name}' have {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()
    ######################################### INSERT DATA ################################################

    @task
    def insert_kehoach_nam() -> None:
        file_local = get_url()
        sql = """
                    INSERT INTO [dbo].[KeHoach_Nam_TTV]
                        ([Name],[Kenh]
                        ,[20240101],[20240201],[20240301],[20240401],[20240501],[20240601]
                        ,[20240701],[20240801],[20240901],[20241001],[20241101],[20241201]
                        ,[20250101],[20250201],[20250301],[20250401],[20250501],[20250601]
                        ,[20250701],[20250801],[20250901],[20251001],[20251101],[20251201]
                        ,[20260101],[20260201],[20260301],[20260401],[20260501],[20260601]
                        ,[20260701],[20260801],[20260901],[20261001],[20261101],[20261201]
                        ,[20270101],[20270201],[20270301],[20270401],[20270501],[20270601]
                        ,[20270701],[20270801],[20270901],[20271001],[20271101],[20271201]
                        ,[20280101],[20280201],[20280301],[20280401],[20280501],[20280601]
                        ,[20280701],[20280801],[20280901],[20281001],[20281101],[20281201]
                        ,[20290101],[20290201],[20290301],[20290401],[20290501],[20290601]
                        ,[20290701],[20290801],[20290901],[20291001],[20291101],[20291201]
                        ,[20300101],[20300201],[20300301],[20300401],[20300501],[20300601]
                        ,[20300701],[20300801],[20300901],[20301001],[20301101],[20301201]
                        ,[20310101],[20310201],[20310301],[20310401],[20310501],[20310601]
                        ,[20310701],[20310801],[20310901],[20311001],[20311101],[20311201]
                        ,[20320101],[20320201],[20320301],[20320401],[20320501],[20320601]
                        ,[20320701],[20320801],[20320901],[20321001],[20321101],[20321201]
                        ,[20330101],[20330201],[20330301],[20330401],[20330501],[20330601]
                        ,[20330701],[20330801],[20330901],[20331001],[20331101],[20331201]
                        ,[20340101],[20340201],[20340301],[20340401],[20340501],[20340601]
                        ,[20340701],[20340801],[20340901],[20341001],[20341101],[20341201]
                        ,[20350101],[20350201],[20350301],[20350401],[20350501],[20350601]
                        ,[20350701],[20350801],[20350901],[20351001],[20351101],[20351201])
                            VALUES(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s)
                """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_Nam_TTV]",
                    sheet_name="Kế hoạch năm TTV")

    @task
    def insert_kehoach_vanhanh_BH_MT():
        file_local = get_url()
        sql = """
            INSERT INTO [dbo].[KeHoach_VanHanh_BH_MT] (
                    [MA_HANG_HOA] ,[TEN_HANG_HOA] 
                    ,[KA_20240101] ,[SIEU_THI_20240101] ,[CVS_20240101] ,[HB_20240101] ,[KA_KHAC_20240101] 
                    ,[KENH_MT_1_20240101] ,[KENH_MT_2_20240101] ,[KENH_MT_3_20240101] ,[KENH_MT_4_20240101] 
                    ,[KENH_MT_5_20240101] ,[KENH_MT_6_20240101] ,[KENH_MT_7_20240101] ,[KENH_MT_8_20240101] 
                    ,[KENH_MT_9_20240101] ,[KENH_MT_10_20240101] ,[KA_20240201] ,[SIEU_THI_20240201] 
                    ,[CVS_20240201] ,[HB_20240201] ,[KA_KHAC_20240201] ,[KENH_MT_1_20240201] ,[KENH_MT_2_20240201] 
                    ,[KENH_MT_3_20240201] ,[KENH_MT_4_20240201] ,[KENH_MT_5_20240201] 
                    ,[KENH_MT_6_20240201] ,[KENH_MT_7_20240201] ,[KENH_MT_8_20240201] 
                    ,[KENH_MT_9_20240201] ,[KENH_MT_10_20240201] ,[KA_20240301] ,[SIEU_THI_20240301] 
                    ,[CVS_20240301] ,[HB_20240301] ,[KA_KHAC_20240301] ,[KENH_MT_1_20240301] ,[KENH_MT_2_20240301] 
                    ,[KENH_MT_3_20240301] ,[KENH_MT_4_20240301] ,[KENH_MT_5_20240301] ,[KENH_MT_6_20240301] 
                    ,[KENH_MT_7_20240301] ,[KENH_MT_8_20240301] ,[KENH_MT_9_20240301] ,[KENH_MT_10_20240301] 
                    ,[KA_20240401] ,[SIEU_THI_20240401] ,[CVS_20240401] ,[HB_20240401] ,[KA_KHAC_20240401] 
                    ,[KENH_MT_1_20240401] ,[KENH_MT_2_20240401] ,[KENH_MT_3_20240401] ,[KENH_MT_4_20240401] 
                    ,[KENH_MT_5_20240401] ,[KENH_MT_6_20240401] ,[KENH_MT_7_20240401] ,[KENH_MT_8_20240401] 
                    ,[KENH_MT_9_20240401] ,[KENH_MT_10_20240401] ,[KA_20240501] ,[SIEU_THI_20240501] ,[CVS_20240501] 
                    ,[HB_20240501] ,[KA_KHAC_20240501] ,[KENH_MT_1_20240501] ,[KENH_MT_2_20240501] ,[KENH_MT_3_20240501] 
                    ,[KENH_MT_4_20240501] ,[KENH_MT_5_20240501] ,[KENH_MT_6_20240501] ,[KENH_MT_7_20240501] 
                    ,[KENH_MT_8_20240501] ,[KENH_MT_9_20240501] ,[KENH_MT_10_20240501] ,[KA_20240601] 
                    ,[SIEU_THI_20240601] ,[CVS_20240601] ,[HB_20240601] ,[KA_KHAC_20240601] ,[KENH_MT_1_20240601] 
                    ,[KENH_MT_2_20240601] ,[KENH_MT_3_20240601] ,[KENH_MT_4_20240601] ,[KENH_MT_5_20240601] 
                    ,[KENH_MT_6_20240601] ,[KENH_MT_7_20240601] ,[KENH_MT_8_20240601] ,[KENH_MT_9_20240601] 
                    ,[KENH_MT_10_20240601] ,[KA_20240701] ,[SIEU_THI_20240701] ,[CVS_20240701] ,[HB_20240701] 
                    ,[KA_KHAC_20240701] ,[KENH_MT_1_20240701] ,[KENH_MT_2_20240701] ,[KENH_MT_3_20240701] 
                    ,[KENH_MT_4_20240701] ,[KENH_MT_5_20240701] ,[KENH_MT_6_20240701] ,[KENH_MT_7_20240701] 
                    ,[KENH_MT_8_20240701] ,[KENH_MT_9_20240701] ,[KENH_MT_10_20240701] ,[KA_20240801] 
                    ,[SIEU_THI_20240801] ,[CVS_20240801] ,[HB_20240801] ,[KA_KHAC_20240801] ,[KENH_MT_1_20240801] 
                    ,[KENH_MT_2_20240801] ,[KENH_MT_3_20240801] ,[KENH_MT_4_20240801] ,[KENH_MT_5_20240801] ,[KENH_MT_6_20240801] 
                    ,[KENH_MT_7_20240801] ,[KENH_MT_8_20240801] ,[KENH_MT_9_20240801] ,[KENH_MT_10_20240801] ,[KA_20240901] ,[SIEU_THI_20240901] 
                    ,[CVS_20240901] ,[HB_20240901] ,[KA_KHAC_20240901] ,[KENH_MT_1_20240901] ,[KENH_MT_2_20240901] ,[KENH_MT_3_20240901] 
                    ,[KENH_MT_4_20240901] ,[KENH_MT_5_20240901] ,[KENH_MT_6_20240901] ,[KENH_MT_7_20240901] ,[KENH_MT_8_20240901] 
                    ,[KENH_MT_9_20240901] ,[KENH_MT_10_20240901] ,[KA_20241001] ,[SIEU_THI_20241001] ,[CVS_20241001] ,[HB_20241001] 
                    ,[KA_KHAC_20241001] ,[KENH_MT_1_20241001] ,[KENH_MT_2_20241001] ,[KENH_MT_3_20241001] ,[KENH_MT_4_20241001] 
                    ,[KENH_MT_5_20241001] ,[KENH_MT_6_20241001] ,[KENH_MT_7_20241001] ,[KENH_MT_8_20241001] ,[KENH_MT_9_20241001] 
                    ,[KENH_MT_10_20241001] ,[KA_20241101] ,[SIEU_THI_20241101] ,[CVS_20241101] ,[HB_20241101] ,[KA_KHAC_20241101] 
                    ,[KENH_MT_1_20241101] ,[KENH_MT_2_20241101] ,[KENH_MT_3_20241101] ,[KENH_MT_4_20241101] ,[KENH_MT_5_20241101] 
                    ,[KENH_MT_6_20241101] ,[KENH_MT_7_20241101] ,[KENH_MT_8_20241101] ,[KENH_MT_9_20241101] ,[KENH_MT_10_20241101] 
                    ,[KA_20241201] ,[SIEU_THI_20241201] ,[CVS_20241201] ,[HB_20241201] ,[KA_KHAC_20241201] ,[KENH_MT_1_20241201] 
                    ,[KENH_MT_2_20241201] ,[KENH_MT_3_20241201] ,[KENH_MT_4_20241201] ,[KENH_MT_5_20241201] ,[KENH_MT_6_20241201] 
                    ,[KENH_MT_7_20241201] ,[KENH_MT_8_20241201] ,[KENH_MT_9_20241201] ,[KENH_MT_10_20241201] ,[KA_20250101] 
                    ,[SIEU_THI_20250101] ,[CVS_20250101] ,[HB_20250101] ,[KA_KHAC_20250101] ,[KENH_MT_1_20250101] ,[KENH_MT_2_20250101] 
                    ,[KENH_MT_3_20250101] ,[KENH_MT_4_20250101] ,[KENH_MT_5_20250101] ,[KENH_MT_6_20250101] ,[KENH_MT_7_20250101] 
                    ,[KENH_MT_8_20250101] ,[KENH_MT_9_20250101] ,[KENH_MT_10_20250101] ,[KA_20250201] ,[SIEU_THI_20250201] ,[CVS_20250201] 
                    ,[HB_20250201] ,[KA_KHAC_20250201] ,[KENH_MT_1_20250201] ,[KENH_MT_2_20250201] ,[KENH_MT_3_20250201] ,[KENH_MT_4_20250201] 
                    ,[KENH_MT_5_20250201] ,[KENH_MT_6_20250201] ,[KENH_MT_7_20250201] ,[KENH_MT_8_20250201] ,[KENH_MT_9_20250201] 
                    ,[KENH_MT_10_20250201] ,[KA_20250301] ,[SIEU_THI_20250301] ,[CVS_20250301] ,[HB_20250301] ,[KA_KHAC_20250301] 
                    ,[KENH_MT_1_20250301] ,[KENH_MT_2_20250301] ,[KENH_MT_3_20250301] ,[KENH_MT_4_20250301] ,[KENH_MT_5_20250301] 
                    ,[KENH_MT_6_20250301] ,[KENH_MT_7_20250301] ,[KENH_MT_8_20250301] ,[KENH_MT_9_20250301] ,[KENH_MT_10_20250301] 
                    ,[KA_20250401] ,[SIEU_THI_20250401] ,[CVS_20250401] ,[HB_20250401] ,[KA_KHAC_20250401] ,[KENH_MT_1_20250401] 
                    ,[KENH_MT_2_20250401] ,[KENH_MT_3_20250401] ,[KENH_MT_4_20250401] ,[KENH_MT_5_20250401] ,[KENH_MT_6_20250401] 
                    ,[KENH_MT_7_20250401] ,[KENH_MT_8_20250401] ,[KENH_MT_9_20250401] ,[KENH_MT_10_20250401] ,[KA_20250501] 
                    ,[SIEU_THI_20250501] ,[CVS_20250501] ,[HB_20250501] ,[KA_KHAC_20250501] ,[KENH_MT_1_20250501] ,[KENH_MT_2_20250501] 
                    ,[KENH_MT_3_20250501] ,[KENH_MT_4_20250501] ,[KENH_MT_5_20250501] ,[KENH_MT_6_20250501] ,[KENH_MT_7_20250501] 
                    ,[KENH_MT_8_20250501] ,[KENH_MT_9_20250501] ,[KENH_MT_10_20250501] ,[KA_20250601] ,[SIEU_THI_20250601] ,[CVS_20250601] 
                    ,[HB_20250601] ,[KA_KHAC_20250601] ,[KENH_MT_1_20250601] ,[KENH_MT_2_20250601] ,[KENH_MT_3_20250601] ,[KENH_MT_4_20250601] 
                    ,[KENH_MT_5_20250601] ,[KENH_MT_6_20250601] ,[KENH_MT_7_20250601] ,[KENH_MT_8_20250601] ,[KENH_MT_9_20250601] ,[KENH_MT_10_20250601] 
                    ,[KA_20250701] ,[SIEU_THI_20250701] ,[CVS_20250701] ,[HB_20250701] ,[KA_KHAC_20250701] ,[KENH_MT_1_20250701] ,[KENH_MT_2_20250701] 
                    ,[KENH_MT_3_20250701] ,[KENH_MT_4_20250701] ,[KENH_MT_5_20250701] ,[KENH_MT_6_20250701] ,[KENH_MT_7_20250701] ,[KENH_MT_8_20250701] 
                    ,[KENH_MT_9_20250701] ,[KENH_MT_10_20250701] ,[KA_20250801] ,[SIEU_THI_20250801] ,[CVS_20250801] ,[HB_20250801] 
                    ,[KA_KHAC_20250801] ,[KENH_MT_1_20250801] ,[KENH_MT_2_20250801] ,[KENH_MT_3_20250801] ,[KENH_MT_4_20250801] 
                    ,[KENH_MT_5_20250801] ,[KENH_MT_6_20250801] ,[KENH_MT_7_20250801] ,[KENH_MT_8_20250801] ,[KENH_MT_9_20250801] 
                    ,[KENH_MT_10_20250801] ,[KA_20250901] ,[SIEU_THI_20250901] ,[CVS_20250901] ,[HB_20250901] ,[KA_KHAC_20250901] ,[KENH_MT_1_20250901] 
                    ,[KENH_MT_2_20250901] ,[KENH_MT_3_20250901] ,[KENH_MT_4_20250901] ,[KENH_MT_5_20250901] ,[KENH_MT_6_20250901] ,[KENH_MT_7_20250901] 
                    ,[KENH_MT_8_20250901] ,[KENH_MT_9_20250901] ,[KENH_MT_10_20250901] ,[KA_20251001] ,[SIEU_THI_20251001] ,[CVS_20251001] 
                    ,[HB_20251001] ,[KA_KHAC_20251001] ,[KENH_MT_1_20251001] ,[KENH_MT_2_20251001] ,[KENH_MT_3_20251001] ,[KENH_MT_4_20251001] 
                    ,[KENH_MT_5_20251001] ,[KENH_MT_6_20251001] ,[KENH_MT_7_20251001] ,[KENH_MT_8_20251001] ,[KENH_MT_9_20251001] 
                    ,[KENH_MT_10_20251001] ,[KA_20251101] ,[SIEU_THI_20251101] ,[CVS_20251101] ,[HB_20251101] ,[KA_KHAC_20251101] 
                    ,[KENH_MT_1_20251101] ,[KENH_MT_2_20251101] ,[KENH_MT_3_20251101] ,[KENH_MT_4_20251101] ,[KENH_MT_5_20251101] 
                    ,[KENH_MT_6_20251101] ,[KENH_MT_7_20251101] ,[KENH_MT_8_20251101] ,[KENH_MT_9_20251101] ,[KENH_MT_10_20251101] 
                    ,[KA_20251201] ,[SIEU_THI_20251201] ,[CVS_20251201] ,[HB_20251201] ,[KA_KHAC_20251201] ,[KENH_MT_1_20251201] 
                    ,[KENH_MT_2_20251201] ,[KENH_MT_3_20251201] ,[KENH_MT_4_20251201] ,[KENH_MT_5_20251201] ,[KENH_MT_6_20251201] 
                    ,[KENH_MT_7_20251201] ,[KENH_MT_8_20251201] ,[KENH_MT_9_20251201] ,[KENH_MT_10_20251201])     
                VALUES  (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_MT]",
                    sheet_name="Kế hoạch vận hành BH MT", skip_rows=1)

    @task
    def insert_kehoach_vanhanh_BH_TT():
        file_local = get_url()
        sql = """
            INSERT INTO [dbo].[KeHoach_VanHanh_BH_TT]
                    ([MA_HANG_HOA]
                    ,[TEN_HANG_HOA]
                    ,[DCYK_20240101]
                    ,[NHA_THUOC_20240101]
                    ,[TAP_HOA_20240101]
                    ,[NHA_PHAN_PHOI_20240101]
                    ,[TT_KHAC_20240101]
                    ,[ETC_20240101]
                    ,[KENH_TT_1_20240101]
                    ,[KENH_TT_2_20240101]
                    ,[KENH_TT_3_20240101]
                    ,[KENH_TT_4_20240101]
                    ,[KENH_TT_5_20240101]
                    ,[KENH_TT_6_20240101]
                    ,[KENH_TT_7_20240101]
                    ,[KENH_TT_8_20240101]
                    ,[KENH_TT_9_20240101]
                    ,[KENH_TT_10_20240101]
                    ,[DCYK_20240201]
                    ,[NHA_THUOC_20240201]
                    ,[TAP_HOA_20240201]
                    ,[NHA_PHAN_PHOI_20240201]
                    ,[TT_KHAC_20240201]
                    ,[ETC_20240201]
                    ,[KENH_TT_1_20240201]
                    ,[KENH_TT_2_20240201]
                    ,[KENH_TT_3_20240201]
                    ,[KENH_TT_4_20240201]
                    ,[KENH_TT_5_20240201]
                    ,[KENH_TT_6_20240201]
                    ,[KENH_TT_7_20240201]
                    ,[KENH_TT_8_20240201]
                    ,[KENH_TT_9_20240201]
                    ,[KENH_TT_10_20240201]
                    ,[DCYK_20240301]
                    ,[NHA_THUOC_20240301]
                    ,[TAP_HOA_20240301]
                    ,[NHA_PHAN_PHOI_20240301]
                    ,[TT_KHAC_20240301]
                    ,[ETC_20240301]
                    ,[KENH_TT_1_20240301]
                    ,[KENH_TT_2_20240301]
                    ,[KENH_TT_3_20240301]
                    ,[KENH_TT_4_20240301]
                    ,[KENH_TT_5_20240301]
                    ,[KENH_TT_6_20240301]
                    ,[KENH_TT_7_20240301]
                    ,[KENH_TT_8_20240301]
                    ,[KENH_TT_9_20240301]
                    ,[KENH_TT_10_20240301]
                    ,[DCYK_20240401]
                    ,[NHA_THUOC_20240401]
                    ,[TAP_HOA_20240401]
                    ,[NHA_PHAN_PHOI_20240401]
                    ,[TT_KHAC_20240401]
                    ,[ETC_20240401]
                    ,[KENH_TT_1_20240401]
                    ,[KENH_TT_2_20240401]
                    ,[KENH_TT_3_20240401]
                    ,[KENH_TT_4_20240401]
                    ,[KENH_TT_5_20240401]
                    ,[KENH_TT_6_20240401]
                    ,[KENH_TT_7_20240401]
                    ,[KENH_TT_8_20240401]
                    ,[KENH_TT_9_20240401]
                    ,[KENH_TT_10_20240401]
                    ,[DCYK_20240501]
                    ,[NHA_THUOC_20240501]
                    ,[TAP_HOA_20240501]
                    ,[NHA_PHAN_PHOI_20240501]
                    ,[TT_KHAC_20240501]
                    ,[ETC_20240501]
                    ,[KENH_TT_1_20240501]
                    ,[KENH_TT_2_20240501]
                    ,[KENH_TT_3_20240501]
                    ,[KENH_TT_4_20240501]
                    ,[KENH_TT_5_20240501]
                    ,[KENH_TT_6_20240501]
                    ,[KENH_TT_7_20240501]
                    ,[KENH_TT_8_20240501]
                    ,[KENH_TT_9_20240501]
                    ,[KENH_TT_10_20240501]
                    ,[DCYK_20240601]
                    ,[NHA_THUOC_20240601]
                    ,[TAP_HOA_20240601]
                    ,[NHA_PHAN_PHOI_20240601]
                    ,[TT_KHAC_20240601]
                    ,[ETC_20240601]
                    ,[KENH_TT_1_20240601]
                    ,[KENH_TT_2_20240601]
                    ,[KENH_TT_3_20240601]
                    ,[KENH_TT_4_20240601]
                    ,[KENH_TT_5_20240601]
                    ,[KENH_TT_6_20240601]
                    ,[KENH_TT_7_20240601]
                    ,[KENH_TT_8_20240601]
                    ,[KENH_TT_9_20240601]
                    ,[KENH_TT_10_20240601]
                    ,[DCYK_20240701]
                    ,[NHA_THUOC_20240701]
                    ,[TAP_HOA_20240701]
                    ,[NHA_PHAN_PHOI_20240701]
                    ,[TT_KHAC_20240701]
                    ,[ETC_20240701]
                    ,[KENH_TT_1_20240701]
                    ,[KENH_TT_2_20240701]
                    ,[KENH_TT_3_20240701]
                    ,[KENH_TT_4_20240701]
                    ,[KENH_TT_5_20240701]
                    ,[KENH_TT_6_20240701]
                    ,[KENH_TT_7_20240701]
                    ,[KENH_TT_8_20240701]
                    ,[KENH_TT_9_20240701]
                    ,[KENH_TT_10_20240701]
                    ,[DCYK_20240801]
                    ,[NHA_THUOC_20240801]
                    ,[TAP_HOA_20240801]
                    ,[NHA_PHAN_PHOI_20240801]
                    ,[TT_KHAC_20240801]
                    ,[ETC_20240801]
                    ,[KENH_TT_1_20240801]
                    ,[KENH_TT_2_20240801]
                    ,[KENH_TT_3_20240801]
                    ,[KENH_TT_4_20240801]
                    ,[KENH_TT_5_20240801]
                    ,[KENH_TT_6_20240801]
                    ,[KENH_TT_7_20240801]
                    ,[KENH_TT_8_20240801]
                    ,[KENH_TT_9_20240801]
                    ,[KENH_TT_10_20240801]
                    ,[DCYK_20240901]
                    ,[NHA_THUOC_20240901]
                    ,[TAP_HOA_20240901]
                    ,[NHA_PHAN_PHOI_20240901]
                    ,[TT_KHAC_20240901]
                    ,[ETC_20240901]
                    ,[KENH_TT_1_20240901]
                    ,[KENH_TT_2_20240901]
                    ,[KENH_TT_3_20240901]
                    ,[KENH_TT_4_20240901]
                    ,[KENH_TT_5_20240901]
                    ,[KENH_TT_6_20240901]
                    ,[KENH_TT_7_20240901]
                    ,[KENH_TT_8_20240901]
                    ,[KENH_TT_9_20240901]
                    ,[KENH_TT_10_20240901]
                    ,[DCYK_20241001]
                    ,[NHA_THUOC_20241001]
                    ,[TAP_HOA_20241001]
                    ,[NHA_PHAN_PHOI_20241001]
                    ,[TT_KHAC_20241001]
                    ,[ETC_20241001]
                    ,[KENH_TT_1_20241001]
                    ,[KENH_TT_2_20241001]
                    ,[KENH_TT_3_20241001]
                    ,[KENH_TT_4_20241001]
                    ,[KENH_TT_5_20241001]
                    ,[KENH_TT_6_20241001]
                    ,[KENH_TT_7_20241001]
                    ,[KENH_TT_8_20241001]
                    ,[KENH_TT_9_20241001]
                    ,[KENH_TT_10_20241001]
                    ,[DCYK_20241101]
                    ,[NHA_THUOC_20241101]
                    ,[TAP_HOA_20241101]
                    ,[NHA_PHAN_PHOI_20241101]
                    ,[TT_KHAC_20241101]
                    ,[ETC_20241101]
                    ,[KENH_TT_1_20241101]
                    ,[KENH_TT_2_20241101]
                    ,[KENH_TT_3_20241101]
                    ,[KENH_TT_4_20241101]
                    ,[KENH_TT_5_20241101]
                    ,[KENH_TT_6_20241101]
                    ,[KENH_TT_7_20241101]
                    ,[KENH_TT_8_20241101]
                    ,[KENH_TT_9_20241101]
                    ,[KENH_TT_10_20241101]
                    ,[DCYK_20241201]
                    ,[NHA_THUOC_20241201]
                    ,[TAP_HOA_20241201]
                    ,[NHA_PHAN_PHOI_20241201]
                    ,[TT_KHAC_20241201]
                    ,[ETC_20241201]
                    ,[KENH_TT_1_20241201]
                    ,[KENH_TT_2_20241201]
                    ,[KENH_TT_3_20241201]
                    ,[KENH_TT_4_20241201]
                    ,[KENH_TT_5_20241201]
                    ,[KENH_TT_6_20241201]
                    ,[KENH_TT_7_20241201]
                    ,[KENH_TT_8_20241201]
                    ,[KENH_TT_9_20241201]
                    ,[KENH_TT_10_20241201]
                    ,[DCYK_20250101]
                    ,[NHA_THUOC_20250101]
                    ,[TAP_HOA_20250101]
                    ,[NHA_PHAN_PHOI_20250101]
                    ,[TT_KHAC_20250101]
                    ,[ETC_20250101]
                    ,[KENH_TT_1_20250101]
                    ,[KENH_TT_2_20250101]
                    ,[KENH_TT_3_20250101]
                    ,[KENH_TT_4_20250101]
                    ,[KENH_TT_5_20250101]
                    ,[KENH_TT_6_20250101]
                    ,[KENH_TT_7_20250101]
                    ,[KENH_TT_8_20250101]
                    ,[KENH_TT_9_20250101]
                    ,[KENH_TT_10_20250101]
                    ,[DCYK_20250201]
                    ,[NHA_THUOC_20250201]
                    ,[TAP_HOA_20250201]
                    ,[NHA_PHAN_PHOI_20250201]
                    ,[TT_KHAC_20250201]
                    ,[ETC_20250201]
                    ,[KENH_TT_1_20250201]
                    ,[KENH_TT_2_20250201]
                    ,[KENH_TT_3_20250201]
                    ,[KENH_TT_4_20250201]
                    ,[KENH_TT_5_20250201]
                    ,[KENH_TT_6_20250201]
                    ,[KENH_TT_7_20250201]
                    ,[KENH_TT_8_20250201]
                    ,[KENH_TT_9_20250201]
                    ,[KENH_TT_10_20250201]
                    ,[DCYK_20250301]
                    ,[NHA_THUOC_20250301]
                    ,[TAP_HOA_20250301]
                    ,[NHA_PHAN_PHOI_20250301]
                    ,[TT_KHAC_20250301]
                    ,[ETC_20250301]
                    ,[KENH_TT_1_20250301]
                    ,[KENH_TT_2_20250301]
                    ,[KENH_TT_3_20250301]
                    ,[KENH_TT_4_20250301]
                    ,[KENH_TT_5_20250301]
                    ,[KENH_TT_6_20250301]
                    ,[KENH_TT_7_20250301]
                    ,[KENH_TT_8_20250301]
                    ,[KENH_TT_9_20250301]
                    ,[KENH_TT_10_20250301]
                    ,[DCYK_20250401]
                    ,[NHA_THUOC_20250401]
                    ,[TAP_HOA_20250401]
                    ,[NHA_PHAN_PHOI_20250401]
                    ,[TT_KHAC_20250401]
                    ,[ETC_20250401]
                    ,[KENH_TT_1_20250401]
                    ,[KENH_TT_2_20250401]
                    ,[KENH_TT_3_20250401]
                    ,[KENH_TT_4_20250401]
                    ,[KENH_TT_5_20250401]
                    ,[KENH_TT_6_20250401]
                    ,[KENH_TT_7_20250401]
                    ,[KENH_TT_8_20250401]
                    ,[KENH_TT_9_20250401]
                    ,[KENH_TT_10_20250401]
                    ,[DCYK_20250501]
                    ,[NHA_THUOC_20250501]
                    ,[TAP_HOA_20250501]
                    ,[NHA_PHAN_PHOI_20250501]
                    ,[TT_KHAC_20250501]
                    ,[ETC_20250501]
                    ,[KENH_TT_1_20250501]
                    ,[KENH_TT_2_20250501]
                    ,[KENH_TT_3_20250501]
                    ,[KENH_TT_4_20250501]
                    ,[KENH_TT_5_20250501]
                    ,[KENH_TT_6_20250501]
                    ,[KENH_TT_7_20250501]
                    ,[KENH_TT_8_20250501]
                    ,[KENH_TT_9_20250501]
                    ,[KENH_TT_10_20250501]
                    ,[DCYK_20250601]
                    ,[NHA_THUOC_20250601]
                    ,[TAP_HOA_20250601]
                    ,[NHA_PHAN_PHOI_20250601]
                    ,[TT_KHAC_20250601]
                    ,[ETC_20250601]
                    ,[KENH_TT_1_20250601]
                    ,[KENH_TT_2_20250601]
                    ,[KENH_TT_3_20250601]
                    ,[KENH_TT_4_20250601]
                    ,[KENH_TT_5_20250601]
                    ,[KENH_TT_6_20250601]
                    ,[KENH_TT_7_20250601]
                    ,[KENH_TT_8_20250601]
                    ,[KENH_TT_9_20250601]
                    ,[KENH_TT_10_20250601]
                    ,[DCYK_20250701]
                    ,[NHA_THUOC_20250701]
                    ,[TAP_HOA_20250701]
                    ,[NHA_PHAN_PHOI_20250701]
                    ,[TT_KHAC_20250701]
                    ,[ETC_20250701]
                    ,[KENH_TT_1_20250701]
                    ,[KENH_TT_2_20250701]
                    ,[KENH_TT_3_20250701]
                    ,[KENH_TT_4_20250701]
                    ,[KENH_TT_5_20250701]
                    ,[KENH_TT_6_20250701]
                    ,[KENH_TT_7_20250701]
                    ,[KENH_TT_8_20250701]
                    ,[KENH_TT_9_20250701]
                    ,[KENH_TT_10_20250701]
                    ,[DCYK_20250801]
                    ,[NHA_THUOC_20250801]
                    ,[TAP_HOA_20250801]
                    ,[NHA_PHAN_PHOI_20250801]
                    ,[TT_KHAC_20250801]
                    ,[ETC_20250801]
                    ,[KENH_TT_1_20250801]
                    ,[KENH_TT_2_20250801]
                    ,[KENH_TT_3_20250801]
                    ,[KENH_TT_4_20250801]
                    ,[KENH_TT_5_20250801]
                    ,[KENH_TT_6_20250801]
                    ,[KENH_TT_7_20250801]
                    ,[KENH_TT_8_20250801]
                    ,[KENH_TT_9_20250801]
                    ,[KENH_TT_10_20250801]
                    ,[DCYK_20250901]
                    ,[NHA_THUOC_20250901]
                    ,[TAP_HOA_20250901]
                    ,[NHA_PHAN_PHOI_20250901]
                    ,[TT_KHAC_20250901]
                    ,[ETC_20250901]
                    ,[KENH_TT_1_20250901]
                    ,[KENH_TT_2_20250901]
                    ,[KENH_TT_3_20250901]
                    ,[KENH_TT_4_20250901]
                    ,[KENH_TT_5_20250901]
                    ,[KENH_TT_6_20250901]
                    ,[KENH_TT_7_20250901]
                    ,[KENH_TT_8_20250901]
                    ,[KENH_TT_9_20250901]
                    ,[KENH_TT_10_20250901]
                    ,[DCYK_20251001]
                    ,[NHA_THUOC_20251001]
                    ,[TAP_HOA_20251001]
                    ,[NHA_PHAN_PHOI_20251001]
                    ,[TT_KHAC_20251001]
                    ,[ETC_20251001]
                    ,[KENH_TT_1_20251001]
                    ,[KENH_TT_2_20251001]
                    ,[KENH_TT_3_20251001]
                    ,[KENH_TT_4_20251001]
                    ,[KENH_TT_5_20251001]
                    ,[KENH_TT_6_20251001]
                    ,[KENH_TT_7_20251001]
                    ,[KENH_TT_8_20251001]
                    ,[KENH_TT_9_20251001]
                    ,[KENH_TT_10_20251001]
                    ,[DCYK_20251101]
                    ,[NHA_THUOC_20251101]
                    ,[TAP_HOA_20251101]
                    ,[NHA_PHAN_PHOI_20251101]
                    ,[TT_KHAC_20251101]
                    ,[ETC_20251101]
                    ,[KENH_TT_1_20251101]
                    ,[KENH_TT_2_20251101]
                    ,[KENH_TT_3_20251101]
                    ,[KENH_TT_4_20251101]
                    ,[KENH_TT_5_20251101]
                    ,[KENH_TT_6_20251101]
                    ,[KENH_TT_7_20251101]
                    ,[KENH_TT_8_20251101]
                    ,[KENH_TT_9_20251101]
                    ,[KENH_TT_10_20251101]
                    ,[DCYK_20251201]
                    ,[NHA_THUOC_20251201]
                    ,[TAP_HOA_20251201]
                    ,[NHA_PHAN_PHOI_20251201]
                    ,[TT_KHAC_20251201]
                    ,[ETC_20251201]
                    ,[KENH_TT_1_20251201]
                    ,[KENH_TT_2_20251201]
                    ,[KENH_TT_3_20251201]
                    ,[KENH_TT_4_20251201]
                    ,[KENH_TT_5_20251201]
                    ,[KENH_TT_6_20251201]
                    ,[KENH_TT_7_20251201]
                    ,[KENH_TT_8_20251201]
                    ,[KENH_TT_9_20251201]
                    ,[KENH_TT_10_20251201])
                VALUES
                    (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_TT]",
                    sheet_name="Kế hoạch vận hành BH TT", skip_rows=1)

    @task
    def insert_kehoach_vanhanh_BH_Online():
        file_local = get_url()
        sql = """
            INSERT INTO [dbo].[KeHoach_VanHanh_BH_Online] (
                   [MA_HANG_HOA]
                ,[TEN_HANG_HOA]
                ,[ECOM_20240101]
                ,[ABCARE_20240101]
                ,[KENH_ONLINE_1_20240101]
                ,[KENH_ONLINE_2_20240101]
                ,[KENH_ONLINE_3_20240101]
                ,[KENH_ONLINE_4_20240101]
                ,[KENH_ONLINE_5_20240101]
                ,[KENH_ONLINE_6_20240101]
                ,[KENH_ONLINE_7_20240101]
                ,[KENH_ONLINE_8_20240101]
                ,[KENH_ONLINE_9_20240101]
                ,[KENH_ONLINE_10_20240101]
                ,[ECOM_20240201]
                ,[ABCARE_20240201]
                ,[KENH_ONLINE_1_20240201]
                ,[KENH_ONLINE_2_20240201]
                ,[KENH_ONLINE_3_20240201]
                ,[KENH_ONLINE_4_20240201]
                ,[KENH_ONLINE_5_20240201]
                ,[KENH_ONLINE_6_20240201]
                ,[KENH_ONLINE_7_20240201]
                ,[KENH_ONLINE_8_20240201]
                ,[KENH_ONLINE_9_20240201]
                ,[KENH_ONLINE_10_20240201]
                ,[ECOM_20240301]
                ,[ABCARE_20240301]
                ,[KENH_ONLINE_1_20240301]
                ,[KENH_ONLINE_2_20240301]
                ,[KENH_ONLINE_3_20240301]
                ,[KENH_ONLINE_4_20240301]
                ,[KENH_ONLINE_5_20240301]
                ,[KENH_ONLINE_6_20240301]
                ,[KENH_ONLINE_7_20240301]
                ,[KENH_ONLINE_8_20240301]
                ,[KENH_ONLINE_9_20240301]
                ,[KENH_ONLINE_10_20240301]
                ,[ECOM_20240401]
                ,[ABCARE_20240401]
                ,[KENH_ONLINE_1_20240401]
                ,[KENH_ONLINE_2_20240401]
                ,[KENH_ONLINE_3_20240401]
                ,[KENH_ONLINE_4_20240401]
                ,[KENH_ONLINE_5_20240401]
                ,[KENH_ONLINE_6_20240401]
                ,[KENH_ONLINE_7_20240401]
                ,[KENH_ONLINE_8_20240401]
                ,[KENH_ONLINE_9_20240401]
                ,[KENH_ONLINE_10_20240401]
                ,[ECOM_20240501]
                ,[ABCARE_20240501]
                ,[KENH_ONLINE_1_20240501]
                ,[KENH_ONLINE_2_20240501]
                ,[KENH_ONLINE_3_20240501]
                ,[KENH_ONLINE_4_20240501]
                ,[KENH_ONLINE_5_20240501]
                ,[KENH_ONLINE_6_20240501]
                ,[KENH_ONLINE_7_20240501]
                ,[KENH_ONLINE_8_20240501]
                ,[KENH_ONLINE_9_20240501]
                ,[KENH_ONLINE_10_20240501]
                ,[ECOM_20240601]
                ,[ABCARE_20240601]
                ,[KENH_ONLINE_1_20240601]
                ,[KENH_ONLINE_2_20240601]
                ,[KENH_ONLINE_3_20240601]
                ,[KENH_ONLINE_4_20240601]
                ,[KENH_ONLINE_5_20240601]
                ,[KENH_ONLINE_6_20240601]
                ,[KENH_ONLINE_7_20240601]
                ,[KENH_ONLINE_8_20240601]
                ,[KENH_ONLINE_9_20240601]
                ,[KENH_ONLINE_10_20240601]
                ,[ECOM_20240701]
                ,[ABCARE_20240701]
                ,[KENH_ONLINE_1_20240701]
                ,[KENH_ONLINE_2_20240701]
                ,[KENH_ONLINE_3_20240701]
                ,[KENH_ONLINE_4_20240701]
                ,[KENH_ONLINE_5_20240701]
                ,[KENH_ONLINE_6_20240701]
                ,[KENH_ONLINE_7_20240701]
                ,[KENH_ONLINE_8_20240701]
                ,[KENH_ONLINE_9_20240701]
                ,[KENH_ONLINE_10_20240701]
                ,[ECOM_20240801]
                ,[ABCARE_20240801]
                ,[KENH_ONLINE_1_20240801]
                ,[KENH_ONLINE_2_20240801]
                ,[KENH_ONLINE_3_20240801]
                ,[KENH_ONLINE_4_20240801]
                ,[KENH_ONLINE_5_20240801]
                ,[KENH_ONLINE_6_20240801]
                ,[KENH_ONLINE_7_20240801]
                ,[KENH_ONLINE_8_20240801]
                ,[KENH_ONLINE_9_20240801]
                ,[KENH_ONLINE_10_20240801]
                ,[ECOM_20240901]
                ,[ABCARE_20240901]
                ,[KENH_ONLINE_1_20240901]
                ,[KENH_ONLINE_2_20240901]
                ,[KENH_ONLINE_3_20240901]
                ,[KENH_ONLINE_4_20240901]
                ,[KENH_ONLINE_5_20240901]
                ,[KENH_ONLINE_6_20240901]
                ,[KENH_ONLINE_7_20240901]
                ,[KENH_ONLINE_8_20240901]
                ,[KENH_ONLINE_9_20240901]
                ,[KENH_ONLINE_10_20240901]
                ,[ECOM_20241001]
                ,[ABCARE_20241001]
                ,[KENH_ONLINE_1_20241001]
                ,[KENH_ONLINE_2_20241001]
                ,[KENH_ONLINE_3_20241001]
                ,[KENH_ONLINE_4_20241001]
                ,[KENH_ONLINE_5_20241001]
                ,[KENH_ONLINE_6_20241001]
                ,[KENH_ONLINE_7_20241001]
                ,[KENH_ONLINE_8_20241001]
                ,[KENH_ONLINE_9_20241001]
                ,[KENH_ONLINE_10_20241001]
                ,[ECOM_20241101]
                ,[ABCARE_20241101]
                ,[KENH_ONLINE_1_20241101]
                ,[KENH_ONLINE_2_20241101]
                ,[KENH_ONLINE_3_20241101]
                ,[KENH_ONLINE_4_20241101]
                ,[KENH_ONLINE_5_20241101]
                ,[KENH_ONLINE_6_20241101]
                ,[KENH_ONLINE_7_20241101]
                ,[KENH_ONLINE_8_20241101]
                ,[KENH_ONLINE_9_20241101]
                ,[KENH_ONLINE_10_20241101]
                ,[ECOM_20241201]
                ,[ABCARE_20241201]
                ,[KENH_ONLINE_1_20241201]
                ,[KENH_ONLINE_2_20241201]
                ,[KENH_ONLINE_3_20241201]
                ,[KENH_ONLINE_4_20241201]
                ,[KENH_ONLINE_5_20241201]
                ,[KENH_ONLINE_6_20241201]
                ,[KENH_ONLINE_7_20241201]
                ,[KENH_ONLINE_8_20241201]
                ,[KENH_ONLINE_9_20241201]
                ,[KENH_ONLINE_10_20241201]
                ,[ECOM_20250101]
                ,[ABCARE_20250101]
                ,[KENH_ONLINE_1_20250101]
                ,[KENH_ONLINE_2_20250101]
                ,[KENH_ONLINE_3_20250101]
                ,[KENH_ONLINE_4_20250101]
                ,[KENH_ONLINE_5_20250101]
                ,[KENH_ONLINE_6_20250101]
                ,[KENH_ONLINE_7_20250101]
                ,[KENH_ONLINE_8_20250101]
                ,[KENH_ONLINE_9_20250101]
                ,[KENH_ONLINE_10_20250101]
                ,[ECOM_20250201]
                ,[ABCARE_20250201]
                ,[KENH_ONLINE_1_20250201]
                ,[KENH_ONLINE_2_20250201]
                ,[KENH_ONLINE_3_20250201]
                ,[KENH_ONLINE_4_20250201]
                ,[KENH_ONLINE_5_20250201]
                ,[KENH_ONLINE_6_20250201]
                ,[KENH_ONLINE_7_20250201]
                ,[KENH_ONLINE_8_20250201]
                ,[KENH_ONLINE_9_20250201]
                ,[KENH_ONLINE_10_20250201]
                ,[ECOM_20250301]
                ,[ABCARE_20250301]
                ,[KENH_ONLINE_1_20250301]
                ,[KENH_ONLINE_2_20250301]
                ,[KENH_ONLINE_3_20250301]
                ,[KENH_ONLINE_4_20250301]
                ,[KENH_ONLINE_5_20250301]
                ,[KENH_ONLINE_6_20250301]
                ,[KENH_ONLINE_7_20250301]
                ,[KENH_ONLINE_8_20250301]
                ,[KENH_ONLINE_9_20250301]
                ,[KENH_ONLINE_10_20250301]
                ,[ECOM_20250401]
                ,[ABCARE_20250401]
                ,[KENH_ONLINE_1_20250401]
                ,[KENH_ONLINE_2_20250401]
                ,[KENH_ONLINE_3_20250401]
                ,[KENH_ONLINE_4_20250401]
                ,[KENH_ONLINE_5_20250401]
                ,[KENH_ONLINE_6_20250401]
                ,[KENH_ONLINE_7_20250401]
                ,[KENH_ONLINE_8_20250401]
                ,[KENH_ONLINE_9_20250401]
                ,[KENH_ONLINE_10_20250401]
                ,[ECOM_20250501]
                ,[ABCARE_20250501]
                ,[KENH_ONLINE_1_20250501]
                ,[KENH_ONLINE_2_20250501]
                ,[KENH_ONLINE_3_20250501]
                ,[KENH_ONLINE_4_20250501]
                ,[KENH_ONLINE_5_20250501]
                ,[KENH_ONLINE_6_20250501]
                ,[KENH_ONLINE_7_20250501]
                ,[KENH_ONLINE_8_20250501]
                ,[KENH_ONLINE_9_20250501]
                ,[KENH_ONLINE_10_20250501]
                ,[ECOM_20250601]
                ,[ABCARE_20250601]
                ,[KENH_ONLINE_1_20250601]
                ,[KENH_ONLINE_2_20250601]
                ,[KENH_ONLINE_3_20250601]
                ,[KENH_ONLINE_4_20250601]
                ,[KENH_ONLINE_5_20250601]
                ,[KENH_ONLINE_6_20250601]
                ,[KENH_ONLINE_7_20250601]
                ,[KENH_ONLINE_8_20250601]
                ,[KENH_ONLINE_9_20250601]
                ,[KENH_ONLINE_10_20250601]
                ,[ECOM_20250701]
                ,[ABCARE_20250701]
                ,[KENH_ONLINE_1_20250701]
                ,[KENH_ONLINE_2_20250701]
                ,[KENH_ONLINE_3_20250701]
                ,[KENH_ONLINE_4_20250701]
                ,[KENH_ONLINE_5_20250701]
                ,[KENH_ONLINE_6_20250701]
                ,[KENH_ONLINE_7_20250701]
                ,[KENH_ONLINE_8_20250701]
                ,[KENH_ONLINE_9_20250701]
                ,[KENH_ONLINE_10_20250701]
                ,[ECOM_20250801]
                ,[ABCARE_20250801]
                ,[KENH_ONLINE_1_20250801]
                ,[KENH_ONLINE_2_20250801]
                ,[KENH_ONLINE_3_20250801]
                ,[KENH_ONLINE_4_20250801]
                ,[KENH_ONLINE_5_20250801]
                ,[KENH_ONLINE_6_20250801]
                ,[KENH_ONLINE_7_20250801]
                ,[KENH_ONLINE_8_20250801]
                ,[KENH_ONLINE_9_20250801]
                ,[KENH_ONLINE_10_20250801]
                ,[ECOM_20250901]
                ,[ABCARE_20250901]
                ,[KENH_ONLINE_1_20250901]
                ,[KENH_ONLINE_2_20250901]
                ,[KENH_ONLINE_3_20250901]
                ,[KENH_ONLINE_4_20250901]
                ,[KENH_ONLINE_5_20250901]
                ,[KENH_ONLINE_6_20250901]
                ,[KENH_ONLINE_7_20250901]
                ,[KENH_ONLINE_8_20250901]
                ,[KENH_ONLINE_9_20250901]
                ,[KENH_ONLINE_10_20250901]
                ,[ECOM_20251001]
                ,[ABCARE_20251001]
                ,[KENH_ONLINE_1_20251001]
                ,[KENH_ONLINE_2_20251001]
                ,[KENH_ONLINE_3_20251001]
                ,[KENH_ONLINE_4_20251001]
                ,[KENH_ONLINE_5_20251001]
                ,[KENH_ONLINE_6_20251001]
                ,[KENH_ONLINE_7_20251001]
                ,[KENH_ONLINE_8_20251001]
                ,[KENH_ONLINE_9_20251001]
                ,[KENH_ONLINE_10_20251001]
                ,[ECOM_20251101]
                ,[ABCARE_20251101]
                ,[KENH_ONLINE_1_20251101]
                ,[KENH_ONLINE_2_20251101]
                ,[KENH_ONLINE_3_20251101]
                ,[KENH_ONLINE_4_20251101]
                ,[KENH_ONLINE_5_20251101]
                ,[KENH_ONLINE_6_20251101]
                ,[KENH_ONLINE_7_20251101]
                ,[KENH_ONLINE_8_20251101]
                ,[KENH_ONLINE_9_20251101]
                ,[KENH_ONLINE_10_20251101]
                ,[ECOM_20251201]
                ,[ABCARE_20251201]
                ,[KENH_ONLINE_1_20251201]
                ,[KENH_ONLINE_2_20251201]
                ,[KENH_ONLINE_3_20251201]
                ,[KENH_ONLINE_4_20251201]
                ,[KENH_ONLINE_5_20251201]
                ,[KENH_ONLINE_6_20251201]
                ,[KENH_ONLINE_7_20251201]
                ,[KENH_ONLINE_8_20251201]
                ,[KENH_ONLINE_9_20251201]
                ,[KENH_ONLINE_10_20251201]
               )     
                VALUES  (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_Online]",
                    sheet_name="Kế hoạch vận hành BH Online", skip_rows=1)

    @task
    def insert_kehoach_vanhanh_BH_LocNuoc():
        file_local = get_url()
        sql = """
                INSERT INTO [dbo].[KeHoach_VanHanh_BH_LocNuoc]
                        ([MA_HANG_HOA]
                        ,[TEN_HANG_HOA]
                        ,[TGLN_20240101]
                        ,[DROPPII_20240101]
                        ,[KENH_LOC_NUOC_1_20240101]
                        ,[KENH_LOC_NUOC_2_20240101]
                        ,[KENH_LOC_NUOC_3_20240101]
                        ,[KENH_LOC_NUOC_4_20240101]
                        ,[KENH_LOC_NUOC_5_20240101]
                        ,[KENH_LOC_NUOC_6_20240101]
                        ,[KENH_LOC_NUOC_7_20240101]
                        ,[KENH_LOC_NUOC_8_20240101]
                        ,[KENH_LOC_NUOC_9_20240101]
                        ,[KENH_LOC_NUOC_10_20240101]
                        ,[TGLN_20240201]
                        ,[DROPPII_20240201]
                        ,[KENH_LOC_NUOC_1_20240201]
                        ,[KENH_LOC_NUOC_2_20240201]
                        ,[KENH_LOC_NUOC_3_20240201]
                        ,[KENH_LOC_NUOC_4_20240201]
                        ,[KENH_LOC_NUOC_5_20240201]
                        ,[KENH_LOC_NUOC_6_20240201]
                        ,[KENH_LOC_NUOC_7_20240201]
                        ,[KENH_LOC_NUOC_8_20240201]
                        ,[KENH_LOC_NUOC_9_20240201]
                        ,[KENH_LOC_NUOC_10_20240201]
                        ,[TGLN_20240301]
                        ,[DROPPII_20240301]
                        ,[KENH_LOC_NUOC_1_20240301]
                        ,[KENH_LOC_NUOC_2_20240301]
                        ,[KENH_LOC_NUOC_3_20240301]
                        ,[KENH_LOC_NUOC_4_20240301]
                        ,[KENH_LOC_NUOC_5_20240301]
                        ,[KENH_LOC_NUOC_6_20240301]
                        ,[KENH_LOC_NUOC_7_20240301]
                        ,[KENH_LOC_NUOC_8_20240301]
                        ,[KENH_LOC_NUOC_9_20240301]
                        ,[KENH_LOC_NUOC_10_20240301]
                        ,[TGLN_20240401]
                        ,[DROPPII_20240401]
                        ,[KENH_LOC_NUOC_1_20240401]
                        ,[KENH_LOC_NUOC_2_20240401]
                        ,[KENH_LOC_NUOC_3_20240401]
                        ,[KENH_LOC_NUOC_4_20240401]
                        ,[KENH_LOC_NUOC_5_20240401]
                        ,[KENH_LOC_NUOC_6_20240401]
                        ,[KENH_LOC_NUOC_7_20240401]
                        ,[KENH_LOC_NUOC_8_20240401]
                        ,[KENH_LOC_NUOC_9_20240401]
                        ,[KENH_LOC_NUOC_10_20240401]
                        ,[TGLN_20240501]
                        ,[DROPPII_20240501]
                        ,[KENH_LOC_NUOC_1_20240501]
                        ,[KENH_LOC_NUOC_2_20240501]
                        ,[KENH_LOC_NUOC_3_20240501]
                        ,[KENH_LOC_NUOC_4_20240501]
                        ,[KENH_LOC_NUOC_5_20240501]
                        ,[KENH_LOC_NUOC_6_20240501]
                        ,[KENH_LOC_NUOC_7_20240501]
                        ,[KENH_LOC_NUOC_8_20240501]
                        ,[KENH_LOC_NUOC_9_20240501]
                        ,[KENH_LOC_NUOC_10_20240501]
                        ,[TGLN_20240601]
                        ,[DROPPII_20240601]
                        ,[KENH_LOC_NUOC_1_20240601]
                        ,[KENH_LOC_NUOC_2_20240601]
                        ,[KENH_LOC_NUOC_3_20240601]
                        ,[KENH_LOC_NUOC_4_20240601]
                        ,[KENH_LOC_NUOC_5_20240601]
                        ,[KENH_LOC_NUOC_6_20240601]
                        ,[KENH_LOC_NUOC_7_20240601]
                        ,[KENH_LOC_NUOC_8_20240601]
                        ,[KENH_LOC_NUOC_9_20240601]
                        ,[KENH_LOC_NUOC_10_20240601]
                        ,[TGLN_20240701]
                        ,[DROPPII_20240701]
                        ,[KENH_LOC_NUOC_1_20240701]
                        ,[KENH_LOC_NUOC_2_20240701]
                        ,[KENH_LOC_NUOC_3_20240701]
                        ,[KENH_LOC_NUOC_4_20240701]
                        ,[KENH_LOC_NUOC_5_20240701]
                        ,[KENH_LOC_NUOC_6_20240701]
                        ,[KENH_LOC_NUOC_7_20240701]
                        ,[KENH_LOC_NUOC_8_20240701]
                        ,[KENH_LOC_NUOC_9_20240701]
                        ,[KENH_LOC_NUOC_10_20240701]
                        ,[TGLN_20240801]
                        ,[DROPPII_20240801]
                        ,[KENH_LOC_NUOC_1_20240801]
                        ,[KENH_LOC_NUOC_2_20240801]
                        ,[KENH_LOC_NUOC_3_20240801]
                        ,[KENH_LOC_NUOC_4_20240801]
                        ,[KENH_LOC_NUOC_5_20240801]
                        ,[KENH_LOC_NUOC_6_20240801]
                        ,[KENH_LOC_NUOC_7_20240801]
                        ,[KENH_LOC_NUOC_8_20240801]
                        ,[KENH_LOC_NUOC_9_20240801]
                        ,[KENH_LOC_NUOC_10_20240801]
                        ,[TGLN_20240901]
                        ,[DROPPII_20240901]
                        ,[KENH_LOC_NUOC_1_20240901]
                        ,[KENH_LOC_NUOC_2_20240901]
                        ,[KENH_LOC_NUOC_3_20240901]
                        ,[KENH_LOC_NUOC_4_20240901]
                        ,[KENH_LOC_NUOC_5_20240901]
                        ,[KENH_LOC_NUOC_6_20240901]
                        ,[KENH_LOC_NUOC_7_20240901]
                        ,[KENH_LOC_NUOC_8_20240901]
                        ,[KENH_LOC_NUOC_9_20240901]
                        ,[KENH_LOC_NUOC_10_20240901]
                        ,[TGLN_20241001]
                        ,[DROPPII_20241001]
                        ,[KENH_LOC_NUOC_1_20241001]
                        ,[KENH_LOC_NUOC_2_20241001]
                        ,[KENH_LOC_NUOC_3_20241001]
                        ,[KENH_LOC_NUOC_4_20241001]
                        ,[KENH_LOC_NUOC_5_20241001]
                        ,[KENH_LOC_NUOC_6_20241001]
                        ,[KENH_LOC_NUOC_7_20241001]
                        ,[KENH_LOC_NUOC_8_20241001]
                        ,[KENH_LOC_NUOC_9_20241001]
                        ,[KENH_LOC_NUOC_10_20241001]
                        ,[TGLN_20241101]
                        ,[DROPPII_20241101]
                        ,[KENH_LOC_NUOC_1_20241101]
                        ,[KENH_LOC_NUOC_2_20241101]
                        ,[KENH_LOC_NUOC_3_20241101]
                        ,[KENH_LOC_NUOC_4_20241101]
                        ,[KENH_LOC_NUOC_5_20241101]
                        ,[KENH_LOC_NUOC_6_20241101]
                        ,[KENH_LOC_NUOC_7_20241101]
                        ,[KENH_LOC_NUOC_8_20241101]
                        ,[KENH_LOC_NUOC_9_20241101]
                        ,[KENH_LOC_NUOC_10_20241101]
                        ,[TGLN_20241201]
                        ,[DROPPII_20241201]
                        ,[KENH_LOC_NUOC_1_20241201]
                        ,[KENH_LOC_NUOC_2_20241201]
                        ,[KENH_LOC_NUOC_3_20241201]
                        ,[KENH_LOC_NUOC_4_20241201]
                        ,[KENH_LOC_NUOC_5_20241201]
                        ,[KENH_LOC_NUOC_6_20241201]
                        ,[KENH_LOC_NUOC_7_20241201]
                        ,[KENH_LOC_NUOC_8_20241201]
                        ,[KENH_LOC_NUOC_9_20241201]
                        ,[KENH_LOC_NUOC_10_20241201]
                        ,[TGLN_20250101]
                        ,[DROPPII_20250101]
                        ,[KENH_LOC_NUOC_1_20250101]
                        ,[KENH_LOC_NUOC_2_20250101]
                        ,[KENH_LOC_NUOC_3_20250101]
                        ,[KENH_LOC_NUOC_4_20250101]
                        ,[KENH_LOC_NUOC_5_20250101]
                        ,[KENH_LOC_NUOC_6_20250101]
                        ,[KENH_LOC_NUOC_7_20250101]
                        ,[KENH_LOC_NUOC_8_20250101]
                        ,[KENH_LOC_NUOC_9_20250101]
                        ,[KENH_LOC_NUOC_10_20250101]
                        ,[TGLN_20250201]
                        ,[DROPPII_20250201]
                        ,[KENH_LOC_NUOC_1_20250201]
                        ,[KENH_LOC_NUOC_2_20250201]
                        ,[KENH_LOC_NUOC_3_20250201]
                        ,[KENH_LOC_NUOC_4_20250201]
                        ,[KENH_LOC_NUOC_5_20250201]
                        ,[KENH_LOC_NUOC_6_20250201]
                        ,[KENH_LOC_NUOC_7_20250201]
                        ,[KENH_LOC_NUOC_8_20250201]
                        ,[KENH_LOC_NUOC_9_20250201]
                        ,[KENH_LOC_NUOC_10_20250201]
                        ,[TGLN_20250301]
                        ,[DROPPII_20250301]
                        ,[KENH_LOC_NUOC_1_20250301]
                        ,[KENH_LOC_NUOC_2_20250301]
                        ,[KENH_LOC_NUOC_3_20250301]
                        ,[KENH_LOC_NUOC_4_20250301]
                        ,[KENH_LOC_NUOC_5_20250301]
                        ,[KENH_LOC_NUOC_6_20250301]
                        ,[KENH_LOC_NUOC_7_20250301]
                        ,[KENH_LOC_NUOC_8_20250301]
                        ,[KENH_LOC_NUOC_9_20250301]
                        ,[KENH_LOC_NUOC_10_20250301]
                        ,[TGLN_20250401]
                        ,[DROPPII_20250401]
                        ,[KENH_LOC_NUOC_1_20250401]
                        ,[KENH_LOC_NUOC_2_20250401]
                        ,[KENH_LOC_NUOC_3_20250401]
                        ,[KENH_LOC_NUOC_4_20250401]
                        ,[KENH_LOC_NUOC_5_20250401]
                        ,[KENH_LOC_NUOC_6_20250401]
                        ,[KENH_LOC_NUOC_7_20250401]
                        ,[KENH_LOC_NUOC_8_20250401]
                        ,[KENH_LOC_NUOC_9_20250401]
                        ,[KENH_LOC_NUOC_10_20250401]
                        ,[TGLN_20250501]
                        ,[DROPPII_20250501]
                        ,[KENH_LOC_NUOC_1_20250501]
                        ,[KENH_LOC_NUOC_2_20250501]
                        ,[KENH_LOC_NUOC_3_20250501]
                        ,[KENH_LOC_NUOC_4_20250501]
                        ,[KENH_LOC_NUOC_5_20250501]
                        ,[KENH_LOC_NUOC_6_20250501]
                        ,[KENH_LOC_NUOC_7_20250501]
                        ,[KENH_LOC_NUOC_8_20250501]
                        ,[KENH_LOC_NUOC_9_20250501]
                        ,[KENH_LOC_NUOC_10_20250501]
                        ,[TGLN_20250601]
                        ,[DROPPII_20250601]
                        ,[KENH_LOC_NUOC_1_20250601]
                        ,[KENH_LOC_NUOC_2_20250601]
                        ,[KENH_LOC_NUOC_3_20250601]
                        ,[KENH_LOC_NUOC_4_20250601]
                        ,[KENH_LOC_NUOC_5_20250601]
                        ,[KENH_LOC_NUOC_6_20250601]
                        ,[KENH_LOC_NUOC_7_20250601]
                        ,[KENH_LOC_NUOC_8_20250601]
                        ,[KENH_LOC_NUOC_9_20250601]
                        ,[KENH_LOC_NUOC_10_20250601]
                        ,[TGLN_20250701]
                        ,[DROPPII_20250701]
                        ,[KENH_LOC_NUOC_1_20250701]
                        ,[KENH_LOC_NUOC_2_20250701]
                        ,[KENH_LOC_NUOC_3_20250701]
                        ,[KENH_LOC_NUOC_4_20250701]
                        ,[KENH_LOC_NUOC_5_20250701]
                        ,[KENH_LOC_NUOC_6_20250701]
                        ,[KENH_LOC_NUOC_7_20250701]
                        ,[KENH_LOC_NUOC_8_20250701]
                        ,[KENH_LOC_NUOC_9_20250701]
                        ,[KENH_LOC_NUOC_10_20250701]
                        ,[TGLN_20250801]
                        ,[DROPPII_20250801]
                        ,[KENH_LOC_NUOC_1_20250801]
                        ,[KENH_LOC_NUOC_2_20250801]
                        ,[KENH_LOC_NUOC_3_20250801]
                        ,[KENH_LOC_NUOC_4_20250801]
                        ,[KENH_LOC_NUOC_5_20250801]
                        ,[KENH_LOC_NUOC_6_20250801]
                        ,[KENH_LOC_NUOC_7_20250801]
                        ,[KENH_LOC_NUOC_8_20250801]
                        ,[KENH_LOC_NUOC_9_20250801]
                        ,[KENH_LOC_NUOC_10_20250801]
                        ,[TGLN_20250901]
                        ,[DROPPII_20250901]
                        ,[KENH_LOC_NUOC_1_20250901]
                        ,[KENH_LOC_NUOC_2_20250901]
                        ,[KENH_LOC_NUOC_3_20250901]
                        ,[KENH_LOC_NUOC_4_20250901]
                        ,[KENH_LOC_NUOC_5_20250901]
                        ,[KENH_LOC_NUOC_6_20250901]
                        ,[KENH_LOC_NUOC_7_20250901]
                        ,[KENH_LOC_NUOC_8_20250901]
                        ,[KENH_LOC_NUOC_9_20250901]
                        ,[KENH_LOC_NUOC_10_20250901]
                        ,[TGLN_20251001]
                        ,[DROPPII_20251001]
                        ,[KENH_LOC_NUOC_1_20251001]
                        ,[KENH_LOC_NUOC_2_20251001]
                        ,[KENH_LOC_NUOC_3_20251001]
                        ,[KENH_LOC_NUOC_4_20251001]
                        ,[KENH_LOC_NUOC_5_20251001]
                        ,[KENH_LOC_NUOC_6_20251001]
                        ,[KENH_LOC_NUOC_7_20251001]
                        ,[KENH_LOC_NUOC_8_20251001]
                        ,[KENH_LOC_NUOC_9_20251001]
                        ,[KENH_LOC_NUOC_10_20251001]
                        ,[TGLN_20251101]
                        ,[DROPPII_20251101]
                        ,[KENH_LOC_NUOC_1_20251101]
                        ,[KENH_LOC_NUOC_2_20251101]
                        ,[KENH_LOC_NUOC_3_20251101]
                        ,[KENH_LOC_NUOC_4_20251101]
                        ,[KENH_LOC_NUOC_5_20251101]
                        ,[KENH_LOC_NUOC_6_20251101]
                        ,[KENH_LOC_NUOC_7_20251101]
                        ,[KENH_LOC_NUOC_8_20251101]
                        ,[KENH_LOC_NUOC_9_20251101]
                        ,[KENH_LOC_NUOC_10_20251101]
                        ,[TGLN_20251201]
                        ,[DROPPII_20251201]
                        ,[KENH_LOC_NUOC_1_20251201]
                        ,[KENH_LOC_NUOC_2_20251201]
                        ,[KENH_LOC_NUOC_3_20251201]
                        ,[KENH_LOC_NUOC_4_20251201]
                        ,[KENH_LOC_NUOC_5_20251201]
                        ,[KENH_LOC_NUOC_6_20251201]
                        ,[KENH_LOC_NUOC_7_20251201]
                        ,[KENH_LOC_NUOC_8_20251201]
                        ,[KENH_LOC_NUOC_9_20251201]
                        ,[KENH_LOC_NUOC_10_20251201])
                VALUES  (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_LocNuoc]",
                    sheet_name="Kế hoạch vận hành BH Lọc nước", skip_rows=1)

    @task
    def insert_kehoach_vanhanh_BH_BCA():
        file_local = get_url()
        sql = """
                INSERT INTO [dbo].[KeHoach_VanHanh_BH_BCA]
                        ([MA_HANG_HOA]
                        ,[TEN_HANG_HOA]
                        ,[DROPPII_ABENA_20240101]
                        ,[KENH_BCA_1_20240101]
                        ,[KENH_BCA_2_20240101]
                        ,[KENH_BCA_3_20240101]
                        ,[KENH_BCA_4_20240101]
                        ,[KENH_BCA_5_20240101]
                        ,[KENH_BCA_6_20240101]
                        ,[KENH_BCA_7_20240101]
                        ,[KENH_BCA_8_20240101]
                        ,[KENH_BCA_9_20240101]
                        ,[KENH_BCA_10_20240101]
                        ,[DROPPII_ABENA_20240201]
                        ,[KENH_BCA_1_20240201]
                        ,[KENH_BCA_2_20240201]
                        ,[KENH_BCA_3_20240201]
                        ,[KENH_BCA_4_20240201]
                        ,[KENH_BCA_5_20240201]
                        ,[KENH_BCA_6_20240201]
                        ,[KENH_BCA_7_20240201]
                        ,[KENH_BCA_8_20240201]
                        ,[KENH_BCA_9_20240201]
                        ,[KENH_BCA_10_20240201]
                        ,[DROPPII_ABENA_20240301]
                        ,[KENH_BCA_1_20240301]
                        ,[KENH_BCA_2_20240301]
                        ,[KENH_BCA_3_20240301]
                        ,[KENH_BCA_4_20240301]
                        ,[KENH_BCA_5_20240301]
                        ,[KENH_BCA_6_20240301]
                        ,[KENH_BCA_7_20240301]
                        ,[KENH_BCA_8_20240301]
                        ,[KENH_BCA_9_20240301]
                        ,[KENH_BCA_10_20240301]
                        ,[DROPPII_ABENA_20240401]
                        ,[KENH_BCA_1_20240401]
                        ,[KENH_BCA_2_20240401]
                        ,[KENH_BCA_3_20240401]
                        ,[KENH_BCA_4_20240401]
                        ,[KENH_BCA_5_20240401]
                        ,[KENH_BCA_6_20240401]
                        ,[KENH_BCA_7_20240401]
                        ,[KENH_BCA_8_20240401]
                        ,[KENH_BCA_9_20240401]
                        ,[KENH_BCA_10_20240401]
                        ,[DROPPII_ABENA_20240501]
                        ,[KENH_BCA_1_20240501]
                        ,[KENH_BCA_2_20240501]
                        ,[KENH_BCA_3_20240501]
                        ,[KENH_BCA_4_20240501]
                        ,[KENH_BCA_5_20240501]
                        ,[KENH_BCA_6_20240501]
                        ,[KENH_BCA_7_20240501]
                        ,[KENH_BCA_8_20240501]
                        ,[KENH_BCA_9_20240501]
                        ,[KENH_BCA_10_20240501]
                        ,[DROPPII_ABENA_20240601]
                        ,[KENH_BCA_1_20240601]
                        ,[KENH_BCA_2_20240601]
                        ,[KENH_BCA_3_20240601]
                        ,[KENH_BCA_4_20240601]
                        ,[KENH_BCA_5_20240601]
                        ,[KENH_BCA_6_20240601]
                        ,[KENH_BCA_7_20240601]
                        ,[KENH_BCA_8_20240601]
                        ,[KENH_BCA_9_20240601]
                        ,[KENH_BCA_10_20240601]
                        ,[DROPPII_ABENA_20240701]
                        ,[KENH_BCA_1_20240701]
                        ,[KENH_BCA_2_20240701]
                        ,[KENH_BCA_3_20240701]
                        ,[KENH_BCA_4_20240701]
                        ,[KENH_BCA_5_20240701]
                        ,[KENH_BCA_6_20240701]
                        ,[KENH_BCA_7_20240701]
                        ,[KENH_BCA_8_20240701]
                        ,[KENH_BCA_9_20240701]
                        ,[KENH_BCA_10_20240701]
                        ,[DROPPII_ABENA_20240801]
                        ,[KENH_BCA_1_20240801]
                        ,[KENH_BCA_2_20240801]
                        ,[KENH_BCA_3_20240801]
                        ,[KENH_BCA_4_20240801]
                        ,[KENH_BCA_5_20240801]
                        ,[KENH_BCA_6_20240801]
                        ,[KENH_BCA_7_20240801]
                        ,[KENH_BCA_8_20240801]
                        ,[KENH_BCA_9_20240801]
                        ,[KENH_BCA_10_20240801]
                        ,[DROPPII_ABENA_20240901]
                        ,[KENH_BCA_1_20240901]
                        ,[KENH_BCA_2_20240901]
                        ,[KENH_BCA_3_20240901]
                        ,[KENH_BCA_4_20240901]
                        ,[KENH_BCA_5_20240901]
                        ,[KENH_BCA_6_20240901]
                        ,[KENH_BCA_7_20240901]
                        ,[KENH_BCA_8_20240901]
                        ,[KENH_BCA_9_20240901]
                        ,[KENH_BCA_10_20240901]
                        ,[DROPPII_ABENA_20241001]
                        ,[KENH_BCA_1_20241001]
                        ,[KENH_BCA_2_20241001]
                        ,[KENH_BCA_3_20241001]
                        ,[KENH_BCA_4_20241001]
                        ,[KENH_BCA_5_20241001]
                        ,[KENH_BCA_6_20241001]
                        ,[KENH_BCA_7_20241001]
                        ,[KENH_BCA_8_20241001]
                        ,[KENH_BCA_9_20241001]
                        ,[KENH_BCA_10_20241001]
                        ,[DROPPII_ABENA_20241101]
                        ,[KENH_BCA_1_20241101]
                        ,[KENH_BCA_2_20241101]
                        ,[KENH_BCA_3_20241101]
                        ,[KENH_BCA_4_20241101]
                        ,[KENH_BCA_5_20241101]
                        ,[KENH_BCA_6_20241101]
                        ,[KENH_BCA_7_20241101]
                        ,[KENH_BCA_8_20241101]
                        ,[KENH_BCA_9_20241101]
                        ,[KENH_BCA_10_20241101]
                        ,[DROPPII_ABENA_20241201]
                        ,[KENH_BCA_1_20241201]
                        ,[KENH_BCA_2_20241201]
                        ,[KENH_BCA_3_20241201]
                        ,[KENH_BCA_4_20241201]
                        ,[KENH_BCA_5_20241201]
                        ,[KENH_BCA_6_20241201]
                        ,[KENH_BCA_7_20241201]
                        ,[KENH_BCA_8_20241201]
                        ,[KENH_BCA_9_20241201]
                        ,[KENH_BCA_10_20241201]
                        ,[DROPPII_ABENA_20250101]
                        ,[KENH_BCA_1_20250101]
                        ,[KENH_BCA_2_20250101]
                        ,[KENH_BCA_3_20250101]
                        ,[KENH_BCA_4_20250101]
                        ,[KENH_BCA_5_20250101]
                        ,[KENH_BCA_6_20250101]
                        ,[KENH_BCA_7_20250101]
                        ,[KENH_BCA_8_20250101]
                        ,[KENH_BCA_9_20250101]
                        ,[KENH_BCA_10_20250101]
                        ,[DROPPII_ABENA_20250201]
                        ,[KENH_BCA_1_20250201]
                        ,[KENH_BCA_2_20250201]
                        ,[KENH_BCA_3_20250201]
                        ,[KENH_BCA_4_20250201]
                        ,[KENH_BCA_5_20250201]
                        ,[KENH_BCA_6_20250201]
                        ,[KENH_BCA_7_20250201]
                        ,[KENH_BCA_8_20250201]
                        ,[KENH_BCA_9_20250201]
                        ,[KENH_BCA_10_20250201]
                        ,[DROPPII_ABENA_20250301]
                        ,[KENH_BCA_1_20250301]
                        ,[KENH_BCA_2_20250301]
                        ,[KENH_BCA_3_20250301]
                        ,[KENH_BCA_4_20250301]
                        ,[KENH_BCA_5_20250301]
                        ,[KENH_BCA_6_20250301]
                        ,[KENH_BCA_7_20250301]
                        ,[KENH_BCA_8_20250301]
                        ,[KENH_BCA_9_20250301]
                        ,[KENH_BCA_10_20250301]
                        ,[DROPPII_ABENA_20250401]
                        ,[KENH_BCA_1_20250401]
                        ,[KENH_BCA_2_20250401]
                        ,[KENH_BCA_3_20250401]
                        ,[KENH_BCA_4_20250401]
                        ,[KENH_BCA_5_20250401]
                        ,[KENH_BCA_6_20250401]
                        ,[KENH_BCA_7_20250401]
                        ,[KENH_BCA_8_20250401]
                        ,[KENH_BCA_9_20250401]
                        ,[KENH_BCA_10_20250401]
                        ,[DROPPII_ABENA_20250501]
                        ,[KENH_BCA_1_20250501]
                        ,[KENH_BCA_2_20250501]
                        ,[KENH_BCA_3_20250501]
                        ,[KENH_BCA_4_20250501]
                        ,[KENH_BCA_5_20250501]
                        ,[KENH_BCA_6_20250501]
                        ,[KENH_BCA_7_20250501]
                        ,[KENH_BCA_8_20250501]
                        ,[KENH_BCA_9_20250501]
                        ,[KENH_BCA_10_20250501]
                        ,[DROPPII_ABENA_20250601]
                        ,[KENH_BCA_1_20250601]
                        ,[KENH_BCA_2_20250601]
                        ,[KENH_BCA_3_20250601]
                        ,[KENH_BCA_4_20250601]
                        ,[KENH_BCA_5_20250601]
                        ,[KENH_BCA_6_20250601]
                        ,[KENH_BCA_7_20250601]
                        ,[KENH_BCA_8_20250601]
                        ,[KENH_BCA_9_20250601]
                        ,[KENH_BCA_10_20250601]
                        ,[DROPPII_ABENA_20250701]
                        ,[KENH_BCA_1_20250701]
                        ,[KENH_BCA_2_20250701]
                        ,[KENH_BCA_3_20250701]
                        ,[KENH_BCA_4_20250701]
                        ,[KENH_BCA_5_20250701]
                        ,[KENH_BCA_6_20250701]
                        ,[KENH_BCA_7_20250701]
                        ,[KENH_BCA_8_20250701]
                        ,[KENH_BCA_9_20250701]
                        ,[KENH_BCA_10_20250701]
                        ,[DROPPII_ABENA_20250801]
                        ,[KENH_BCA_1_20250801]
                        ,[KENH_BCA_2_20250801]
                        ,[KENH_BCA_3_20250801]
                        ,[KENH_BCA_4_20250801]
                        ,[KENH_BCA_5_20250801]
                        ,[KENH_BCA_6_20250801]
                        ,[KENH_BCA_7_20250801]
                        ,[KENH_BCA_8_20250801]
                        ,[KENH_BCA_9_20250801]
                        ,[KENH_BCA_10_20250801]
                        ,[DROPPII_ABENA_20250901]
                        ,[KENH_BCA_1_20250901]
                        ,[KENH_BCA_2_20250901]
                        ,[KENH_BCA_3_20250901]
                        ,[KENH_BCA_4_20250901]
                        ,[KENH_BCA_5_20250901]
                        ,[KENH_BCA_6_20250901]
                        ,[KENH_BCA_7_20250901]
                        ,[KENH_BCA_8_20250901]
                        ,[KENH_BCA_9_20250901]
                        ,[KENH_BCA_10_20250901]
                        ,[DROPPII_ABENA_20251001]
                        ,[KENH_BCA_1_20251001]
                        ,[KENH_BCA_2_20251001]
                        ,[KENH_BCA_3_20251001]
                        ,[KENH_BCA_4_20251001]
                        ,[KENH_BCA_5_20251001]
                        ,[KENH_BCA_6_20251001]
                        ,[KENH_BCA_7_20251001]
                        ,[KENH_BCA_8_20251001]
                        ,[KENH_BCA_9_20251001]
                        ,[KENH_BCA_10_20251001]
                        ,[DROPPII_ABENA_20251101]
                        ,[KENH_BCA_1_20251101]
                        ,[KENH_BCA_2_20251101]
                        ,[KENH_BCA_3_20251101]
                        ,[KENH_BCA_4_20251101]
                        ,[KENH_BCA_5_20251101]
                        ,[KENH_BCA_6_20251101]
                        ,[KENH_BCA_7_20251101]
                        ,[KENH_BCA_8_20251101]
                        ,[KENH_BCA_9_20251101]
                        ,[KENH_BCA_10_20251101]
                        ,[DROPPII_ABENA_20251201]
                        ,[KENH_BCA_1_20251201]
                        ,[KENH_BCA_2_20251201]
                        ,[KENH_BCA_3_20251201]
                        ,[KENH_BCA_4_20251201]
                        ,[KENH_BCA_5_20251201]
                        ,[KENH_BCA_6_20251201]
                        ,[KENH_BCA_7_20251201]
                        ,[KENH_BCA_8_20251201]
                        ,[KENH_BCA_9_20251201]
                        ,[KENH_BCA_10_20251201])
                VALUES  (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_BCA]",
                    sheet_name="Kế hoạch vận hành BH BCA", skip_rows=1)

    @task
    def insert_kehoach_BH_Thang():
        file_local = get_url()
        sql = """
                INSERT INTO [dbo].[KeHoach_VanHanh_BH_Thang]
                        ([MA_NHAN_VIEN]
                        ,[TEN_NVKD]
                        ,[NHOM_NGANH_HANG]
                        ,[20240701]
                        ,[20240801]
                        ,[20240901]
                        ,[20241001]
                        ,[20241101]
                        ,[20241201]
                        ,[20250101]
                        ,[20250201]
                        ,[20250301]
                        ,[20250401]
                        ,[20250501]
                        ,[20250601]
                        ,[20250701]
                        ,[20250801]
                        ,[20250901]
                        ,[20251001]
                        ,[20251101]
                        ,[20251201])
                VALUES  (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_VanHanh_BH_Thang]",
                    sheet_name="Kế hoạch BH tháng")

    @task
    def insert_kehoach_Kenh_DonVi():
        file_local = get_url()
        sql = """
                INSERT INTO [dbo].[KeHoach_Kenh_DonVi_NVKD]
                        ([MA_KHACH_HANG]
                        ,[TEN_KHACH_HANG]
                        ,[PHONG_BAN]
                        ,[KENH]
                        ,[MA_NHAN_VIEN]
                        ,[TEN_NHAN_VIEN])
                VALUES  (
                    %s, %s, %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[KeHoach_Kenh_DonVi_NVKD]",
                    sheet_name="KH, kênh, đơn vị NVKD ")

    @task
    def insert_sanpham_NhomSP_Brand():
        file_local = get_url()
        sql = """
                INSERT INTO [dbo].[SanPham_NhomSP_Brand]
                    ([MA_HANG_HOA]
                    ,[HANG_HOA]
                    ,[NHAN_HANG]
                    ,[NHOM_NHAN_HANG]
                    ,[GIA_DAI_LY]
                    ,[HAN_SU_DUNG] -- THOI_HAN_SU_DUNG
                    ,[GIA_TT]
                    ,[GIA_MT]
                    ,[GIA_LOC_NUOC]
                    ,[GIA_BCA]
                    ,[GIA_ONLINE]
                    ,[GIA_NIEM_YET])
                VALUES  (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
        """
        insert_data(path_file=file_local, query=sql, table_name="[dbo].[SanPham_NhomSP_Brand]",
                    sheet_name="Sản phẩm, nhóm sản phẩm, brand")

    ############ DAG FLOW ############

    download_latest_file() >> insert_kehoach_nam() >> insert_kehoach_vanhanh_BH_MT() >> insert_kehoach_vanhanh_BH_TT(
    ) >> insert_kehoach_vanhanh_BH_Online() >> insert_kehoach_vanhanh_BH_LocNuoc(
    ) >> insert_kehoach_vanhanh_BH_BCA() >> insert_kehoach_BH_Thang() >> insert_kehoach_Kenh_DonVi() >> insert_sanpham_NhomSP_Brand() >> move_files()


dag = Ke_hoach_TTV()
