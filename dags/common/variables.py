from airflow.models import Variable

# Constanst Definitions
############################################## FILLOUT ##############################################
FILLOUT_GET_FORM = Variable.get('fillout_get_form')
FILLOUT_GET_FORM_METADATA = Variable.get('fillout_get_form_metadata')
FILLOUT_GET_ALL_SUBMISSIONS = Variable.get('fillout_get_all_submissions')
FILLOUT_GET_SUBMISSION_BY_ID = Variable.get('fillout_get_submission_by_id')
FILLOUT_CREATE_WEBHOOK = Variable.get('fillout_create_webhook')
FILLOUT_REMOVE_WEBHOOK = Variable.get('fillout_remove_webhook')

############################################## CLICKUP ##############################################
CLICKUP_GET_SPACE_IDS = Variable.get('clickup_get_space_ids')
CLICKUP_GET_SPACES = Variable.get('clickup_get_spaces')
CLICKUP_GET_SPACE_DETAILS = Variable.get('clickup_get_space_details')
CLICKUP_GET_TASKS = Variable.get('clickup_get_tasks')
CLICKUP_GET_TASK_DETAILS = Variable.get('clickup_get_task_details')
CLICKUP_GET_CUSTOM_FIELDS = Variable.get('clickup_get_custom_fields')
CLICKUP_GET_FOLDERS = Variable.get('clickup_get_folders')
CLICKUP_GET_FOLDER_DETAILS = Variable.get('clickup_get_folder_details')
CLICKUP_GET_LISTS = Variable.get('clickup_get_lists')
CLICKUP_GET_LIST_DETAILS = Variable.get('clickup_get_list_details')
CLICKUP_DELETE_TASK = Variable.get('clickup_delete_task')
CLICKUP_CREATE_TASK = Variable.get('clickup_create_task')
CLICKUP_GET_LISTS_FOLDERLESS = Variable.get('clickup_get_lists_folderless')
CLICKUP_ATTACHMENT = Variable.get('clickup_attachment')
CLICKUP_COMMENT = Variable.get('clickup_comment')
STATUS_CLICKUP_SALES = Variable.get('status_clickup_sales')
STATUS_CLICKUP_PURCHASE = Variable.get('status_clickup_purchase')

############################################## JOTFORM ##############################################

JOTFORM_GET_USER = Variable.get('jotform_get_user')
JOTFORM_GET_USER_USAGE = Variable.get('jotform_get_user_usage')
JOTFORM_GET_USER_SUBMISSIONS = Variable.get('jotform_get_user_submissions')
JOTFORM_GET_USER_FOLDERS = Variable.get('jotform_get_user_folders')
JOTFORM_GET_USER_REPORTS = Variable.get('jotform_get_user_reports')
JOTFORM_GET_USER_FORMS = Variable.get('jotform_get_user_forms')
JOTFORM_GET_FORM_BY_ID = Variable.get('jotform_get_form_by_id')
JOTFORM_GET_FORM_BY_ID_QUESTIONS = Variable.get(
    'jotform_get_form_by_id_questions')
JOTFORM_GET_FORM_BY_ID_QUESTIONS_BY_QID = Variable.get(
    'jotform_get_form_by_id_questions_by_qid')
JOTFORM_GET_FORM_PROPERTIES_BY_ID = Variable.get(
    'jotform_get_form_properties_by_id')
JOTFORM_GET_FORM_PROPERTIES_BY_ID_BY_KEY = Variable.get(
    'jotform_get_form_properties_by_id_by_key')
JOTFORM_GET_FORM_REPORTS_BY_ID = Variable.get('jotform_get_form_reports_by_id')
JOTFORM_GET_FORM_FILES_BY_ID = Variable.get('jotform_get_form_files_by_id')
JOTFORM_GET_FORM_WEBHOOKS_BY_ID = Variable.get(
    'jotform_get_form_webhooks_by_id')
JOTFORM_GET_FORM_SUBMISSIONS_BY_ID = Variable.get(
    'jotform_get_form_submissions_by_id')
JOTFORM_GET_FORM_SUBMISSIONS_BY_SUBID = Variable.get(
    'jotform_get_form_submissions_by_subid')
JOTFORM_GET_REPORT_BY_ID = Variable.get('jotform_get_report_by_id')
JOTFORM_GET_FOLDER_BY_ID = Variable.get('jotform_get_folder_by_id')
ID_JOTFORM_SUBMISSION = Variable.get('id_jotform_submission')

############################################## KEY ##############################################

API_KEY = Variable.get('api_key')
API_KEY_JOTFORM = Variable.get('api_key_jotform')
API_TOKEN_NIKO = Variable.get('api_token_niko')
FOLDER_ID_ROOT = Variable.get('folder_id_root')
ID_CLICKUP_SPACE_BAN_HANG = Variable.get('id_clickup_space_ban_hang')

############################################## ONEDRIVE ##############################################

CLIENT_ID = Variable.get('onedrive_client_id')
CLIENT_SECRET = Variable.get('onedrive_client_secret')
TENANT_ID = Variable.get('onedrive_tenant_id')
USER_ID = Variable.get('onedrive_user_id')
FILE_ID = Variable.get('onedrive_file_id')

############################################## MISA ##############################################

MISA_APP_ID = Variable.get('misa_app_id')
MISA_ACCESS_CODE = Variable.get('misa_access_code')
MISA_API_DANHMUC = Variable.get('misa_api_danhmuc')
MISA_API_TONKHO = Variable.get('misa_api_tonkho')
MISA_API_GET_TOKEN = Variable.get('misa_api_get_token')
MISA_API_CONGNO = Variable.get('misa_api_congno')
MISA_LUU_DON_HANG = Variable.get('misa_luu_don_hang')

MISA_CRM_DON_HANG_API = Variable.get('misa_crm_don_hang')
MISA_CRM_CLIENT_SECRET = Variable.get('misa_crm_client_secret')
MISA_CRM_CLIENT_ID = Variable.get('misa_crm_client_id')
MISA_CRM_GET_TOKEN = Variable.get('misa_crm_token')
MISA_CRM_SAN_PHAM_API = Variable.get('misa_crm_san_pham')
############################################## FOLDER FILES DRIVER ##############################################

FOLDER_ID_SOCHITIETBANHANG = Variable.get('folder_id_sochitietbanhang')
FOLDER_ID_KEHOACHBANHANG = Variable.get('folder_id_kehoachbanhang')
FOLDER_ID_SANPHAM = Variable.get('folder_id_sanpham')
FOLDER_ID_TONKHO = Variable.get('folder_id_tonkho')
FOLDER_ID_SANPHAM_NHOM = Variable.get('folder_id_sanpham_nhom')
FOLDER_ID_CONGNO = Variable.get('folder_id_congno')
FOLDER_ID_BANHANG = Variable.get('folder_id_banhang')
FOLDER_ID_KHACHHANG = Variable.get('folder_id_khachhang')
# URL
URL_DRIVER_REQUESTS = Variable.get('url_driver_requests')
URL_DRIVER_DOWNLOAD = Variable.get('url_driver_download')
############################################## NHANH.VN ##############################################

NHANH_BUSINESSID = Variable.get('nhanh_businessId')
NHANH_APPID = Variable.get('nhanh_appId')
NHANH_SECRETKEY = Variable.get('nhanh_secretkey')
NHANH_TOKEN = Variable.get('nhanh_token')
NHANH_GET_LIST_ORDERS = Variable.get('nhanh_get_list_orders')

############################################## AIRFLOW ##############################################

AIRFLOW_USER = Variable.get('airflow_user')
AIRFLOW_PASSWORD = Variable.get('airflow_password')
TEMP_PATH = Variable.get('temp_path')

MSSQL_CONNECTION = Variable.get('mssql_connection')
BEARER_TOKEN = Variable.get('bearer_token')
API_TOKEN = Variable.get('api_token')
MISA_TOKEN = Variable.get("misa_token")
