import airflow.providers.microsoft.mssql.hooks.mssql as mssql
from common.variables import MSSQL_CONNECTION

hook = mssql.MsSqlHook(MSSQL_CONNECTION)
