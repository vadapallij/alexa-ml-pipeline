import os
from azure.storage.filedatalake import DataLakeServiceClient

def _dls_client(account_name: str, sas_token: str):
    url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=url, credential=sas_token)

def upload_file_to_adls(local_path: str, account_name: str, container: str, dest_path: str, sas_token: str):
    svc = _dls_client(account_name, sas_token)
    fs = svc.get_file_system_client(file_system=container)
    # Create directories as needed
    dir_path = "/".join(dest_path.split("/")[:-1])
    if dir_path:
        try:
            fs.create_directory(dir_path)
        except Exception:
            pass
    file_client = fs.get_file_client(dest_path)
    with open(local_path, "rb") as f:
        data = f.read()
    try:
        file_client.create_file()
    except Exception:
        pass
    file_client.upload_data(data, overwrite=True)
    return f"abfss://{container}@{account_name}.dfs.core.windows.net/{dest_path}"
