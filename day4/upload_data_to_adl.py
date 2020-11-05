import os
from azure.storage.filedatalake import DataLakeServiceClient


def upload_file_to_directory(file_system_client, directory, client_file_name, local_file_path):
    try:
        directory_client = file_system_client.get_directory_client(directory)
        file_client = directory_client.create_file(client_file_name)
        local_file = open(local_file_path)
        file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print(f"File '{client_file_name}' was uploaded into '{directory}'")

    except Exception as e:
        print(e)

try:
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", "lesson02str02"), credential="##################")

    file_system_client = service_client.create_file_system(file_system="nyt")

    upload_file_to_directory(file_system_client, '/',
                             'yellow_tripdata_2020-01.csv', 'yellow_tripdata_2020-01.csv')

except Exception as e:
    print(e)
