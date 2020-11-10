import os, uuid, sys
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.storage.filedatalake import FileSystemClient

def establish_connection():
    global service
    global connection_string
    connection_string='DefaultEndpointsProtocol=https;AccountName=lsampayo;AccountKey=FsaCGRDZiQ+a972WS6NJ9+Gq7THCW/CKZHIfdtCfrPBqRMGsj419ZdWZAzPXillaiVtI9Ks/r2GEvjYWcgYVxQ==;EndpointSuffix=core.windows.net'
    service = DataLakeServiceClient.from_connection_string(conn_str=connection_string)

def create_file_system():
    try:
        global file_system_client

        file_system_client = service.create_file_system(file_system="my-file-system")
    
    except Exception as e:
        print(e) 

def create_directory(directory):
    file_system_client = FileSystemClient.from_connection_string(connection_string, file_system_name="my-file-system")
    try:
        file_system_client.create_directory(directory)
    
    except Exception as e:
     print(e) 

def delete_file_in_directory(fileP):
    file = DataLakeFileClient.from_connection_string(connection_string, 
                                                 file_system_name="my-file-system", file_path=fileP)
    
    try:
        file.get_file_properties()
        file.delete_file()
    except Exception as error:
        print(error)    
        if type(error).__name__ =='ResourceNotFoundError':
            print("the path does not exist: ", fileP)



def upload_file_to_directory(file, created_file, directory):
    try:

        file_system_client = service.get_file_system_client(file_system="my-file-system")

        directory_client = file_system_client.get_directory_client(directory)
        
        file_client = directory_client.create_file(created_file)
        local_file = open(file,'rb')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e) 

establish_connection()

delete_file_in_directory("/data/input/erp/products.csv")
delete_file_in_directory("/data/input/erp/calories.csv")
delete_file_in_directory("/data/input/erp/export.csv")
delete_file_in_directory("/data/input/teinvento.inc/metricsales.csv")

#create_file_system()
#create_directory("/data/input/erp/")
#create_directory("/data/input/teinvento.inc/")

#create_directory("/data/crudo/generador/fuente/empty")
#create_directory("/data/procesado/generador/fuente/empty")    

upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\products.csv", "products.csv", "/data/input/erp")
upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\calories.csv", "calories.csv", "/data/input/erp")
upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\export.csv", "export.csv", "/data/input/erp")
upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\teinvento.inc\\metricsales.csv", "metricsales.csv", "/data/input/teinvento.inc")

print("Done")
