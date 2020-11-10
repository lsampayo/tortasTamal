#This is the main program for uploading csv files to cloud storage
#Author: Luis Enrique Marquez
#Date: Nov 8th, 2020
#Parameters: {period} YYYYmmdd, {source_path} where csv files are located
#Example: python upload2.py 20200801 .\\data\\source

import os, uuid, sys
from os.path import expanduser
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

def main(argv):

    print("Args:", argv)
    establish_connection()

    #Traverse folders in order to get source csv files to be uploaded
    path_tamales_inc = argv[1] + ".\\tamales_inc\\ventas_mensuales_tamales_inc"
    path_teinvento_inc = argv[1] + ".\\teinvento_inc\\ventas_reportadas_mercado_tamales"
    period = argv[0]

    tamales_inc_structure = []
    teinvento_inc_structure = []

    for dirpath, dirs, files in os.walk(path_tamales_inc):
	    tamales_inc_structure.append(dirs)

    for dirpath, dirs, files in os.walk(path_teinvento_inc):
	    teinvento_inc_structure.append(dirs)

    tamales_inc_countries = tamales_inc_structure[0]
    teinvento_inc_countries = teinvento_inc_structure[0]

    for country in tamales_inc_countries:
        country_path=path_tamales_inc + "\\" + country
        if (period in tamales_inc_structure[1]):
            print("Period was found for tamales_inc")
            create_directory("/data2/input/tamales_inc/" + country + "/" + period + "/empty")
            fname = []
            fnameonly = []
            for root,d_names,f_names in os.walk(country_path+"\\"+period):
                for f in f_names:
                    fname.append(os.path.join(root, f))
                    fnameonly.append(f)
            
            counter = 0
            for filesource in fname:
                print(filesource)
                upload_file_to_directory(filesource, fnameonly[counter], "/data2/input/tamales_inc/" + country + "/" + period)
                counter=counter+1

    for country in teinvento_inc_countries:
        country_path=path_teinvento_inc + "\\" + country
        if (period in teinvento_inc_structure[1]):
            print("Period was found for teinvento_inc")
            create_directory("/data2/input/teinvento_inc/" + country + "/" + period + "/empty")
            fname = []
            fnameonly = []
            elements = []
            for root,d_names,f_names in os.walk(country_path+"\\"+period):
                elements.append(d_names)
                for f in f_names:
                    fname.append(os.path.join(root, f))
                    fnameonly.append(f)
            
            for element in elements[0]:
                for country in teinvento_inc_countries:
                    #create_directory("/data2/input/teinvento_inc/" + country + "/" + period + "/" + element + "/empty")

                    element_path = country_path + "\\"  + period + "\\" + element
                    os.system("copy " + element_path + "\\*.csv " + element_path + "\\" + element + ".csv") 

                    fname = []
                    fnameonly = []
                    for root,d_names,f_names in os.walk(country_path+"\\"+period+"\\"+element):
                        for f in f_names:
                            fname.append(os.path.join(root, f))
                            fnameonly.append(f)
                    
                    counter = 0
                    for filesource in fname:
                        if (filesource.endswith(element+".csv")):
                                print("Uploading " + element + ".csv")
                                upload_file_to_directory(filesource, element+".csv", "/data2/input/teinvento_inc/" + country + "/" + period)
                                counter = counter + 1
 

        else:
            print("Error: Period was not found")
    

if __name__ == "__main__":
    main(sys.argv[1:])

#create_file_system()
#create_directory("/data/input/erp/")
#create_directory("/data/input/teinvento.inc/")

#establish_connection()
#create_directory("/data2/input/tamales_inc/empty")
#create_directory("/data2/input/teinvento_inc/empty")
#create_directory("/data2/crudo/generador/fuente/empty")
#create_directory("/data2/procesado/generador/fuente/empty")    

#upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\products.csv", "products.csv", "/data/input/erp")
#upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\calories.csv", "calories.csv", "/data/input/erp")
#upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\erp\\export.csv", "export.csv", "/data/input/erp")
#upload_file_to_directory("C:\\projects\\tortasTamal\\tortasTamal\\data\\teinvento.inc\\metricsales.csv", "metricsales.csv", "/data/input/teinvento.inc")


