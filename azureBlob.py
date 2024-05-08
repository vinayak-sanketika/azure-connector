
from typing import Dict, List, Any
from azure.storage.blob import ContainerClient, BlobClient
from pyspark.sql import DataFrame, SparkSession
from blob_provider import BlobProvider
from object_info import ObjectInfo,Tag
from pyspark.conf import SparkConf
import json




class AzureBlobStorage(BlobProvider):
    def __init__(self, config_file_path: str):
        with open(config_file_path, "r") as config_file:
            config = json.load(config_file)

        self.config=config
        self.account_name = config["credentials"]["account_name"]
        self.account_key = config["credentials"]["account_key"]
        self.container_name = config["containername"]
        self.blob_endpoint = config["blob_endpoint"]

        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};BlobEndpoint={self.blob_endpoint}" 

   
    def fetch_objects(self) -> List[ObjectInfo]:
        print("inside fetch_objects")
        try:
            objects = self._list_blobs_in_container(self.container_name)
            objects_info=[]
            if objects==None:
                raise Exception("No objects found")
            
            for obj in objects:
                blob_client = BlobClient.from_connection_string(
                    conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
                )
                account_name = self.connection_string.split(";")[1].split("=")[-1]
                blob_location = f"https://{account_name}.blob.core.windows.net/{self.container_name}/{obj}"
                properties = blob_client.get_blob_properties()
            
                #print("prop",obj)
                object_info = ObjectInfo(
                name=obj.split(".")[0],
                file_size_kb=properties.size,
                location= blob_location,
                type=obj.split(".")[-1],
                tags= self.get_blob_tags(obj)


            )
                objects_info.append(object_info.to_json())
        #print(objects_info)
        except Exception as e:
            print("Exception:",e)
            exit()
        return objects_info
    
    def read_object(self, object_path: str, sc: SparkSession, file_format: str) -> DataFrame:
        try:
            if file_format=="csv":
                df = sc.read.format("csv").option("header",True).load(object_path)
                df.show(5)
        
                return df
            elif file_format=="json":
                df = sc.read.format("json").option("multiLine", False).load(object_path)
                df.show(truncate=False)
                return df
            else:
                raise Exception("UNSUPPORTED_FILE_FORMAT", f"unsupported file format: {file_format}")
        except Exception as e:
             print(f"Failed to read data from Blob {e}")
        


    def _list_blobs_in_container(self, container_name):
        try:
            container = ContainerClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name
            )
            if container.exists():
                print("Container exists")
            else:
                raise Exception("Container does not exist")

            blob_list = container.list_blobs()
            print(" ")
            print("Listing blobs...\n")
            blob_metadata = []
            for blob in blob_list:
                blob_metadata.append(blob.name)
            print(blob_metadata, "\n")

            for blob_name in blob_metadata:
                self._get_properties(container_name, blob_name)
            if blob_metadata==None:
                raise Exception("NO objs")
            
            return blob_metadata
        except Exception as ex:
            print("Exception:", ex)
            ##
        

    def _get_spark_session(self):
        spark = SparkSession.builder\
            .appName("ObsrvAzureBlobStoreConnector")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1") \
            .getOrCreate()
        return spark

    def _get_properties(self, container_name, blob_name):
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=container_name, blob_name=blob_name
            )
            properties = blob_client.get_blob_properties()

            account_name = self.connection_string.split(";")[1].split("=")[-1]
            blob_location = f"wasb://{account_name}/{container_name}/{blob_name}"

            print(f"Blob location: {blob_location}")
            print(f"Blob name: {blob_name.split('.')[0]}")
            print(f"File Type: {blob_name.split('.')[-1]}")
            print(f"Container: {properties.container}")
            print(f"Created Time:{properties.creation_time}")
            print(f"Blob file_size: {properties.size}")
            print(f"Blob metadata: {properties.metadata}")
            print(f"Blob etag: {properties.etag}")
            print(" ")

        except Exception as ex:
            print("Exception:", ex)

    def get_blob_tags(self, object_path:str) -> List[Tag]:
        obj=object_path.split("//")[-1].split("/")[-1]
        print("obj :",obj)
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
            )
            tags = blob_client.get_blob_tags()
            print("Blob tags:")
            
            for k, v in tags.items():
                print(k, v)
                
            #return tag_objects
            
        except (ValueError, IOError) as e:
            print(f"Error retrieving tags: {e}")
        except Exception as ex:
            print("Exception:", ex)
        return [Tag(k,v) for k,v in tags.items()]
    

    def set_blob_tag(self,object_path:str, tags) -> bool:
        try:
            obj=object_path.split("//")[-1].split("/")[-1]
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
            )
            existing_tags = blob_client.get_blob_tags() or {}
            existing_tags.update(tags)
            blob_client.set_blob_tags(existing_tags)
            print("Blob tags updated!")
        except (ValueError, IOError) as e:
            print(f"Error setting tags: {e}")
        except Exception as ex:
            print("Exception:", ex)



# config_file_path = "config.json" 
# blob_storage = AzureBlobStorage(config_file_path)
# #blob_storage._list_blobs_in_container(blob_storage.container_name)


# blob_name = "transform.txt"

# blob_storage.fetch_objects()

# print(" ")

# # Updateing the tags:
# new_tags = {"apt":"testing" }



# object_path="wasb://vinstore@storageemulator/data.json"
# file_format="json"
# obj_file_type=object_path.split(".")[1]
# try:
#     if file_format!=obj_file_type:
#        raise Exception(f"File_format {file_format} not matching with the blob file type {obj_file_type}")
    
#     else:
#         spark = blob_storage._get_spark_session()
#         blob_storage.read_object(object_path,spark,file_format)
# except Exception as e:
#     print("Exception:",e)
# blob_storage.get_blob_tags(object_path)
# blob_storage.set_blob_tag(object_path,new_tags)
# blob_storage.get_blob_tags(object_path)

