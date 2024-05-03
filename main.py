
from azure.storage.blob import ContainerClient, BlobClient
import json

class AzureBlobStorage:
    def __init__(self, config_file_path: str):
        with open(config_file_path, "r") as config_file:
            config = json.load(config_file)

        
        self.account_name = config["credentials"]["account_name"]
        self.account_key = config["credentials"]["account_key"]
        self.container_name = config["containername"]
        self.blob_endpoint = config.get("blob_endpoint")

        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};BlobEndpoint={self.blob_endpoint}" 

    

    def list_blobs_in_container(self, container_name):
        try:
            container = ContainerClient.from_connection_string(
                conn_str=self.connection_string, container_name=container_name
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
                self.get_properties(container_name, blob_name)

        except Exception as ex:
            print("Exception:", ex)

    def get_properties(self, container_name, blob_name):
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=container_name, blob_name=blob_name
            )
            properties = blob_client.get_blob_properties()

            account_name = self.connection_string.split(";")[1].split("=")[-1]
            blob_location = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"

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

    def get_blob_tags(self, blob_name):
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=blob_name
            )
            tags = blob_client.get_blob_tags()
            print("Blob tags:")
            for k, v in tags.items():
                print(k, v)
        except (ValueError, IOError) as e:
            print(f"Error retrieving tags: {e}")
        except Exception as ex:
            print("Exception:", ex)

    def set_blob_tags(self, tags):
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=blob_name
            )
            existing_tags = blob_client.get_blob_tags() or {}
            existing_tags.update(tags)
            blob_client.set_blob_tags(existing_tags)
            print("Blob tags updated!")
        except (ValueError, IOError) as e:
            print(f"Error setting tags: {e}")
        except Exception as ex:
            print("Exception:", ex)



config_file_path = "config.json" 
blob_storage = AzureBlobStorage(config_file_path)
blob_storage.list_blobs_in_container(blob_storage.container_name)


blob_name = "transform.txt"

print(" ")
blob_storage.get_blob_tags(blob_name)

# Updateing the tags:
new_tags = {"Dept":"implementation" }
blob_storage.set_blob_tags(new_tags)
blob_storage.get_blob_tags(blob_name)