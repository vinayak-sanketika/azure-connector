
from azure.storage.blob import ContainerClient, BlobClient
import json

class AzureBlobStorage:
    def __init__(self, connection_string):
        self.connection_string = connection_string

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
            print(f"Blob size: {properties.size}")
            print(f"Blob metadata: {properties.metadata}")
            print(f"Blob etag: {properties.etag}")

            print(" ")

        except Exception as ex:
            print("Exception:", ex)

    def get_blob_tags(self, blob_client):
        try:
            tags = blob_client.get_blob_tags()
            print("Blob tags:")
            for k, v in tags.items():
                print(k, v)
        except (ValueError, IOError) as e:
            print(f"Error retrieving tags: {e}")
        except Exception as ex:
            print("Exception:", ex)

    def set_blob_tags(self, blob_client, tags):
        try:
            existing_tags = blob_client.get_blob_tags() or {}
            existing_tags.update(tags)
            blob_client.set_blob_tags(existing_tags)
            print("Blob tags updated!")
        except (ValueError, IOError) as e:
            print(f"Error setting tags: {e}")
        except Exception as ex:
            print("Exception:", ex)



connection_string="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
cont_name="vinstore"

blob_storage = AzureBlobStorage(connection_string)
blob_storage.list_blobs_in_container(cont_name)

blob_name = "transform.txt"
blob_client = BlobClient.from_connection_string(connection_string, cont_name, blob_name)
print(" ")
blob_storage.get_blob_tags(blob_client)

# Updateing the tags:
new_tags = {"hey":"there" }
blob_storage.set_blob_tags(blob_client, new_tags)
blob_storage.get_blob_tags(blob_client)
