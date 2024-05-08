from azureBlob import AzureBlobStorage



config_file_path = "config.json" 
blob_storage = AzureBlobStorage(config_file_path)
#blob_storage._list_blobs_in_container(blob_storage.container_name)


blob_name = "transform.txt"

blob_storage.fetch_objects()

print(" ")
#blob_storage.get_blob_tags(blob_name)

# Updateing the tags:
new_tags = {"apt":"testing" }



object_path="wasb://vinstore@storageemulator/unemployment.csv"
file_format="iv"
obj_file_type=object_path.split(".")[1]

#Tags

blob_storage.get_blob_tags(object_path)
blob_storage.set_blob_tag(object_path,new_tags)
blob_storage.get_blob_tags(object_path)

# try:
#     if file_format!=obj_file_type:
#        raise Exception(f"File_format {file_format} not matching with the blob file type {obj_file_type}")
    
#     else:
#         spark = blob_storage._get_spark_session()
#         blob_storage.read_object(object_path,spark,file_format)
# except Exception as e:
#     print("Exception:",e)
spark = blob_storage._get_spark_session()
blob_storage.read_object(object_path,spark,file_format)



#blob_storage.get_spark_config()
#blob_storage.read_object(object_path,file_format,SparkSession)
#blob_storage._get_spark_session()
# blob_storage.read_object(object_path,spark,file_format)