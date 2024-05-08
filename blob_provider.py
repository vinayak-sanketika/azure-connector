from abc import ABC, abstractmethod
from object_info import ObjectInfo, Tag
from typing import List,Dict,Any
from pyspark.sql import SparkSession, DataFrame

class BlobProvider(ABC):
    @abstractmethod
    def fetch_objects(self,config: Dict[str, Any] = None) -> List[ObjectInfo]:
        pass
    @abstractmethod
    def read_object(self,config: Dict[str, Any] = None, sc: SparkSession = None) -> DataFrame:
        pass
    @abstractmethod
    def get_blob_tags(self,) -> List[Tag]:
        pass
    @abstractmethod
    def set_blob_tag(self,tags:List[Tag],) -> bool:
        pass