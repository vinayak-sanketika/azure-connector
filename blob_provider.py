from abc import ABC, abstractmethod
from object_info import ObjectInfo
from typing import List,Dict,Any

class BlobProvider(ABC):
    @abstractmethod
    def fetch_objects(self,connectorConfig: Dict[str, Any] = None) -> List[ObjectInfo]:
        pass