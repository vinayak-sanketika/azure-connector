from dataclasses import dataclass, field
from uuid import uuid4
from datetime import datetime
from typing import List

@dataclass
class Tag:
    key: str
    value: str
    def to_dict(self):
        return {
            'key': self.key,
            'value': self.value
        }

@dataclass
class ObjectInfo:
    id: str = field(default_factory=lambda: str(uuid4()))
    location: str = None
    name: str = None
    type: str = None
    file_size_kb: int = 0
    in_time: datetime = field(default_factory=datetime.now)
    start_processing_time: datetime = None
    end_processing_time: datetime = None
    # download_time: float = 0.0
    # file_hash: str = None
    # num_of_retries: int = 0
    tags: List[Tag] = field(default_factory=list)
    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'location': self.location,
            'type': self.type,
            'file_size_kb': self.file_size_kb,
            'in_time': self.in_time,
            'start_processing_time': self.start_processing_time,
            'end_processing_time': self.end_processing_time,
            # 'file_hash': self.file_hash,
            # 'num_of_retries': self.num_of_retries,
            'tags': [tag.__dict__ for tag in self.tags]
        }