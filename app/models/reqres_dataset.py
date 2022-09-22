# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List
from .base_models import APIResponse


class Activity(BaseModel):
    action: str
    resource: str
    detail = {}


class DatasetPostForm(BaseModel):
    '''
    DatasetPostForm
    '''
    username: str
    title: str
    code: str
    authors: list
    type: str = "GENERAL"
    modality: List[str] = []
    collection_method: list = []
    license: str = ""
    tags: list = []
    description: str
    file_count: int = 0
    total_size: int = 0  # unit as byte


class DatasetPostResponse(APIResponse):
    '''
    DatasetPostResponse
    '''
    result: dict = Field({}, example={
        "global_entity_id": "xxxxx",
        "source": "project_geid",
        "title": "title",
        "authors": ["author1", ],
        "code": "(unique identifier)",
        "creator": "creator",
        "type": "type",
        "modality": "modality",
        "collection_method": ["collection_method", ],
        "license": "license",
        "tags": ["tag", ],
        "description": "description",
        "size": 0,
        "total_files": 0
    })


class DatasetVerifyForm(BaseModel):
    dataset_geid: str
    type: str


class DatasetListForm(BaseModel):
    filter = {}
    order_by: str
    order_type = "desc"
    page: int = 0
    page_size: int = 10


class DatasetListResponse(APIResponse):
    '''
    List response
    '''
    result: dict = Field({}, example=[
        {
            "global_entity_id": "xxxxx",
            "source": "project_geid",
            "title": "title",
            "authors": ["author1", ],
            "code": "(unique identifier)",
            "creator": "creator",
            "type": "type",
            "modality": "modality",
            "collection_method": ["collection_method", ],
            "license": "license",
            "tags": ["tag", ],
            "description": "description",
            "size": 0,
            "total_files": 0
        }
    ])
