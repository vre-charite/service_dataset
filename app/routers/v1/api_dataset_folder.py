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

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from fastapi_utils import cbv

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.models.base_models import APIResponse, EAPIResponseCode
from app.models.folder_models import FolderResponse, FolderRequest
from app.resources.helpers import get_geid
from app.resources.neo4j_helper import get_node_by_geid, query_relation, create_node, create_relation
from app.resources.error_handler import APIException
from app.config import ConfigClass
import requests
import re


logger = SrvLoggerFactory("api_preview").get_logger()
router = APIRouter()


@cbv.cbv(router)
class DatasetFolder:
    '''
        Create an empty folder 
    '''
    @router.post("/v1/dataset/{dataset_geid}/folder", tags=["V1 DATASET"], response_model=FolderResponse, summary="Create an empty folder")
    async def create_folder(self, dataset_geid: str, data: FolderRequest):
        api_response = FolderResponse()
        dataset_node = get_node_by_geid(dataset_geid, label="Dataset")

        # length 1-20, exclude invalid character, ensure start & end aren't a space
        folder_pattern = re.compile("^(?=.{1,20}$)([^\s\/:?*<>|”]{1})+([^\/:?*<>|”])+([^\s\/:?*<>|”]{1})$")
        match = re.search(folder_pattern, data.folder_name)
        if not match:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = "Invalid folder name"
            logger.info(api_response.error_msg)
            return api_response.json_response()

        if not dataset_node:
            logger.error(f"Dataset not found: {dataset_geid}")
            raise APIException(error_msg="Dataset not found", status_code=EAPIResponseCode.not_found.value)

        if data.parent_folder_geid:
            # Folder is being added as a subfolder
            start_label = "Folder"
            folder_node = get_node_by_geid(data.parent_folder_geid, label="Folder")
            if not folder_node:
                logger.error(f"Folder not found: {data.parent_folder_geid}")
                raise APIException(error_msg="Folder not found", status_code=EAPIResponseCode.not_found.value)
            folder_relative_path = folder_node["folder_relative_path"] +  "/" + folder_node["name"]
            parent_node = folder_node
        else:
            # Folder is being added to the root of the dataset
            folder_relative_path = ""
            start_label = "Dataset"
            parent_node = dataset_node

        # Duplicate name check
        result = query_relation(
            "own",
            start_label,
            "Folder",
            start_params={"global_entity_id": parent_node["global_entity_id"]},
            end_params={"name": data.folder_name},
        )
        if result:
            api_response.code = EAPIResponseCode.conflict
            api_response.error_msg = "folder with that name already exists"
            logger.error(api_response.error_msg)
            return api_response.json_response()

        # create node in neo4j
        payload = {
            "name": data.folder_name,
            "create_by": data.username,
            "global_entity_id": get_geid(),
            "dataset_code": dataset_node["code"],
            "folder_relative_path": folder_relative_path,
            "folder_level": parent_node.get("folder_level", -1) + 1,
            "archived": False,
        }
        folder_node = create_node("Folder", payload)

        # Create relation between folder and parent
        relation_payload = {
            "start_id": parent_node["id"],
            "end_id": folder_node["id"],
        }
        result = create_relation("own", relation_payload)
        api_response.result = folder_node
        return api_response.json_response()



