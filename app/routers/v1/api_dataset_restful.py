from ...models.base_models import APIResponse, EAPIResponseCode
from ...models.models_dataset import SrvDatasetMgr
from ...models.reqres_dataset import DatasetPostForm, DatasetPostResponse, DatasetPutForm, DatasetVerifyForm
from ...models.validator_dataset import DatasetValidator
from fastapi import APIRouter, Header
from fastapi_utils import cbv
from fastapi_sqlalchemy import db
from typing import Optional

import json
import subprocess
import shutil
import time
import requests
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.error_handler import catch_internal
from ...resources.utils import get_files_recursive, get_related_nodes, http_query_node, get_node_relative_path, make_temp_folder
from ...resources.dataset_validator import validator_messages
from app.config import ConfigClass
from app.models.bids_sql import BIDSResult


router = APIRouter()

_API_TAG = 'V1 Dataset Restful'
_API_NAMESPACE = "api_dataset_restful"


@cbv.cbv(router)
class DatasetRestful:
    '''
    API Dataset Restful
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory(_API_NAMESPACE).get_logger()

    @router.post("/v1/dataset", tags=[_API_TAG], response_model=DatasetPostResponse,
                 summary="Create a dataset.")
    @catch_internal(_API_NAMESPACE)
    async def create_dataset(self, request_payload: DatasetPostForm):
        '''
        dataset creation api
        '''
        res = APIResponse()

        srv_dataset = SrvDatasetMgr()

        check_created = srv_dataset.get_bycode(request_payload.code)
        if check_created.status_code == 200:
            if len(check_created.json()) > 0:
                res.result = None
                res.error_msg = "[Invalid 'code']: already taken by other dataset."
                res.code = EAPIResponseCode.conflict
                return res.json_response()

        post_dict = request_payload.dict()
        for k, v in post_dict.items():
            if v != None:
                # use the factory to get the validator function
                validator = DatasetValidator.get(k)
                validation = validator(v)
                if not validation:
                    res.code = EAPIResponseCode.bad_request
                    res.result = None
                    res.error_msg = "Invalid {}".format(k)
                    return res.json_response()

        created = srv_dataset.create(
            request_payload.username,
            request_payload.code,
            request_payload.title,
            request_payload.authors,
            request_payload.type,
            request_payload.modality,
            request_payload.collection_method,
            request_payload.tags,
            request_payload.license,
            request_payload.description,
        )

        res.code = EAPIResponseCode.success
        res.result = created
        return res.json_response()

    @router.get("/v1/dataset/{dataset_geid}", tags=[_API_TAG], response_model=DatasetPostResponse,
                summary="Get a dataset.")
    @catch_internal(_API_NAMESPACE)
    async def get_dataset(self, dataset_geid):
        '''
        dataset creation api
        '''
        res = APIResponse()

        srv_dataset = SrvDatasetMgr()

        dataset_gotten = None

        response_dataset_node = srv_dataset.get_bygeid(dataset_geid)
        if response_dataset_node.status_code == 200:
            if len(response_dataset_node.json()) > 0:
                dataset_gotten = response_dataset_node.json()[0]
                res.code = EAPIResponseCode.success
                res.result = dataset_gotten
                return res.json_response()
            else:
                res.code = EAPIResponseCode.not_found
                res.result = dataset_gotten
                res.error_msg = "Not Found, invalid geid"
                return res.json_response()
        else:
            raise(Exception(response_dataset_node.text))

    @router.get("/v1/dataset-peek/{code}", tags=[_API_TAG], response_model=DatasetPostResponse,
                summary="Get a dataset.")
    @catch_internal(_API_NAMESPACE)
    async def get_dataset_bycode(self, code):
        '''
        dataset creation api
        '''
        res = APIResponse()

        srv_dataset = SrvDatasetMgr()

        dataset_gotten = None

        response_dataset_node = srv_dataset.get_bycode(code)
        if response_dataset_node.status_code == 200:
            if len(response_dataset_node.json()) > 0:
                dataset_gotten = response_dataset_node.json()[0]
                res.code = EAPIResponseCode.success
                res.result = dataset_gotten
                return res.json_response()
            else:
                res.code = EAPIResponseCode.not_found
                res.result = dataset_gotten
                res.error_msg = "Not Found, invalid dataset code"
                return res.json_response()
        else:
            raise(Exception(response_dataset_node.text))

    @router.put("/v1/dataset/{dataset_geid}", tags=[_API_TAG], response_model=DatasetPostResponse,
                summary="Update a dataset.")
    @catch_internal(_API_NAMESPACE)
    async def update_dataset(self, dataset_geid, request_payload: DatasetPutForm):
        '''
        dataset creation api
        '''
        res = APIResponse()

        srv_dataset = SrvDatasetMgr()

        dataset_gotten = None

        # get dataset
        response_dataset_node = srv_dataset.get_bygeid(dataset_geid)
        if response_dataset_node.status_code == 200:
            if len(response_dataset_node.json()) > 0:
                dataset_gotten = response_dataset_node.json()[0]
            else:
                res.code = EAPIResponseCode.not_found
                res.result = dataset_gotten
                res.error_msg = "Not Found, invalid geid"
                return res.json_response()
        else:
            raise(Exception(response_dataset_node.text))

        # parse update json
        update_json = {}
        put_dict = request_payload.dict()
        allowed = ['title', 'authors', 'modality',
                   'collection_method', 'license', 'tags',
                   'description', 'file_count', 'total_size',
                   'activity',
                   ]
        for k, v in put_dict.items():
            if k not in allowed:
                res.code = EAPIResponseCode.bad_request
                res.result = None
                res.error_msg = "{} update is not allowed".format(k)
                return res.json_response()
            if v != None and k != 'activity':
                validator = DatasetValidator.get(k)
                validation = validator(v)
                if not validation:
                    res.code = EAPIResponseCode.bad_request
                    res.result = None
                    res.error_msg = "Invalid {}".format(k)
                    return res.json_response()
                update_json[k] = v

        # do update
        updated_node = srv_dataset.update(dataset_gotten,
                                          update_json, request_payload.activity)

        res.code = EAPIResponseCode.success
        res.result = updated_node
        return res.json_response()

    @router.post("/v1/dataset/verify", tags=[_API_TAG], summary="verify a bids dataset.")
    @catch_internal(_API_NAMESPACE)
    async def verify_dataset(self, request_payload: DatasetVerifyForm):
        res = APIResponse()
        payload = request_payload.dict()

        dataset_geid = payload['dataset_geid']
        verify_type = payload['type']

        dataset_res = http_query_node(
            'Dataset', {'global_entity_id': dataset_geid})

        if dataset_res.status_code != 200:
            res.code = EAPIResponseCode.bad_request
            res.result = {"result": "dataset not exist"}
            return res.json_response()

        dataset_info = dataset_res.json()
        if len(dataset_info) == 0:
            res.code = EAPIResponseCode.bad_request
            res.result = {"result": "dataset not exist"}
            return res.json_response()

        dataset_info = dataset_info[0]
        dataset_code = dataset_info['code']
        nodes = get_related_nodes(dataset_geid)

        files_info = []
        TEMP_FOLDER = 'temp/'

        for node in nodes:
            if 'File' in node['labels']:
                file_path = get_node_relative_path(
                    dataset_code, node['location'])
                files_info.append(
                    {"file_path": TEMP_FOLDER + dataset_code + file_path, "file_size": node['file_size']})

            if 'Folder' in node['labels']:
                files = get_files_recursive(node['global_entity_id'])
                for file in files:
                    file_path = get_node_relative_path(
                        dataset_code, file['location'])
                    files_info.append(
                        {"file_path": TEMP_FOLDER + dataset_code + file_path, "file_size": file['file_size']})

        try:
            make_temp_folder(files_info)
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.result = "failed to create temp folder for bids"
            return res.json_response()

        try:
            result = subprocess.run(['bids-validator', TEMP_FOLDER + dataset_code, '--json',
                                     '--ignoreNiftiHeaders', '--ignoreSubjectConsistency'], stdout=subprocess.PIPE)
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.result = "failed to validate bids folder"
            return res.json_response()

        try:
            shutil.rmtree(TEMP_FOLDER + dataset_code)
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.result = "failed to remove temp bids folder"
            return res.json_response()

        res.result = json.loads(result.stdout)
        return res.json_response()

    @router.post("/v1/dataset/verify/pre", tags=[_API_TAG], summary="pre verify a bids dataset.")
    @catch_internal(_API_NAMESPACE)
    async def pre_verify_dataset(self, request_payload: DatasetVerifyForm, Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):
        res = APIResponse()
        payload = request_payload.dict()

        dataset_geid = payload['dataset_geid']
        verify_type = payload['type']

        dataset_res = http_query_node(
            'Dataset', {'global_entity_id': dataset_geid})

        if dataset_res.status_code != 200:
            res.code = EAPIResponseCode.bad_request
            res.result = {"result": "dataset not exist"}
            return res.json_response()

        access_token = Authorization.split(' ')[1]

        payload = {
            "event_type": "bids_validate",
            "payload": {
                "dataset_geid": dataset_geid,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "project": "dataset",
            },
            "create_timestamp": time.time()
        }
        url = ConfigClass.SEND_MESSAGE_URL
        self.__logger.info("Sending Message To Queue: " + str(payload))
        msg_res = requests.post(
            url=url,
            json=payload,
            headers={"Content-type": "application/json; charset=utf-8"}
        )
        if msg_res.status_code != 200:
            res.code = EAPIResponseCode.internal_error
            res.result = {"result": msg_res.text}
            return res.json_response()

        res.code = EAPIResponseCode.success
        res.result = msg_res.json()

        return res.json_response()

    @router.get("/v1/dataset/bids-msg/{dataset_geid}", tags=[_API_TAG], summary="pre verify a bids dataset.")
    @catch_internal(_API_NAMESPACE)
    async def get_bids_msg(self, dataset_geid):
        api_response = APIResponse()
        try:
            bids_results = db.session.query(BIDSResult).filter_by(
                dataset_geid=dataset_geid,
            ).order_by(BIDSResult.created_time.desc())
            bids_result = bids_results.first()

            if not bids_result:
                api_response.result = {}
                return api_response.json_response()

            bids_result = bids_result.to_dict()
            api_response.result = bids_result
            return api_response.json_response()
        except Exception as e:
            self.__logger.error("Psql Error: " + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = "Psql Error: " + str(e)
            return api_response.json_response()
