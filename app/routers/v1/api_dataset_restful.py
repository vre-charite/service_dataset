from ...models.base_models import APIResponse, EAPIResponseCode
from ...models.models_dataset import SrvDatasetMgr
from ...models.reqres_dataset import DatasetPostForm, DatasetPostResponse, DatasetPutForm
from ...models.validator_dataset import DatasetValidator
from fastapi import APIRouter
from fastapi_utils import cbv
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.error_handler import catch_internal

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
                validator = DatasetValidator.get(k)
                validation = validator(v)
                if validation:
                    pass
                else:
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
