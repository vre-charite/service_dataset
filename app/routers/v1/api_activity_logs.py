from fastapi import APIRouter
from fastapi_utils import cbv
from requests.api import post
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.error_handler import catch_internal
from ...resources.es_helper import search
from ...models.base_models import APIResponse, EAPIResponseCode
import json
from typing import Optional


router = APIRouter()

_API_TAG = 'V1 Activity Logs Query API'
_API_NAMESPACE = "api_activity_logs"


@cbv.cbv(router)
class ActivityLogs:
    '''
    API Activity Logs
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory(_API_NAMESPACE).get_logger()

    @router.get("/activity-logs", tags=[_API_TAG], summary="list activity logs.")
    @catch_internal(_API_NAMESPACE)
    async def query_activity_logs(
        self,
        query: str,
        page: Optional[int] = 0,
        page_size: Optional[int] = 10,
        sort_by: Optional[str] = 'create_timestamp',
        sort_type: Optional[str] = 'desc'
    ):
        response = APIResponse()
        queries = json.loads(query)
        search_params = []

        for key in queries:
            if key == 'create_timestamp':
                filed_params = {
                    "nested": False,
                    "field": key,
                    "range": queries[key]['value'],
                    "multi_values": False,
                    "search_type": queries[key]['condition']
                }
                search_params.append(filed_params)
            else:
                filed_params = {
                    "nested": False,
                    "field": key,
                    "range": False,
                    "multi_values": False,
                    "value": queries[key]['value'],
                    "search_type": queries[key]['condition']
                }
                search_params.append(filed_params)

        res = search('activity-logs', page, page_size,
                     search_params, sort_by, sort_type)

        response.code = EAPIResponseCode.success
        response.result = res['hits']['hits']
        response.total = res['hits']['total']['value']
        return response
