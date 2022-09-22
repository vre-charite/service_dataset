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

from sqlalchemy.sql.operators import is_precedent
from fastapi import APIRouter
from fastapi_utils import cbv
from fastapi_sqlalchemy import db
from requests.api import post
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.error_handler import catch_internal
from ...resources.es_helper import search
from ...models.base_models import APIResponse, EAPIResponseCode
from app.models.version_sql import DatasetVersion
import json
from typing import Optional
from datetime import timezone, datetime
import time

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

        self.__logger.info("activity logs query: {}".format(query))

        try:
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

            self.__logger.info("activity logs result: {}".format(res))

            response.code = EAPIResponseCode.success
            response.result = res['hits']['hits']
            response.total = res['hits']['total']['value']
            return response
        except Exception as e:
            self.__logger.error("activity logs error: {}".format(str(e)))
            response.code = EAPIResponseCode.internal_error
            response.result = {"errors": str(e)}
            return response

    @router.get("/activity-logs/{dataset_geid}", tags=[_API_TAG], summary="list activity logs.")
    @catch_internal(_API_NAMESPACE)
    async def query_activity_logs_by_version(
        self,
        dataset_geid,
        version: str,
        page: Optional[int] = 0,
        page_size: Optional[int] = 10
    ):
        response = APIResponse()

        try:
            versions = db.session.query(DatasetVersion).filter_by(
                dataset_geid=dataset_geid,
                version=version
            ).order_by(DatasetVersion.created_at.desc())

            version_info = versions.first()

            if not version_info:
                response.code = EAPIResponseCode.bad_request
                response.result = "there is no version information for dataset {}".format(
                    dataset_geid)
                return response

            version_data = version_info.to_dict()
            created_at = version_data['created_at']
            created_at = created_at[:19]
            if created_at[10] == ' ':
                create_timestamp = datetime.strptime(
                    created_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            else:
                create_timestamp = datetime.strptime(
                    created_at, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()

        except Exception as e:
            self.__logger.error("Psql Error: " + str(e))
            response.code = EAPIResponseCode.internal_error
            response.result = "Psql Error: " + str(e)
            return response

        search_params = []

        search_params.append({
            "nested": False,
            "field": "create_timestamp",
            "range": [int(create_timestamp)],
            "multi_values": False,
            "search_type": 'gte'
        })

        search_params.append({
            "nested": False,
            "field": "dataset_geid",
            "range": False,
            "multi_values": False,
            "value": dataset_geid,
            "search_type": "equal"
        })

        try:
            res = search('activity-logs', page, page_size,
                         search_params, 'create_timestamp', 'desc')
        except Exception as e:
            self.__logger.error("Elastic Search Error: " + str(e))
            response.code = EAPIResponseCode.internal_error
            response.result = "Elastic Search Error: " + str(e)
            return response

        response.code = EAPIResponseCode.success
        response.result = res['hits']['hits']
        response.total = res['hits']['total']['value']
        return response
