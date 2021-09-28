import requests
from fastapi import APIRouter, BackgroundTasks, Header, File, UploadFile, Form, \
    Cookie
from typing import Optional
from fastapi_utils import cbv
import json
import time
import uuid

from sqlalchemy.orm.exc import NoResultFound

from app.models.schema_template_model import SchemaTemplatePost, SchemaTemplatePut,\
    SchemaTemplateList, SrvDatasetSchemaTemplateMgr
from app.models.schema_sql import session, DatasetSchemaTemplate
from app.models.base_models import APIResponse, EAPIResponseCode

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory

from app.resources.error_handler import catch_internal
from app.resources.helpers import get_geid


router = APIRouter()

_API_TAG = 'V1 DATASET'
_API_NAMESPACE = "api_dataset"

HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

# this function will check if the template name already exist
def check_template_name(name, dataset_geid):
    try:
        result = session.query(DatasetSchemaTemplate)\
                .filter(DatasetSchemaTemplate.name == name)\
                .filter(DatasetSchemaTemplate.dataset_geid == dataset_geid).one()
    except NoResultFound:
        return False
    
    return True


@cbv.cbv(router)
class APISchemaTemplate:
    '''
    API for dataset schema template
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_dataset_schema_template').get_logger()
        self.__activity_manager = SrvDatasetSchemaTemplateMgr()

    @router.post("/dataset/{dataset_geid}/schemaTPL", tags=[_API_TAG],
                 summary="API will create the new schema template")
    @catch_internal(_API_NAMESPACE)
    async def create_schema_template(self, dataset_geid, request_payload: SchemaTemplatePost):

        api_response = APIResponse()
        # here we enforce the uniqueness of the name within dataset_geid
        exist = check_template_name(request_payload.name, dataset_geid)
        if exist:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = "The template name already exists."
            return api_response


        new_template = DatasetSchemaTemplate(
            geid=get_geid(),
            name=request_payload.name,
            dataset_geid=dataset_geid,
            standard=request_payload.standard,
            system_defined=request_payload.system_defined,
            is_draft=request_payload.is_draft,
            content=request_payload.content,
            creator=request_payload.creator,
        )

        session.add(new_template)
        session.commit()
        api_response.result = new_template.to_dict()

        # create the log activity
        self.__activity_manager.on_create_event(
            dataset_geid,
            new_template.geid,
            request_payload.creator,
            request_payload.name
        )

        return api_response.json_response()


    @router.post("/dataset/{dataset_geid}/schemaTPL/list", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will list the template by condition")
    @catch_internal(_API_NAMESPACE)
    async def list_schema_template(self, dataset_geid, request_payload: SchemaTemplateList):
        api_response = APIResponse()
        result = None
        if dataset_geid == "default":
            result = session.query(DatasetSchemaTemplate).\
                filter(DatasetSchemaTemplate.system_defined == True).all()
        else:
            result = session.query(DatasetSchemaTemplate).\
                filter(DatasetSchemaTemplate.dataset_geid == dataset_geid).all()

        ret = []
        for x in result:
            ret.append({
                "geid": x.geid,
                "name": x.name,
                "system_defined": x.system_defined,
                "standard": x.standard,
            })
        api_response.result = ret

        return api_response

##########################################################################################################

    @router.get("/dataset/{dataset_geid}/schemaTPL/{template_geid}", tags=[_API_TAG],
                 summary="API will get the template by geid")
    @catch_internal(_API_NAMESPACE)
    async def get_schema_template(self, dataset_geid, template_geid):

        api_response = APIResponse()

        try:
            result = session.query(DatasetSchemaTemplate)\
                    .filter(DatasetSchemaTemplate.geid == template_geid)
            if dataset_geid == "default":
                result = result.filter(DatasetSchemaTemplate.system_defined == True).one()
            else:
                result = result.filter(DatasetSchemaTemplate.dataset_geid == dataset_geid).one()

            api_response.result = result.to_dict()
        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found

        return api_response.json_response()


    
    @router.put("/dataset/{dataset_geid}/schemaTPL/{template_geid}", tags=[_API_TAG], 
                 summary="API will create the new schema template")
    @catch_internal(_API_NAMESPACE)
    async def update_schema_template(self, template_geid, dataset_geid, request_payload: SchemaTemplatePut):

        api_response = APIResponse()

        # here we enforce the uniqueness of the name with in dataset_geid
        exist = check_template_name(request_payload.name, dataset_geid)
        if exist:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = "The template name already exists."
            return api_response, EAPIResponseCode.forbidden

        try:
            result = session.query(DatasetSchemaTemplate)\
                .filter(DatasetSchemaTemplate.geid == template_geid)\
                .filter(DatasetSchemaTemplate.dataset_geid == dataset_geid).one()

            # update the row if we find it
            result.name = request_payload.name
            result.content = request_payload.content
            result.is_draft = request_payload.is_draft
            session.commit()
            api_response.result = result.to_dict()

            # based on the frontend infomation, create the log activity
            activities = request_payload.activity
            for act in activities:
                self.__activity_manager.on_update_event(
                    dataset_geid,
                    template_geid,
                    result.creator,
                    act.get("action"),
                    act.get("detail", {})
                )
        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found

        return api_response.json_response()



    @router.delete("/dataset/{dataset_geid}/schemaTPL/{template_geid}", tags=[_API_TAG],
                 summary="API will create the new schema template")
    @catch_internal(_API_NAMESPACE)
    async def remove_schema_template(self, dataset_geid, template_geid):

        api_response = APIResponse()

        # delete the row if we find it
        try:
            result = session.query(DatasetSchemaTemplate).\
                filter(DatasetSchemaTemplate.geid == template_geid).one()

            session.delete(result)
            session.commit()
            api_response.result = result.to_dict()

            # create the log activity
            self.__activity_manager.on_delete_event(
                result.dataset_geid,
                template_geid,
                result.creator,
                result.name
            )

        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found
            return api_response.json_response()

        return api_response.json_response()