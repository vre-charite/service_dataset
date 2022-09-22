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

import requests
from app.resources.error_handler import APIException
from app.models.base_models import EAPIResponseCode

from ..commons.logger_services.logger_factory_service import SrvLoggerFactory
from ..commons.service_connection.minio_client import Minio_Client_

from ..config import ConfigClass

logger = SrvLoggerFactory('api_dataset_import').get_logger()



def create_relation(label, payload):
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/own", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j node API: {response.json()}", 
                status_code=response.status_code
            )
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j node API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return response.json()

def create_node(label, payload):
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + f"nodes/{label}", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j node API: {response.json()}", 
                status_code=response.status_code
            )
        created_node = response.json()[0]
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j node API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return created_node


def query_relation(relation_label, start_label, end_label, start_params={}, end_params={}):
    payload = {
        "label": relation_label,
        "start_label": start_label,
        "start_params": start_params,
        "end_label": end_label,
        "end_params": end_params,
    }
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j relation query API: {response.json()}", 
                status_code=response.status_code
            )
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j relation query API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return response.json()

def get_node_by_geid(geid, label: str = None):
    try:
        response = None
        # since we have new api to directly fetch by label
        if label:
            payload = {
                'global_entity_id': geid,
            }
            node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/query"%(label)
            response = requests.post(node_query_url, json=payload)
        else:
            node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/geid/%s"%(geid)
            response = requests.get(node_query_url)

        # here if we dont find any node then return None
        if len(response.json()) == 0:
            return None

        return response.json()[0]
    except Exception as e:
        raise APIException(f"Error calling neo4j API: {str(e)}", EAPIResponseCode.internal_error.value)


def get_parent_node(current_node):
    # here we have to find the parent node and delete the relationship
    relation_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
    query_payload = {
        "label": "own",
        "end_label": current_node.get("labels")[0],
        "end_params": {"id":current_node.get("id")}
    }
    response = requests.post(relation_query_url, json=query_payload)
    # print(response.json()[0])
    parent_node_id = response.json()[0].get("start_node")

    return parent_node_id


def get_children_nodes(start_geid, start_label="Folder"):

    payload = {
        "label": "own",
        "start_label": start_label,
        "start_params": {"global_entity_id":start_geid},
    }

    node_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
    response = requests.post(node_query_url, json=payload)
    ffs = [x.get("end_node") for x in response.json()]

    return ffs


def delete_relation_bw_nodes(start_id, end_id):
    # then delete the relationship between all the fils
    relation_delete_url = ConfigClass.NEO4J_SERVICE + "relations"
    delete_params = {
        "start_id": start_id,
        "end_id": end_id,
    }
    response = requests.delete(relation_delete_url, params=delete_params)
    return response


def delete_node(target_node, access_token, refresh_token):

    node_label = target_node.get('labels')[0]
    node_id = target_node.get('id')
    node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/node/%s"%(node_label, node_id)
    response = requests.delete(node_delete_url)

    # delete the file in minio if it is the file
    if node_label == "File":
        try:
            mc = Minio_Client_(access_token, refresh_token)

            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = target_node.get('location').split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))

            mc.delete_object(bucket, obj_path)
            logger.info("Minio %s/%s Delete Success"%(bucket, obj_path))

        except Exception as e:
            logger.error("error when deleting: "+str(e))


def create_file_node(dataset_code, source_file, operator, parent_id, relative_path, \
    access_token, refresh_token, new_name=None):

    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    file_name = new_name if new_name else source_file.get("name")
    # generate minio object path
    fuf_path = relative_path+"/"+file_name

    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    location = "minio://%s/%s/%s"%(minio_http, dataset_code, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        "file_size": source_file.get("file_size", -1), # if the folder then it is -1
        "operator": operator,
        "name": file_name,
        "global_entity_id": geid,
        "location": location,
        "dataset_code": dataset_code,
        "display_path": fuf_path,
    }

    new_file_node, new_relation = create_node_with_parent("File", file_attribute, parent_id)

    # make minio copy
    try:
        mc = Minio_Client_(access_token, refresh_token)
        # minio location is minio://http://<end_point>/bucket/user/object_path
        minio_path = source_file.get('location').split("//")[-1]
        _, bucket, obj_path = tuple(minio_path.split("/", 2))

        mc.copy_object(dataset_code, fuf_path, bucket, obj_path)
        logger.info("Minio Copy %s/%s Success"%(dataset_code, fuf_path))
    except Exception as e:
        logger.error("error when uploading: "+str(e))

    return new_file_node, new_relation


def create_folder_node(dataset_code, source_folder, operator, parent_node, relative_path, new_name=None):
    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    folder_name = new_name if new_name else source_folder.get("name")

    # then copy the node under the dataset
    folder_attribute = {
        "create_by": operator,
        "name": folder_name,
        "global_entity_id": geid,
        "folder_relative_path": relative_path,
        "folder_level": parent_node.get("folder_level", -1)+1,
        "dataset_code": dataset_code,
        "display_path": relative_path+"/"+folder_name,
    }
    folder_node, relation = create_node_with_parent("Folder", folder_attribute, parent_node.get('id'))

    return folder_node, relation


# this function will help to create a target node
# and connect to parent with "own" relationship
def create_node_with_parent(node_label, node_property, parent_id):
    # create the node with following attribute
    # - global_entity_id: unique identifier
    # - create_by: who import the files
    # - size: file size in byte (if it is a folder then size will be -1)
    # - create_time: neo4j timeobject (API will create but not passed in api)
    # - location: indicate the minio location as minio://http://<domain>/object
    create_node_url = ConfigClass.NEO4J_SERVICE + 'nodes/' + node_label
    response = requests.post(create_node_url, json=node_property)
    new_node = response.json()[0]

    # now create the relationship
    # the parent can be two possible: 1.dataset 2.folder under it
    create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
    new_relation = requests.post(create_node_url, json={"start_id":parent_id, "end_id":new_node.get("id")})

    return new_node, new_relation
