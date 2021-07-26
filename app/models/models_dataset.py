from ..config import ConfigClass
from ..commons.logger_services.logger_factory_service import SrvLoggerFactory
from ..commons.service_connection.minio_client import Minio_Client
from minio.sseconfig import Rule, SSEConfig
import requests
import time

class SrvDatasetMgr():
    '''
    Service for Dataset Entity INFO Manager
    ''' 
    logger = SrvLoggerFactory('SrvDatasetMgr').get_logger()

    def create(self, username, code, title, authors,
        type, modality, collection_method, tags, license, description):
        '''
        Create File Data Entity V2
        '''
        post_json_form = {
            "source": "",
            "title": title,
            "authors": authors,
            "code": code,
            "type": type,
            "modality": modality,
            "collection_method": collection_method,
            "license": license,
            "tags": tags,
            "description": description,
            "size": 0,
            "total_files": 0,
            "name": code,
            "creator": username,
        }
        self.logger.debug('SrvDatasetMgr post_json_form' +
                          str(post_json_form))     
        result_create_node = http_post_node(post_json_form)
        if result_create_node.status_code == 200:
            node_created = result_create_node.json()[0]
            self.__link_user(node_created["id"], username)
            created_atlas = self.__create_atlas_node(node_created["global_entity_id"], username)
            event_created = self.__on_create_event(node_created["global_entity_id"], username)

            # and also create minio bucket with the dataset code
            try:
                mc = Minio_Client()
                mc.client.make_bucket(code)
                mc.client.set_bucket_encryption(code, SSEConfig(Rule.new_sse_s3_rule()))
            except Exception as e:
                self.logger.error("error when creating minio: "+str(e))

            return node_created
        else:
            raise Exception(str(result_create_node.text))

    def update(self, current_node, update_json, activities: list):
        res_update_node = http_update_node("Dataset", current_node['id'], update_json)
        if res_update_node.status_code == 200:
            pass
        else:
            raise Exception(str(res_update_node.text))
        for activity in activities:
            event_created = self.__on_update_event(
                current_node["global_entity_id"],
                current_node['creator'],
                activity)
        return res_update_node.json()[0]


    def get_bygeid(self, geid):
        payload = {
            "global_entity_id": geid
        }
        node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/Dataset/query"
        response = requests.post(node_query_url, json=payload)
        return response

    def get_bycode(self, code):
        payload = {
            "code": code
        }
        node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/Dataset/query"
        response = requests.post(node_query_url, json=payload)
        return response

    def __link_user(self, dataset_id, username):
        '''
        link user
        '''
        respon_user_query = http_get_usernode(username)
        if not respon_user_query.status_code == 200:
            raise(Exception("[respon_user_query Error] {} {}".format(
                respon_user_query.status_code, respon_user_query.text)))
        users_fetch = respon_user_query.json()
        if len(users_fetch) < 1:
            raise(Exception("[respon_user_query Error] Not Found User {}".format(username)))
        user_node = respon_user_query.json()[0]
        relation_payload = {
            "start_id": user_node["id"], "end_id": dataset_id}
        response = requests.post(ConfigClass.NEO4J_SERVICE +
                                "relations/own", json=relation_payload)
        if response.status_code // 100 == 2:
            return response
        else:
            raise(Exception("[link_user Error] {} {}".format(
                response.status_code, response.text)))

    def __on_create_event(self, geid, username):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_CREATE_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "CREATE",
                "resource": "Dataset",
                "detail": {
                    "source": geid
                }
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
            "name": "DATASET_ACTS",
            "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_create_event {}: {}'.format(res.status_code, res.text))
        return res

    def __on_update_event(self, geid, username, activity):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_UPDATE_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": activity.action,
                "resource": activity.resource,
                "detail": activity.detail
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
            "name": "DATASET_ACTS",
            "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_create_event {}: {}'.format(res.status_code, res.text))
        return res


    def __create_atlas_node(self, geid, username):
        res = create_atlas_dataset(geid, username)
        if res.status_code != 200:
            raise Exception('__create_atlas_node {}: {}'.format(res.status_code, res.text))
        return res


def get_geid():
    '''
    get geid
    '''
    url = ConfigClass.COMMON_SERVICE + "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))


def http_post_node(node_dict: dict, geid=None):
    '''
    will assign the geid automaticly
    '''
    if not geid:
        node_dict["global_entity_id"] = get_geid()
    node_creation_url = ConfigClass.NEO4J_SERVICE + "nodes/Dataset"
    response = requests.post(node_creation_url, json=node_dict)
    return response


def http_get_usernode(username):
    payload = {
        "username": username
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/User/query"
    response = requests.post(node_query_url, json=payload)
    return response


def http_query_node_bygeid(label, geid):
    payload = {
        "global_entity_id": geid
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/{}/query".format(label)
    response = requests.post(node_query_url, json=payload)
    return response


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    update_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/node/{}".format(primary_label, neo4j_id)
    res = requests.put(url=update_url, json=update_json)
    return res


def create_atlas_dataset(geid, operator):
        attrs = {
            'global_entity_id': geid,
            'qualifiedName': geid,
            'name': geid,
            ## ------------------------------------------------------------------------------------
            'createTime': time.time(),
            'modifiedTime': 0,
            'replicatedTo': None,
            'userDescription': None,
            'isFile': False,
            'numberOfReplicas': 0,
            'replicatedFrom': None,
            'displayName': None,
            'extendedAttributes': None,
            'nameServiceId': None,
            'posixPermissions': None,
            'clusterName': None,
            'isSymlink': False,
            'group': None,
        }
        atlas_post_form_json = {
            'referredEntities': {},
            'entity': {
                'typeName': 'dataset',
                'attributes': attrs,
                'isIncomplete': False,
                'status': 'ACTIVE',
                'createdBy': operator,
                'version': 0,
                'relationshipAttributes': {
                    'schema': [],
                    'inputToProcesses': [],
                    'meanings': [],
                    'outputFromProcesses': []
                },
                'customAttributes': {},
                'labels': []
            }
        }
        url = ConfigClass.CATALOGUING_SERVICE_V1 + "entity"
        res = requests.post(url, json=atlas_post_form_json)
        return res