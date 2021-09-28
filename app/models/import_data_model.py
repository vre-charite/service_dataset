from pydantic import BaseModel, Field
import requests

from ..config import ConfigClass
from ..commons.logger_services.logger_factory_service import SrvLoggerFactory

class ImportDataPost(BaseModel):
    '''
    the post request payload for import data from project
    '''
    source_list: list
    operator: str
    project_geid: str


class DatasetFileDelete(BaseModel):
    '''
    the delete request payload for dataset to delete files
    '''
    source_list: list
    operator: str


class DatasetFileMove(BaseModel):
    '''
    the post request payload for dataset to move files
    under the dataset
    '''
    source_list: list
    operator: str
    target_geid: str


class DatasetFileRename(BaseModel):
    '''
    the post request payload for dataset to move files
    under the dataset
    '''
    new_name: str
    operator: str

######################################################################
class SrvDatasetFileMgr():

    logger = SrvLoggerFactory('SrvDatasetFileMgr').get_logger()


    def on_import_event(self, geid, username, source_list):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_FILE_IMPORT_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "ADD",
                "resource": "File",
                "detail": {
                    "source_list": source_list #list of file name
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
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res


    def on_delete_event(self, geid, username, source_list):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_FILE_DELETE_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "REMOVE",
                "resource": "File",
                "detail": {
                    "source_list": source_list #list of file name
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
            raise Exception('__on_delete_event {}: {}'.format(res.status_code, res.text))
        return res

    
    # this function will be per file/folder since the batch display
    # is not human readable
    def on_move_event(self, geid, username, source, target):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_FILE_MOVE_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "MOVE",
                "resource": "File",
                "detail": {
                    "from": source,
                    "to": target
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
            raise Exception('__on_move_event {}: {}'.format(res.status_code, res.text))
        return res


    def on_rename_event(self, geid, username, source, target):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_FILE_RENAME_SUCCEED",
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "UPDATE",
                "resource": "File",
                "detail": {
                    "from": source,
                    "to": target
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
            raise Exception('on_rename_event {}: {}'.format(res.status_code, res.text))
        return res


#########################################################################
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
