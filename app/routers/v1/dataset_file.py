import requests
from fastapi import APIRouter, BackgroundTasks, Header, File, UploadFile, Form, \
    Cookie
from typing import Optional
from fastapi_utils import cbv
import json
import time
import uuid

from ...models.import_data_model import ImportDataPost, DatasetFileDelete, \
    DatasetFileMove, DatasetFileRename, SrvDatasetFileMgr
from ...models.base_models import APIResponse, EAPIResponseCode
from ...models.models_dataset import SrvDatasetMgr
from ...commons.resource_lock import lock_resource, unlock_resource, check_lock

from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...commons.service_connection.minio_client import Minio_Client

from ...resources.error_handler import catch_internal
from ...resources.neo4j_helper import get_node_by_geid, get_parent_node, \
    get_children_nodes, delete_relation_bw_nodes, delete_node, create_file_node, \
    create_folder_node

from ...config import ConfigClass



router = APIRouter()

_API_TAG = 'V1 DATASET'
_API_NAMESPACE = "api_dataset"

HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

@cbv.cbv(router)
class APIImportData:
    '''
    API to import data from project to dataset
    '''

    def __init__(self):
        self.__logger = SrvLoggerFactory('api_dataset_import').get_logger()

    @router.put("/dataset/{dataset_geid}/files", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will recieve the file list from a project and \n\
                 Copy them under the dataset.")
    @catch_internal(_API_NAMESPACE)
    async def import_dataset(self, dataset_geid, request_payload: ImportDataPost, \
        background_tasks: BackgroundTasks, sessionId: Optional[str] = Cookie(None), \
        Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):

        import_list = request_payload.source_list
        oper = request_payload.operator
        source_project = request_payload.project_geid
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token
        api_response = APIResponse()

        # if dataset not found return 404
        dataset_obj = get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()

        # here we only allow user to import from one project
        # if user try to import from another project block the action
        imported_project = dataset_obj.get("project_geid", None)
        if imported_project and imported_project != source_project:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = "Cannot import from another project"
            return api_response.json_response()


        # TODO check if the file is from core?
        # check if file is from source project or exist
        # and check if file has been under the dataset
        import_list, wrong_file = self.validate_files_folders(import_list, source_project, "Container")
        duplicate, import_list = self.remove_duplicate_file(import_list, dataset_geid, "Dataset")
        # fomutate the result
        api_response.result = {
            "processing": import_list,
            "ignored": wrong_file + duplicate
        }
        
        # start the background job to copy the file one by one
        if len(import_list) > 0:
            background_tasks.add_task(self.copy_files_worker, import_list, dataset_obj, oper, \
                source_project, session_id, minio_access_token, minio_refresh_token)

        return api_response.json_response()


    @router.delete("/dataset/{dataset_geid}/files", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will delete file by geid from list")
    @catch_internal(_API_NAMESPACE)
    async def delete_files(self, dataset_geid, request_payload: DatasetFileDelete, \
        background_tasks: BackgroundTasks, sessionId: Optional[str] = Cookie(None), \
        Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):
        
        api_response = APIResponse()
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token

        # validate the dataset if exists
        dataset_obj = get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()

        # validate the file IS from the dataset 
        delete_list = request_payload.source_list
        delete_list, wrong_file = self.validate_files_folders(delete_list, dataset_geid, "Dataset")
        # fomutate the result
        api_response.result = {
            "processing": delete_list,
            "ignored": wrong_file
        }

        # loop over the list and delete the file one by one
        if len(delete_list) > 0:
            background_tasks.add_task(self.delete_files_work, delete_list, dataset_obj, \
                request_payload.operator, session_id, minio_access_token, minio_refresh_token)

        return api_response.json_response()

    
    @router.get("/dataset/{dataset_geid}/files", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will list files under the target dataset")
    @catch_internal(_API_NAMESPACE)
    async def list_files(self, dataset_geid, page: int = 0, page_size: int = 25, order_by: str = "createTime", 
        order_type: str = "desc", query: str = '{}', folder_geid: str = None):
        '''
        the api will list the file/folder at level 1 by default.
        If folder_geid is not None, then it will treat the folder_geid
        as root and find the relative level 1 file/folder
        '''
        api_response = APIResponse()

        # validate the dataset if exists
        dataset_obj = get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()

        # find the root folder node
        root_geid = folder_geid if folder_geid else dataset_geid
        root_node_label = "Folder" if folder_geid else "Dataset"

        # then get the first level nodes
        query = json.loads(query)
        relation_payload = {
            'page': page,
            'page_size': page_size,
            'order_by': order_by,
            'order_type': order_type,
            "start_label": root_node_label,
            "end_labels":["File", "Folder"],
            "query": {
                "start_params": {"global_entity_id": root_geid},
            },
        }

        response = requests.post(ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=relation_payload)
        file_folder_nodes = response.json().get("results", [])
        # print(file_folder_nodes)

        # then get the routing this will return as parent level
        # like admin->folder1->file1 in UI
        node_query_url = ConfigClass.NEO4J_SERVICE + "relations/connected/"+root_geid
        response = requests.get(node_query_url, params={"direction":"input"})
        file_routing = response.json().get("result", [])
        ret_routing = [x for x in file_routing if "User" not in x.get("labels")]

        ret = {
            "data": file_folder_nodes,
            "route": ret_routing,
        }
        api_response.result = ret

        return api_response.json_response()


    @router.post("/dataset/{dataset_geid}/files", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will move files within the dataset")
    @catch_internal(_API_NAMESPACE)
    async def move_files(self, dataset_geid, request_payload: DatasetFileMove, \
        background_tasks: BackgroundTasks, sessionId: Optional[str] = Cookie(None), \
        Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):

        api_response = APIResponse()
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token

        # validate the dataset if exists
        dataset_obj = get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()


        # first get the target -> the target must be a folder or dataset root
        target_minio_path = None
        root_label = "Folder"
        if request_payload.target_geid ==  dataset_geid:
            target_folder = dataset_obj
            target_minio_path = ""
            root_label = "Dataset"
        else:
            target_folder = get_node_by_geid(request_payload.target_geid, "Folder")
            if len(target_folder) == 0:
                api_response.code = EAPIResponseCode.not_found
                api_response.error_msg = "The target folder does not exist"
                return api_response.json_response()
            # also the folder MUST under the same dataset
            if target_folder.get("dataset_code") != dataset_obj.get("code"):
                api_response.code = EAPIResponseCode.not_found
                api_response.error_msg = "The target folder does not exist"
                return api_response.json_response()


            # formate the target location 
            if target_folder.get('folder_relative_path') == "":
                target_minio_path = target_folder.get('name')+'/'
            else:
                target_minio_path = target_folder.get('folder_relative_path')+'/'+target_folder.get('name')+'/'
        # print(dataset_obj.get('code'), target_minio_path)

        # validate the file if it is under the dataset
        move_list = request_payload.source_list
        move_list, wrong_file = self.validate_files_folders(move_list, dataset_geid, "Dataset")
        duplicate, move_list = self.remove_duplicate_file(move_list, request_payload.target_geid, root_label)
        # fomutate the result
        api_response.result = {
            "processing": move_list,
            "ignored": wrong_file + duplicate
        }

        # start the background job to copy the file one by one
        if len(move_list) > 0:
            background_tasks.add_task(self.move_file_worker, move_list, dataset_obj, 
                request_payload.operator, target_folder, target_minio_path, session_id,
                minio_access_token, minio_refresh_token)
        

        return api_response.json_response()



    @router.post("/dataset/{dataset_geid}/files/{target_file}", tags=[_API_TAG],
                 summary="API will update files within the dataset")
    @catch_internal(_API_NAMESPACE)
    async def rename_file(self, dataset_geid, target_file, request_payload: DatasetFileRename, \
        background_tasks: BackgroundTasks, sessionId: Optional[str] = Cookie(None), \
        Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)):

        api_response = APIResponse()
        session_id = sessionId
        new_name = request_payload.new_name
        minio_access_token = Authorization

        # validate the dataset if exists
        dataset_obj = get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()

        # TODO filename check? regx

        # validate the file IS from the dataset 
        # rename to same name will be blocked
        rename_list, wrong_file = self.validate_files_folders([target_file], dataset_geid, "Dataset")
        # check if there is a file under the folder
        parent_node = get_parent_node(get_node_by_geid(target_file))
        pgeid = parent_node.get("global_entity_id")
        root_label = parent_node.get("labels")[0]
        duplicate, _ = self.remove_duplicate_file([{"name":new_name}], pgeid, root_label)

        # cannot rename to self
        if len(duplicate) > 0:
             duplicate = rename_list
             rename_list = []


        # fomutate the result
        api_response.result = {
            "processing": rename_list,
            "ignored": wrong_file+duplicate
        }

        # loop over the list and delete the file one by one
        if len(rename_list) > 0:
            background_tasks.add_task(self.rename_file_worker, rename_list, new_name, dataset_obj, \
                request_payload.operator, session_id, minio_access_token, refresh_token)

        return api_response.json_response()


##########################################################################################################
    #
    # the function will walk throught the list and validate
    # if the node is from correct root geid. for example:
    # - PUT: the imported files must from target project
    # - POST: the moved files must from correct dataset
    # - DELETE: the deleted file must from correct dataset
    #
    # function will return two list: 
    # - passed_file is the validated file
    # - not_passed_file is not under the target node
    def validate_files_folders(self, ff_list, root_geid, root_label):

        # TODO handle if the geid does not exist

        passed_file = []
        not_passed_file = []
        for ff in ff_list:
            # fetch the current node
            current_node = get_node_by_geid(ff)
            # if not exist skip the node
            if not current_node:
                not_passed_file.append({
                    "global_entity_id": ff,
                    "feedback": "not exists"
                })
                continue

            relation_payload={
                "label": "own*",
                "start_label": root_label,
                "end_label": current_node.get("labels", []),
                "start_params": {"global_entity_id":root_geid},
                "end_params": {"global_entity_id":current_node.get("global_entity_id", None)}
            }

            response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", json=relation_payload)
            file_folder_nodes = response.json()


            # if there is no connect then the node is not correct
            # else it is correct
            if len(file_folder_nodes) == 0:
                not_passed_file.append({
                    "global_entity_id": ff,
                    "feedback": "unauthorized"
                })
            else:
                # also update a feedback as exists
                node_detail = current_node
                node_detail.update({"feedback": "exist"})
                passed_file.append(node_detail)


        return passed_file, not_passed_file
    

    # the function will reuse the <validate_files_folders> to check 
    # if the file already exist directly under the root node
    # return True if duplicate else false
    def remove_duplicate_file(self, ff_list, root_geid, root_label):

        duplic_file = []
        not_duplic_file = []
        for current_node in ff_list:
            # here we dont check if node is None since
            # the previous function already check it

            relation_payload={
                "label": "own",
                "start_label": root_label,
                "start_params": {"global_entity_id":root_geid},
                "end_params": {
                    "name":current_node.get("name", None)
                }
            }

            response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", 
                data=json.dumps(relation_payload).encode('utf-8'),
                headers={"Content-Type": "application/json"})
            file_folder_nodes = response.json()

            # if there is no connect then the node is correct
            # else it is not correct
            if len(file_folder_nodes) == 0:
                not_duplic_file.append(current_node)
            else:
                current_node.update({"feedback":"duplicate or unauthorized"})
                duplic_file.append(current_node)

        return duplic_file, not_duplic_file

    # TODO make it into the helper function
    # match (n)-[r:own*]->(f) where n.global_entity_id="9ff8382d-f476-4cdf-a357-66c4babf8320-1626104650" delete 
    # FOREACH(r), f
    def send_notification(self, session_id, source_list, action, \
        status, dataset_geid, operator, task_id, payload={}):
        
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_FILE_NOTIFICATION",
            "payload": {
                "session_id":session_id,
                "task_id": task_id,
                "source":source_list,
                "action":action,
                "status":status,         # INIT/RUNNING/FINISH/ERROR
                "dataset":dataset_geid, 
                "operator":operator,
                "payload":payload,
                "update_timestamp": time.time()
            },
            "binary": True,
            "queue": "socketio",
            "routing_key": "socketio",
            "exchange": {
                "name": "socketio",
                "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('send_notification() {}: {}'.format(res.status_code, res.text))

        return res

    # 
    def create_job_status(self, session_id, source_file, action, \
        status, dataset, operator, task_id, payload={}):

        # first send the notification
        dataset_geid = dataset.get("global_entity_id")
        dataset_code = dataset.get("code")
        self.send_notification(session_id, source_file, action, status, dataset_geid, operator, task_id)

        # also save to redis for display
        source_geid = source_file.get("global_entity_id")
        job_id = action+"-"+source_geid+"-"+str(int(time.time()))
        task_url = ConfigClass.DATA_UTILITY_SERVICE + "tasks"
        post_json = {
            "session_id":session_id,
            "label":"Dataset",
            "source": source_geid,
            "task_id": task_id,
            "job_id": job_id,
            "action":action,
            "code": dataset_code,
            "target_status": status,
            "operator": operator,
            "payload": source_file,
        }
        res = requests.post(task_url, json=post_json)
        if res.status_code != 200:
            raise Exception('save redis error {}: {}'.format(res.status_code, res.text))

        return {source_geid:job_id}


    def update_job_status(self, session_id, source_file, action, \
        status, dataset, operator, task_id, job_id, payload={}):

        # first send the notification
        dataset_geid = dataset.get("global_entity_id")
        self.send_notification(session_id, source_file, action, status, dataset_geid, \
            operator, task_id, payload)

        # also save to redis for display
        task_url = ConfigClass.DATA_UTILITY_SERVICE + "tasks"
        post_json = {
            "session_id":session_id,
            "label":"Dataset",
            "task_id": task_id,
            "job_id": job_id,
            "status": status,
            "add_payload":payload,
        }
        res = requests.put(task_url, json=post_json)
        if res.status_code != 200:
            raise Exception('save redis error {}: {}'.format(res.status_code, res.text))

        return res


    # the function will call the create_job_status to initialize
    # the job status in the redis and prepare for update in copy/delete
    # the function will return the job object include:
    #   - session id: fetch from frontend
    #   - task id: random generate for batch operation
    #   - action: the file action name
    #   - job id mapping: dictionary for tracking EACH file progress
    def initialize_file_jobs(self, session_id, action, batch_list, dataset_obj, oper):
        # use the dictionary to keep track the file action with
        session_id = "local_test" if not session_id else session_id
        # action = "dataset_file_import"
        task_id = action+"-"+str(int(time.time()))
        job_tracker = {
            "session_id": session_id,
            "task_id": task_id, 
            "action": action,
            "job_id":{}
        }
        for file_object in batch_list:
            tracker = self.create_job_status(session_id, file_object, action, \
                "INIT", dataset_obj, oper, task_id)
            job_tracker["job_id"].update(tracker)

        return job_tracker

###########################################################################################

    def recursive_copy(self, currenct_nodes, dataset, oper, current_root_path, \
        parent_node, access_token, refresh_token, job_tracker=None, new_name=None):

        num_of_files = 0
        total_file_size = 0
        # this variable DOESNOT contain the child nodes
        new_lv1_nodes = []

        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:
            ff_geid = ff_object.get("global_entity_id")
            new_node = None

            # update here if the folder/file is archieved then skip
            if ff_object.get("archived", False):
                continue

            # here ONLY the first level file/folder will trigger the notification&job status
            if job_tracker:
                job_id = job_tracker["job_id"].get(ff_geid)
                self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                    "RUNNING", dataset, oper, job_tracker["task_id"], job_id)
            
            ################################################################################################
            # recursive logic below
            
            if 'File' in ff_object.get("labels"):
                # TODO simplify here
                minio_path = ff_object.get('location').split("//")[-1]
                _, bucket, old_path = tuple(minio_path.split("/", 2))
                # lock the resource
                lockkey_template = "{}/{}/{}"
                old_lockkey = "{}/{}".format(bucket, old_path)
                new_lockkey = lockkey_template.format(dataset.get("code"), \
                    current_root_path, new_name if new_name else ff_object.get("name"))
                
                # try to aquire the lock for old path and lock the new resources
                is_lock_approved = self.try_lock(old_lockkey)
                lock_resource(new_lockkey)
                if not is_lock_approved:
                    if job_tracker:
                        job_id = job_tracker["job_id"].get(ff_geid)
                        self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                            "TERMINATED", dataset, oper, job_tracker["task_id"], job_id)

                    self.__logger.warn("Resource %s has been used by other process"%old_lockkey)
                    # terminate process and unlock the new
                    unlock_resource(new_lockkey)
                    return num_of_files, total_file_size
                
                # create the copied node
                new_node, _ = create_file_node(dataset.get("code"), ff_object, oper, parent_node.get('id'), \
                    current_root_path, access_token, refresh_token, new_name) 
                # update for number and size
                num_of_files += 1; total_file_size += ff_object.get("file_size", 0)
                new_lv1_nodes.append(new_node)
                unlock_resource(old_lockkey)
                unlock_resource(new_lockkey)

            # else it is folder will trigger the recursive
            elif 'Folder' in ff_object.get("labels"):
                
                # first create the folder
                new_node, _ = create_folder_node(dataset.get("code"), ff_object, oper, \
                    parent_node, current_root_path, new_name)
                new_lv1_nodes.append(new_node)

                # seconds recursively go throught the folder/subfolder by same proccess
                # also if we want the folder to be renamed if new_name is not None
                next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_geid)
                num_of_child_files, num_of_child_size, _ = \
                    self.recursive_copy(children_nodes, dataset, oper, next_root, new_node, \
                        access_token, refresh_token)

                # append the log together
                num_of_files += num_of_child_files
                total_file_size += num_of_child_size
            ##########################################################################################################


            # here after all use the geid to mark the job done for either first level folder/file
            # if the geid is not in the tracker then it is child level ff. ignore them
            if job_tracker:
                job_id = job_tracker["job_id"].get(ff_geid)
                self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                    "FINISH", dataset, oper, job_tracker["task_id"], job_id, payload=new_node)

        return num_of_files, total_file_size, new_lv1_nodes


    def recursive_delete(self, currenct_nodes, dataset, oper, parent_node, \
        access_token, refresh_token, job_tracker=None):

        num_of_files = 0
        total_file_size = 0
        deleted_lv1_node = []

        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:
            ff_geid = ff_object.get("global_entity_id")

            # update here if the folder/file is archieved then skip
            if ff_object.get("archived", False):
                continue

            # here ONLY the first level file/folder will trigger the notification&job status
            if job_tracker:
                job_id = job_tracker["job_id"].get(ff_geid)
                self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                    "RUNNING", dataset, oper, job_tracker["task_id"], job_id)
            
            ################################################################################################
            # recursive logic below
            if 'File' in ff_object.get("labels"):

                # lock the resource
                lockkey_template = "{}/{}"
                # minio location is minio://http://<end_point>/bucket/user/object_path
                minio_path = ff_object.get('location').split("//")[-1]
                _, bucket, obj_path = tuple(minio_path.split("/", 2))
                lockkey = lockkey_template.format(bucket, obj_path)
                is_lock_approved = self.try_lock(lockkey)
                if not is_lock_approved:
                    if job_tracker:
                        job_id = job_tracker["job_id"].get(ff_geid)
                        self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                            "TERMINATED", dataset, oper, job_tracker["task_id"], job_id)
                    self.__logger.warn("Resource %s has been used by other process"%lockkey)
                    # terminate process
                    return num_of_files, total_file_size

                # for file we can just disconnect and delete
                # TODO MOVE OUTSIDE <=============================================================
                delete_relation_bw_nodes(parent_node.get("id"), ff_object.get("id"))
                delete_node(ff_object, access_token, refresh_token)
                # unlock resource
                unlock_resource(lockkey)
                # update for number and size
                num_of_files += 1; total_file_size += ff_object.get("file_size", 0)

            # else it is folder will trigger the recursive
            elif 'Folder' in ff_object.get("labels"):
                
                # for folder, we have to disconnect all child node then
                # disconnect it from parent
                children_nodes = get_children_nodes(ff_object.get("global_entity_id"))
                num_of_child_files, num_of_child_size = \
                    self.recursive_delete(children_nodes, dataset, oper, ff_object, access_token, refresh_token)

                # after the child has been deleted then we disconnect current node
                delete_relation_bw_nodes(parent_node.get("id"), ff_object.get("id"))
                delete_node(ff_object, access_token, refresh_token) 

                # append the log together
                num_of_files += num_of_child_files
                total_file_size += num_of_child_size
            ##########################################################################################

            # here after all use the geid to mark the job done for either first level folder/file
            # if the geid is not in the tracker then it is child level ff. ignore them
            if job_tracker:
                job_id = job_tracker["job_id"].get(ff_geid)
                self.update_job_status(job_tracker["session_id"], ff_object, job_tracker["action"], \
                    "FINISH", dataset, oper, job_tracker["task_id"], job_id)

        return num_of_files, total_file_size


######################################################################################################

    def copy_files_worker(self, import_list, dataset_obj, oper, source_project_geid, session_id, \
        access_token, refresh_token):

        action = "dataset_file_import"
        job_tracker = self.initialize_file_jobs(session_id, action, import_list, dataset_obj, oper)

        # recursively go throught the folder level by level
        root_path = ConfigClass.DATASET_FILE_FOLDER
        num_of_files, total_file_size, _ = self.recursive_copy(import_list, dataset_obj, \
            oper, root_path, dataset_obj, access_token, refresh_token, job_tracker)

        # after all update the file number/total size/project geid
        srv_dataset = SrvDatasetMgr()
        update_attribute = {
            "total_files": dataset_obj.get("total_files", 0) + num_of_files,
            "size": dataset_obj.get("size", 0) + total_file_size,
            "project_geid": source_project_geid,
        }
        srv_dataset.update(dataset_obj, update_attribute, [])

        # also update the log
        dataset_geid = dataset_obj.get("global_entity_id")
        source_project = get_node_by_geid(source_project_geid)
        import_logs = [source_project.get("code")+"/"+x.get("display_path") for x in import_list]
        SrvDatasetFileMgr().on_import_event(dataset_geid, oper, import_logs)

        return


    def move_file_worker(self, move_list, dataset_obj, oper, target_folder, target_minio_path, 
        session_id, access_token, refresh_token):
        
        dataset_geid = dataset_obj.get("global_entity_id")
        action = "dataset_file_move"
        job_tracker = self.initialize_file_jobs(session_id, action, move_list, dataset_obj, oper)


        # minio move update the arribute
        # find the parent node for path
        parent_node = target_folder
        parent_path = parent_node.get("folder_relative_path", None)
        parent_path = parent_path+"/"+parent_node.get("name") if parent_path else ConfigClass.DATASET_FILE_FOLDER
        # but note here the job tracker is not pass into the function
        # we only let the delete to state the finish
        _, _, _ = self.recursive_copy(move_list, dataset_obj, oper, parent_path, parent_node, \
            access_token, refresh_token)


        # delete the old one 
        self.recursive_delete(move_list, dataset_obj, oper, parent_node, \
            access_token, refresh_token, job_tracker=job_tracker)

        # generate the activity log
        dff = ConfigClass.DATASET_FILE_FOLDER+"/"
        for ff_geid in move_list:
            if "File" in ff_geid.get("labels"):
                # minio location is minio://http://<end_point>/bucket/user/object_path
                minio_path = ff_geid.get('location').split("//")[-1]
                _, bucket, old_path = tuple(minio_path.split("/", 2))
                old_path = old_path.replace(dff, "", 1)

                # format new path if the temp is None then the path is from
                new_path = (target_minio_path+ff_geid.get("name")).replace(dff, "", 1)

            # else we mark the folder as deleted
            else:
                # update the relative path by remove `data` at begining
                old_path = ff_geid.get("folder_relative_path")+"/"+ff_geid.get("name")
                old_path = old_path.replace(dff, "", 1)

                new_path = target_minio_path+ff_geid.get("name")
                new_path = new_path.replace(dff, "", 1)
            
            # send to the es for logging
            SrvDatasetFileMgr().on_move_event(dataset_geid, oper, old_path, new_path)


    def delete_files_work(self, delete_list, dataset_obj, oper, session_id, access_token, \
        refresh_token):

        deleted_files = [] # for logging action
        action = "dataset_file_delete"
        job_tracker = self.initialize_file_jobs(session_id, action, delete_list, dataset_obj, oper)


        num_of_files, total_file_size = self.recursive_delete(delete_list, dataset_obj, \
            oper, dataset_obj, access_token, refresh_token, job_tracker)

        # TODO try to embed with the notification&job status
        # generate log path
        for ff_geid in delete_list:
            if "File" in ff_geid.get("labels"):
                # minio location is minio://http://<end_point>/bucket/user/object_path
                minio_path = ff_geid.get('location').split("//")[-1]
                _, bucket, obj_path = tuple(minio_path.split("/", 2))

                # update metadata
                dff = ConfigClass.DATASET_FILE_FOLDER + "/"
                obj_path = obj_path[:len(dff)].replace(dff, "") + obj_path[len(dff):]
                deleted_files.append(obj_path)

            # else we mark the folder as deleted
            else:
                # update the relative path by remove `data` at begining
                dff = ConfigClass.DATASET_FILE_FOLDER
                temp = ff_geid.get("folder_relative_path")

                # consider the root level delete will need to remove the data path at begining
                frp = ""
                if dff != temp:
                    dff = dff + "/"
                    frp = temp.replace(dff, "", 1)
                deleted_files.append(frp+ff_geid.get("name"))


        # after all update the file number/total size/project geid
        srv_dataset = SrvDatasetMgr()
        update_attribute = {
            "total_files": dataset_obj.get("total_files", 0) - num_of_files,
            "size": dataset_obj.get("size", 0) - total_file_size,
        }
        srv_dataset.update(dataset_obj, update_attribute, [])

        # also update the message to service queue
        dataset_geid = dataset_obj.get("global_entity_id")
        SrvDatasetFileMgr().on_delete_event(dataset_geid, oper, deleted_files)

        return
        

    # the rename worker will reuse the recursive_copy&recursive_delete
    # with only one file. the old_file is the node object and update
    # attribute to new name
    def rename_file_worker(self, old_file, new_name, dataset_obj, oper, session_id, \
        access_token, refresh_token):

        action = "dataset_file_rename"
        job_tracker = self.initialize_file_jobs(session_id, action, old_file, dataset_obj, oper)
        # since the renanme will be just one file set to the running now
        job_id = job_tracker["job_id"].get(old_file[0].get("global_entity_id"))
        self.update_job_status(job_tracker["session_id"], old_file[0], job_tracker["action"], \
            "RUNNING", dataset_obj, oper, job_tracker["task_id"], job_id)


        # minio move update the arribute
        # find the parent node for path
        parent_node = get_parent_node(old_file[0])
        parent_path = parent_node.get("folder_relative_path", None)
        parent_path = parent_path+"/"+parent_node.get("name") if parent_path else ConfigClass.DATASET_FILE_FOLDER
        # same here the job tracker is not pass into the function
        # we only let the delete to state the finish
        _, _, new_nodes = self.recursive_copy(old_file, dataset_obj, oper, parent_path, parent_node, \
            access_token, refresh_token, new_name=new_name)

        # delete the old one
        self.recursive_delete(old_file, dataset_obj, oper, parent_node, access_token, \
            refresh_token)

        # after deletion set the status using new node
        self.update_job_status(job_tracker["session_id"], old_file[0], job_tracker["action"], \
            "FINISH", dataset_obj, oper, job_tracker["task_id"], job_id, new_nodes[0])

        # update es & log
        dataset_geid = dataset_obj.get("global_entity_id")
        old_file_name = old_file[0].get("name")
        # remove the /data in begining ONLY once
        frp = ""
        if ConfigClass.DATASET_FILE_FOLDER != parent_path:
            frp = parent_path.replace(ConfigClass.DATASET_FILE_FOLDER+"/",'', 1)+"/"
        SrvDatasetFileMgr().on_rename_event(dataset_geid, oper, frp+old_file_name, frp+new_name)

        return


    # LOCK THE RESOURCE, IF return False, 
    def try_lock(self, lock_key):
        res_check_lock = check_lock(lock_key)
        # print(res_check_lock.json())
        if res_check_lock.status_code == 200:
            lock_status = res_check_lock.json()['result']['status']
            if lock_status == 'LOCKED':
                return False
        lock_resource(lock_key)
        return True
