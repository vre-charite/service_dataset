import requests
from fastapi import APIRouter, BackgroundTasks, Header, File, UploadFile, Form
from fastapi_utils import cbv
import json

from ...models.import_data_model import ImportDataPost, DatasetFileDelete, \
    DatasetFileMove, SrvDatasetFileMgr
from ...models.base_models import APIResponse, EAPIResponseCode
from ...models.models_dataset import SrvDatasetMgr

from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...commons.service_connection.minio_client import Minio_Client

from ...resources.error_handler import catch_internal

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
    async def import_dataset(self, dataset_geid, request_payload: ImportDataPost, background_tasks: BackgroundTasks):

        import_list = request_payload.source_list
        oper = request_payload.operator
        source_project = request_payload.project_geid
        api_response = APIResponse()

        # if dataset not found return 404
        dataset_obj = self.get_node_by_geid(dataset_geid, "Dataset")
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
            # import_list = [x.get("global_entity_id") for x in import_list]
            background_tasks.add_task(self.copy_files_worker, import_list, dataset_obj, oper, source_project)

        return api_response.json_response()


    @router.delete("/dataset/{dataset_geid}/files", tags=[_API_TAG], #, response_model=PreUploadResponse,
                 summary="API will delete file by geid from list")
    @catch_internal(_API_NAMESPACE)
    async def delete_files(self, dataset_geid, request_payload: DatasetFileDelete, background_tasks: BackgroundTasks):
        
        # TODO update file number + file size after deletion

        api_response = APIResponse()

        # validate the dataset if exists
        dataset_obj = self.get_node_by_geid(dataset_geid, "Dataset")
        if dataset_obj == None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "Invalid geid for dataset"
            return api_response.json_response()

        # validate the file IS from the dataset <===============================
        delete_list = request_payload.source_list
        delete_list, wrong_file = self.validate_files_folders(delete_list, dataset_geid, "Dataset")
        # fomutate the result
        api_response.result = {
            "processing": delete_list,
            "ignored": wrong_file
        }

        # loop over the list and delete the file one by one
        if len(delete_list) > 0:
            delete_list = [x.get("global_entity_id") for x in delete_list]
            background_tasks.add_task(self.delete_files_work, delete_list, dataset_obj, request_payload.operator)

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

        # TBD check the folder is also under the dataset

        api_response = APIResponse()

        # validate the dataset if exists
        dataset_obj = self.get_node_by_geid(dataset_geid, "Dataset")
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
    async def move_files(self, dataset_geid, request_payload: DatasetFileMove, background_tasks: BackgroundTasks):
        # TBD check the folder is also under the dataset

        api_response = APIResponse()

        # validate the dataset if exists
        dataset_obj = self.get_node_by_geid(dataset_geid, "Dataset")
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
            target_folder = self.get_node_by_geid(request_payload.target_geid, "Folder")
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
        print(dataset_obj.get('code'), target_minio_path)

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
            move_list = [x.get("global_entity_id") for x in move_list]
            background_tasks.add_task(self.move_file_worker, move_list, dataset_obj, 
                request_payload.operator, target_folder, target_minio_path)
        

        return api_response.json_response()



##################################################################################################################
    
    # the function will walk throught the list and validate
    # if the node is from correct root geid. for example:
    # - PUT: the imported files must from target project
    # - POST: the moved files must from correct dataset
    # - DELETE: the deleted file must from correct dataset
    # function will return two list: 
    # - passed_file is the validated file
    # - not_passed_file is not under the target node
    def validate_files_folders(self, ff_list, root_geid, root_label):

        # TODO handle if the geid does not exist

        passed_file = []
        not_passed_file = []
        for ff in ff_list:
            # fetch the current node
            current_node = self.get_node_by_geid(ff)
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

        # TODO make fetch at first

        duplic_file = []
        not_duplic_file = []
        for current_node in ff_list:
            # fetch the current node
            # current_node = self.get_node_by_geid(ff)
            # here we dont check if node is None since
            # the previous function already check it

            relation_payload={
                "label": "own",
                "start_label": root_label,
                "start_params": {"global_entity_id":root_geid},
                "end_params": {
                    "name":current_node.get("name", None),
                }
            }

            response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", json=relation_payload)
            file_folder_nodes = response.json()

            # if there is no connect then the node is correct
            # else it is not correct
            if len(file_folder_nodes) == 0:
                not_duplic_file.append(current_node)
            else:
                current_node.update({"feedback":"duplicate or unauthorized"})
                duplic_file.append(current_node)

        return duplic_file, not_duplic_file

    
    def get_node_by_geid(self, geid, label: str = None):

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


    def get_parent_node(self, current_node):
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

    # # this function is kind of special in import(copy) worker, since
    # # we are copy the node tree from project to dataset. The action will
    # # need to use the unique attribute(like path) to find out the folder
    # # node under the dataset by the path which is from the project file nodes
    # def get_parent_node_by_path_under_dataset(self, relative_path, parent_name, dataset_code):
    #     # note here we add the `data` subfolder so attach it to path
    #     rp = ConfigClass.DATASET_FILE_FOLDER+"/"+relative_path if \
    #         relative_path else ConfigClass.DATASET_FILE_FOLDER
    #     payload = {
    #         "folder_relative_path": rp,
    #         "name": parent_name,
    #         "dataset_code": dataset_code
    #     }
    #     node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/Folder/query"
    #     response = requests.post(node_query_url, json=payload)

    #     if len(response.json()) == 0:
    #         return None

    #     return response.json()[0]

    # 
    def get_children_nodes(self, start_geid):

        payload = {
                "label": "own",
                "start_label": "Folder",
                "start_params": {"global_entity_id":start_geid},
            }

        node_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
        response = requests.post(node_query_url, json=payload)
        ffs = [x.get("end_node") for x in response.json()]

        return ffs


    def delete_relation_bw_nodes(self, start_id, end_id):
        # then delete the relationship between all the fils
        relation_delete_url = ConfigClass.NEO4J_SERVICE + "relations"
        delete_params = {
            "start_id": start_id,
            "end_id": end_id,
        }
        response = requests.delete(relation_delete_url, params=delete_params)
        return response


    def create_file_node(self, dataset_code, source_file, operator, parent_id, relative_path):
        # fecth the geid from common service
        geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
        file_name = source_file.get("name")
        # generate minio object path
        fuf_path = relative_path+"/"+file_name
        # # add the minio file subfolder 
        # fuf_path = ConfigClass.DATASET_FILE_FOLDER+"/"+fuf_path

        minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
        location = "minio://%s/%s/%s"%(minio_http, dataset_code, fuf_path)

        # then copy the node under the dataset
        file_attribute = {
            "file_size": source_file.get("file_size", -1), # if the folder then it is -1
            "operator": operator,
            "name": file_name,
            "global_entity_id": geid,
            "location": location,
            "dataset_code": dataset_code
        }

        # print(location)

        self.create_node_with_parent("File", file_attribute, parent_id)

        # make minio copy
        try:
            mc = Minio_Client()
            self.__logger.info("Minio Connection Success")

            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = source_file.get('location').split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))

            # print(bucket, obj_path)

            mc.copy_object(dataset_code, fuf_path, bucket, obj_path)
            self.__logger.info("Minio Copy Success")
        except Exception as e:
            self.__logger.error("error when uploading: "+str(e))


    def create_folder_node(self, dataset_code, source_folder, operator, parent_node, relative_path):

        # fecth the geid from common service
        geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
        folder_name = source_folder.get("name")

        # then copy the node under the dataset
        folder_attribute = {
            # "file_size": file_object.get("file_size", -1), # if the folder then it is -1
            "create_by": operator,
            "name": folder_name,
            "global_entity_id": geid,
            "folder_relative_path": relative_path,
            "folder_level": parent_node.get("folder_level", -1)+1,
            "dataset_code": dataset_code,
        }

        print(parent_node.get('id'))

        folder_node, _ = self.create_node_with_parent("Folder", folder_attribute, parent_node.get('id'))

        return folder_node, _

    # this function will help to create a target node
    # and connect to parent with "own" relationship
    def create_node_with_parent(self, node_label, node_property, parent_id):
        # print("Creating New Node")
        # print(node_label, node_property, parent_id)

        # create the node with following attribute
        # - global_entity_id: unique identifier
        # - create_by: who import the files
        # - size: file size in byte (if it is a folder then size will be -1)
        # - create_time: neo4j timeobject (API will create but not passed in api)
        # - location: indicate the minio location as minio://http://<domain>/object
        create_node_url = ConfigClass.NEO4J_SERVICE + 'nodes/' + node_label
        response = requests.post(create_node_url, json=node_property)
        new_node = response.json()[0]
        # print(new_node)

        # now create the relationship
        # the parent can be two possible: 1.dataset 2.folder under it
        create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
        new_relation = requests.post(create_node_url, json={"start_id":parent_id, "end_id":new_node.get("id")})
        # print(new_relation.json())

        return new_node, new_relation


##########################################################################################################
    def recursive_copy(self, currenct_nodes, dataset_code, oper, current_root_path, parent_node):
        num_of_files = 0
        total_file_size = 0

        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:

            # update here if the folder/file is archieved then skip
            if ff_object.get("archived", True):
                continue

            # action filter by label
            if 'File' in ff_object.get("labels"):
                # print("File", ff_object.get("name"))

                file_object = ff_object

                self.create_file_node(dataset_code, file_object, oper, parent_node.get('id'), current_root_path) 
                # update for number and size
                num_of_files += 1; total_file_size += file_object.get("file_size", 0)

            # else it is folder
            elif 'Folder' in ff_object.get("labels"):
                # print("Folder", ff_object.get("name"))
                
                # first create the folder
                new_folder_node, _ = self.create_folder_node(dataset_code, ff_object, oper, parent_node, current_root_path)

                # seconds recursively go throught the folder/subfolder by same proccess
                next_root = current_root_path+"/"+ff_object.get("name")
                children_nodes = self.get_children_nodes(ff_object.get("global_entity_id"))
                num_of_child_files, num_of_child_size = \
                    self.recursive_copy(children_nodes, dataset_code, oper, next_root, new_folder_node)

                # append the log together
                num_of_files += num_of_child_files
                total_file_size += num_of_child_size

        return num_of_files, total_file_size
        

    def copy_files_worker(self, import_list, dataset_obj, oper, source_project_geid):

        # recursively go throught the folder level by level
        root_path = ConfigClass.DATASET_FILE_FOLDER
        num_of_files, total_file_size = \
            self.recursive_copy(import_list, dataset_obj.get("code"), oper, root_path, dataset_obj)

        # after all update the file number/total size/project geid
        srv_dataset = SrvDatasetMgr()
        update_attribute = {
            "total_files": dataset_obj.get("total_files", 0) + num_of_files,
            "size": dataset_obj.get("size", 0) + total_file_size,
            "project_geid": source_project_geid,
        }
        srv_dataset.update(dataset_obj, update_attribute, [])

        # also update the message to service queue
        dataset_geid = dataset_obj.get("global_entity_id")
        source_project = self.get_node_by_geid(source_project_geid)
        import_logs = [source_project.get("code")+"/"+x.get("display_path") for x in import_list]
        SrvDatasetFileMgr().on_import_event(dataset_geid, oper, import_logs)

        return


    def move_file_worker(self, move_list, dataset_obj, oper, target_folder, target_minio_path):

        dataset_geid = dataset_obj.get("global_entity_id")

        for ff_geid in move_list:
            ##############################################################
            # step 1: for each of them delete parent connection
            ##############################################################

            # get all connected files to validate it is a folder or file
            # and the files will be used to take minio move later
            node_query_url = ConfigClass.NEO4J_SERVICE + "relations/connected/"+ff_geid
            response = requests.get(node_query_url, params={"direction":"output"})
            files_under_folder = response.json().get("result", [])
            # print(files_under_folder)

            # also get the parent node
            response = requests.get(node_query_url, params={"direction":"input"})
            parent_node = response.json().get("result", [])
            # print(parent_node[0])


            # get the current node 
            ff_object = self.get_node_by_geid(ff_geid)
            ff_label = ff_object.get("labels")[0]
            # then remove the relationship
            response = self.delete_relation_bw_nodes(parent_node[0].get("id"), ff_object.get("id"))

            ##############################################################
            # step 2: reconnect the node with target node
            ##############################################################
            # now create the relationship
            # the parent can be two possible: 1.dataset 2.folder under it
            create_relation_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
            payload = {"start_id":target_folder.get("id"), "end_id":ff_object.get("id")}
            new_relation = requests.post(create_relation_url, json=payload)

            # TODO
            # also update the folder level relative path
            log_path_old = ""; log_path_new = ""
            if ff_label == "Folder":
                update_attribute_url = ConfigClass.NEO4J_SERVICE + 'nodes/%s/node/%s'%(ff_label, ff_object.get("id"))
                frp = target_folder.get("folder_relative_path")
                log_path_old = ff_object.get("folder_relative_path")+'/'+ff_object.get("name")

                frp = frp + "/" + target_folder.get("name") if frp else ConfigClass.DATASET_FILE_FOLDER
                log_path_new = frp+'/'+ff_object.get("name")
                payload = {
                    "folder_level":target_folder.get("folder_level", -1) + 1, 
                    "folder_relative_path":frp
                }
                res = requests.put(update_attribute_url, json=payload)
                
                dff = ConfigClass.DATASET_FILE_FOLDER + "/"
                log_path_old = log_path_old[:len(dff)].replace(dff, "") + log_path_old[len(dff):]
                log_path_new = log_path_new[:len(dff)].replace(dff, "") + log_path_new[len(dff):]
                SrvDatasetFileMgr().on_move_event(dataset_geid, oper, log_path_old, log_path_new)



            ##############################################################
            # step 3: call minio api to move the files
            ##############################################################
            print("=================================================")
            files_under_folder.append(ff_object)
            for fuf in files_under_folder:
                print()
                print(fuf)

                # TODO update to the recursive
                if fuf.get("global_entity_id") in move_list and "Folder" in fuf.get("labels", []):
                    continue

                if "File" in fuf.get("labels", []):
                    try:
                        mc = Minio_Client()
                        # self.__logger.info("Minio Connection Success")

                        # formate the source location
                        minio_path = fuf.get('location').split("//")[-1]
                        # print(minio_path)
                        _, bucket, obj_path = tuple(minio_path.split("/", 2))

                        # get the parent of current fuf
                        parent_fuf = self.get_parent_node(fuf)

                        # also have to update the node attribute in neo4j
                        # new_path = target_minio_path+fuf.get("name")
                        new_path = parent_fuf.get("folder_relative_path")
                        new_path = new_path+"/"+parent_fuf.get("name")+"/"+fuf.get("name") \
                            if new_path else ConfigClass.DATASET_FILE_FOLDER+"/"+fuf.get("name")
                        minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
                        new_location = "minio://%s/%s/%s"%(minio_http, dataset_obj.get('code'), new_path)
                        res = requests.put(ConfigClass.NEO4J_SERVICE+"nodes/File/node/%s"%(fuf.get("id")), \
                            data=('{"location":"%s"}'%(new_location)).encode('utf-8'), headers=HEADERS)

                        print(obj_path, new_path)

                        mc.move_object(
                            dataset_obj.get('code'), 
                            new_path,
                            bucket,
                            obj_path
                        )
                        # self.__logger.info("Minio Move Success")

                        # also update the message to service queue and remove the subfolder in display
                        if fuf.get("global_entity_id") in move_list:
                            dff = ConfigClass.DATASET_FILE_FOLDER + "/"
                            obj_path = obj_path[:len(dff)].replace(dff, "") + obj_path[len(dff):]
                            new_path = new_path[:len(dff)].replace(dff, "") + new_path[len(dff):]
                            SrvDatasetFileMgr().on_move_event(dataset_geid, oper, obj_path, new_path)

                    except Exception as e:
                        self.__logger.error("error when uploading: "+str(e))

                elif "Folder" in fuf.get("labels", []):
                    # then only update the relative path and folder level
                    update_attribute_url = ConfigClass.NEO4J_SERVICE + 'nodes/Folder/node/%s'%(fuf.get("id"))
                    # get the parent of current fuf
                    parent_fuf = self.get_parent_node(fuf)
                    payload = {
                        "folder_level":parent_fuf.get("folder_level", -1) + 1, 
                        "folder_relative_path":parent_fuf.get("folder_relative_path")+'/'+parent_fuf.get("name")
                    }
                    res = requests.put(update_attribute_url, json=payload)
                    print(res.json())


    def delete_files_work(self, delete_list, dataset_obj, oper):
        num_of_files = 0
        total_file_size = 0
        deleted_files = [] # for logging action

        for ff_geid in delete_list:
            # get all connected files
            node_query_url = ConfigClass.NEO4J_SERVICE + "relations/connected/"+ff_geid
            response = requests.get(node_query_url, params={"direction":"output"})
            files_under_folder = response.json().get("result", [])

            # first disconnect all the relationship
            for fuf in files_under_folder:
                
                # here we have to find the parent node and delete the relationship
                parent_node_id = self.get_parent_node(fuf).get("id")
                # then delete the relationship between all the fils
                response = self.delete_relation_bw_nodes(parent_node_id, fuf.get("id"))
                # print(response.__dict__)


            # then remove all the file/folder node + minio object
            for fuf in files_under_folder:
                node_label = fuf.get('labels')[0]
                node_id = fuf.get('id')
                node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/node/%s"%(node_label, node_id)
                response = requests.delete(node_delete_url)
                # print(response.__dict__)

                # delete the node if it is the file
                if node_label == "File":
                    try:
                        mc = Minio_Client()
                        self.__logger.info("Minio Connection Success")

                        # minio location is minio://http://<end_point>/bucket/user/object_path
                        minio_path = fuf.get('location').split("//")[-1]
                        _, bucket, obj_path = tuple(minio_path.split("/", 2))

                        mc.delete_object(bucket, obj_path)
                        self.__logger.info("Minio Delete Success")

                        # some metadata update
                        num_of_files += 1; total_file_size += fuf.get("file_size", 0)
                    except Exception as e:
                        self.__logger.error("error when uploading: "+str(e))


            ###########################################################
            # delete the level 1 folder/file after all
            ###########################################################

            # fetch the current node
            ff_object = self.get_node_by_geid(ff_geid)
            ff_label = ff_object.get("labels")[0]

            # then remove the relationship
            response = self.delete_relation_bw_nodes(dataset_obj.get("id"), ff_object.get("id"))

            # then delete the node itself
            node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/node/%s"%(ff_label, ff_object.get("id"))
            response = requests.delete(node_delete_url)

            # delete from minio if it is the file
            if ff_label == "File":
                try:
                    mc = Minio_Client()
                    self.__logger.info("Minio Connection Success")

                    # minio location is minio://http://<end_point>/bucket/user/object_path
                    minio_path = ff_object.get('location').split("//")[-1]
                    _, bucket, obj_path = tuple(minio_path.split("/", 2))

                    mc.delete_object(bucket, obj_path)
                    self.__logger.info("Minio Delete Success")

                    # update metadata
                    num_of_files += 1; total_file_size += ff_object.get("file_size", 0)
                    dff = ConfigClass.DATASET_FILE_FOLDER + "/"
                    obj_path = obj_path[:len(dff)].replace(dff, "") + obj_path[len(dff):]
                    deleted_files.append(obj_path)
                except Exception as e:
                    self.__logger.error("error when uploading: "+str(e))

            # else we mark the folder as deleted
            else:
                # update the relative path by remove `data` at begining
                dff = ConfigClass.DATASET_FILE_FOLDER + "/"
                temp = ff_object.get("folder_relative_path")
                frp = temp[:len(dff)].replace(dff, "") + temp[len(dff):]
                deleted_files.append(frp+"/"+ff_object.get("name"))


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