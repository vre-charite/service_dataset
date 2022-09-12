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

from app.config import ConfigClass
from fastapi_sqlalchemy import db
from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.commons.service_connection.minio_client import Minio_Client
from app.models.version_sql import DatasetVersion

from app.resources.error_handler import APIException
from app.resources.helpers import get_geid
from app.resources.neo4j_helper import get_children_nodes
from app.resources.locks import recursive_lock_publish, unlock_resource

from app.models.schema_sql import DatasetSchema

from redis import Redis
from datetime import datetime
import requests
import time
import shutil
import json
import os

logger = SrvLoggerFactory("api_version").get_logger()


def parse_minio_location(location):
    minio_path = location.split("//")[-1]
    _, bucket, obj_path = tuple(minio_path.split("/", 2))
    return {"bucket": bucket, "path": obj_path}


def get_dataset_by_geid(dataset_geid):
    payload = {
        "global_entity_id": dataset_geid
    }
    response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Dataset/query", json=payload)
    if not response.json():
        raise APIException(status_code=404, error_msg="Dataset not found")
    return response.json()[0]


class PublishVersion(object):
    def __init__(self, dataset_node, operator, notes, status_id, version):
        self.operator = operator
        self.notes = notes
        self.dataset_node = dataset_node
        self.dataset_geid = dataset_node["global_entity_id"]
        self.dataset_files = []
        tmp_base = "/tmp/"
        self.tmp_folder = tmp_base  + str(time.time()) + "/"
        self.zip_path = tmp_base + dataset_node["code"] + "_" + str(datetime.now())
        self.mc = Minio_Client()
        self.redis_client = Redis(
            host=ConfigClass.REDIS_HOST,
            port=ConfigClass.REDIS_PORT,
            password=ConfigClass.REDIS_PASSWORD,
            db=ConfigClass.REDIS_DB,
        )
        self.status_id = status_id
        self.update_status("inprogress")
        self.version = version

    def publish(self):
        try:
            # TODO some merge needed here since get_children_nodes and 
            # get_dataset_files_recursive both get the nodes under the dataset

            # lock file here
            level1_nodes = get_children_nodes(self.dataset_geid, start_label="Dataset")
            locked_node, err = recursive_lock_publish(level1_nodes)
            if err: raise err

            self.get_dataset_files_recursive(self.dataset_geid)
            self.download_dataset_files()
            self.add_schemas()
            self.zip_files()
            minio_location = self.upload_version()
            try:
                dataset_version = DatasetVersion(
                    dataset_code=self.dataset_node["code"],
                    dataset_geid=self.dataset_geid,
                    version=str(self.version),
                    created_by=self.operator,
                    location=minio_location,
                    notes=self.notes,
                )
                db.session.add(dataset_version)
                db.session.commit()
            except Exception as e:
                logger.error("Psql Error: " + str(e))
                raise e

            logger.info(f"Successfully published {self.dataset_geid} version {self.version}")
            self.update_activity_log()
            self.update_status("success")
        except Exception as e:
            error_msg = f"Error publishing {self.dataset_geid}: {str(e)}"
            logger.error(error_msg)
            self.update_status("failed", error_msg=error_msg)
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                unlock_resource(resource_key, operation)

        return

    def update_activity_log(self):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_PUBLISH_SUCCEED",
            "payload": {
                "dataset_geid": self.dataset_geid,
                "act_geid": get_geid(),
                "operator": self.operator,
                "action": "PUBLISH",
                "resource": "Dataset",
                "detail": {
                    "source": self.version
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
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            logger.error(error_msg)
            raise Exception(error_msg)
        return res


    def update_status(self, status, error_msg=""):
        """
            Updates job status in redis
        """
        redis_status = json.dumps({
            "status": status,
            "error_msg": error_msg,
        })
        self.redis_client.set(
            self.status_id,
            redis_status,
            ex=1*60*60
        )

    def get_dataset_files_recursive(self, geid, start_label="Dataset"):
        """
        get all files from dataset
        """
        query = {
            "start_label": start_label,
            "end_labels": ["File", "Folder"],
            "query": {
                "start_params": {
                    "global_entity_id": geid,
                },
                "end_params": {
                    "archived": False,
                }
            }
        }
        resp = requests.post(ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=query)
        for node in resp.json()["results"]:
            if "File" in node["labels"]:
                self.dataset_files.append(node)
            else:
                self.get_dataset_files_recursive(node["global_entity_id"], start_label="Folder")
        return self.dataset_files

    def download_dataset_files(self):
        """
            Download files from minio 
        """
        file_paths = []
        for file in self.dataset_files:
            location_data = parse_minio_location(file["location"])
            try:
                self.mc.client.fget_object(
                    location_data["bucket"], 
                    location_data["path"], 
                    self.tmp_folder + "/" + location_data["path"]
                )
                file_paths.append(self.tmp_folder + "/" + location_data["path"])
            except Exception as e:
                error_msg = f"Error download files from minio: {str(e)}"
                logger.error(error_msg)
                raise Exception(error_msg)
        return file_paths 

    def zip_files(self):
        shutil.make_archive(self.zip_path, "zip", self.tmp_folder)
        return self.zip_path

    def add_schemas(self):
        """ 
            Saves schema json files to folder that will zipped
        """
        if not os.path.isdir(self.tmp_folder):
            os.mkdir(self.tmp_folder)
            os.mkdir(self.tmp_folder + "/data")

        schemas = db.session.query(DatasetSchema).filter_by(dataset_geid=self.dataset_geid, standard="default", is_draft=False).all()
        for schema in schemas:
            with open(self.tmp_folder + "/default_" + schema.name, 'w') as w:
                w.write(json.dumps(schema.content, indent=4, ensure_ascii=False))
        schemas = db.session.query(DatasetSchema).filter_by(dataset_geid=self.dataset_geid, standard="open_minds", is_draft=False).all()
        for schema in schemas:
            with open(self.tmp_folder + "/openMINDS_" + schema.name, 'w') as w:
                w.write(json.dumps(schema.content, indent=4, ensure_ascii=False))

    def upload_version(self):
        """
            Upload version zip to minio
        """
        bucket = self.dataset_node["code"]
        path = "versions/" + self.zip_path.split("/")[-1] + ".zip"
        try:
            self.mc.client.fput_object(
                bucket,
                path,
                self.zip_path + ".zip",
            )
            minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
            minio_location = f"minio://{minio_http}/{bucket}/{path}"
        except Exception as e:
            error_msg = f"Error uploading files to minio: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        return minio_location


