import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache

SRV_NAMESPACE = os.environ.get("APP_NAME", "service_dataset")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")
CONFIG_CENTER_BASE_URL = os.environ.get("CONFIG_CENTER_BASE_URL", "NOT_SET")

def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        return vault_factory(CONFIG_CENTER_BASE_URL)

def vault_factory(config_center) -> dict:
    url = f"{config_center}/v1/utility/config/{SRV_NAMESPACE}"
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']


class Settings(BaseSettings):
    port: int = 5081
    host: str = "0.0.0.0"
    env: str = ""
    namespace: str = ""

    DATASET_FILE_FOLDER: str = "data"
    DATASET_SCHEMA_FOLDER: str = "schema"

    DATASET_CODE_REGEX: str = "^[a-z0-9]{3,32}$"

    # disk mounts
    NFS_ROOT_PATH: str = "./"
    VRE_ROOT_PATH: str = "/vre-data"
    ROOT_PATH: str = {
        "vre": "/vre-data"
    }.get(os.environ.get('namespace'), "/data/vre-storage")

    # minio
    MINIO_OPENID_CLIENT: str 
    MINIO_ENDPOINT: str
    MINIO_HTTPS: bool = False
    KEYCLOAK_URL: str
    MINIO_TEST_PASS: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    KEYCLOAK_MINIO_SECRET: str

    NEO4J_SERVICE: str
    QUEUE_SERVICE: str
    CATALOGUING_SERVICE: str
    ENTITYINFO_SERVICE: str
    ELASTIC_SEARCH_SERVICE: str
    UTILITY_SERVICE: str
    gm_queue_endpoint: str
    gm_username: str
    gm_password: str
    DATA_OPS_UTIL: str
    SEND_MESSAGE_URL: str

    RDS_HOST: str
    RDS_PORT: str
    RDS_DBNAME: str
    RDS_USER: str
    RDS_PWD: str
    RDS_SCHEMA_DEFAULT: str

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    # download secret
    DOWNLOAD_KEY: str = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT: int = 5

    MAX_PREVIEW_SIZE: int = 500000

    # dataset schema default
    ESSENTIALS_NAME: str = "essential.schema.json"
    ESSENTIALS_TPL_NAME: str = "Essential"


    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                load_vault_settings,
                env_settings,
                init_settings,
                file_secret_settings,
            )
    

@lru_cache(1)
def get_settings():
    settings =  Settings()
    return settings

class ConfigClass(object):
    settings = get_settings()

    version = "0.1.0"
    env = settings.env
    disk_namespace = settings.namespace

     # we use the subfolder to seperate the file
    # and schema in Minio
    DATASET_FILE_FOLDER = settings.DATASET_FILE_FOLDER
    DATASET_SCHEMA_FOLDER = settings.DATASET_SCHEMA_FOLDER

    DATASET_CODE_REGEX = settings.DATASET_CODE_REGEX

    # disk mounts
    NFS_ROOT_PATH = settings.NFS_ROOT_PATH
    VRE_ROOT_PATH = settings.VRE_ROOT_PATH
    ROOT_PATH = settings.ROOT_PATH

    # minio
    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = settings.MINIO_HTTPS
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    MINIO_TEST_PASS = settings.MINIO_TEST_PASS
    MINIO_TMP_PATH = ROOT_PATH + '/tmp/'
    MINIO_ACCESS_KEY = settings.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = settings.MINIO_SECRET_KEY
    KEYCLOAK_MINIO_SECRET = settings.KEYCLOAK_MINIO_SECRET

    NEO4J_SERVICE = settings.NEO4J_SERVICE + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = settings.NEO4J_SERVICE + "/v2/neo4j/"
    QUEUE_SERVICE = settings.QUEUE_SERVICE + "/v1/"
    CATALOGUING_SERVICE_V1 = settings.CATALOGUING_SERVICE + "/v1/"
    CATALOGUING_SERVICE_V2 = settings.CATALOGUING_SERVICE + "/v2/"
    COMMON_SERVICE = settings.UTILITY_SERVICE + "/v1/"
    ENTITYINFO_SERVICE = settings.ENTITYINFO_SERVICE + "/v1/"
    ELASTIC_SEARCH_SERVICE = settings.ELASTIC_SEARCH_SERVICE + "/"
    gm_queue_endpoint = settings.gm_queue_endpoint
    gm_username = settings.gm_username
    gm_password = settings.gm_password
    DATA_UTILITY_SERVICE = settings.DATA_OPS_UTIL + "/v1/"
    DATA_UTILITY_SERVICE_v2 = settings.DATA_OPS_UTIL + "/v2/"
    SEND_MESSAGE_URL = settings.SEND_MESSAGE_URL + "/v1/send_message"

    RDS_HOST = settings.RDS_HOST
    RDS_PORT = settings.RDS_PORT
    RDS_DBNAME = settings.RDS_DBNAME
    RDS_USER = settings.RDS_USER
    RDS_PWD = settings.RDS_PWD
    RDS_SCHEMA_DEFAULT = settings.RDS_SCHEMA_DEFAULT
    OPS_DB_URI = f"postgresql://{RDS_USER}:{RDS_PWD}@{RDS_HOST}/{RDS_DBNAME}"

    # Redis Service
    REDIS_HOST = settings.REDIS_HOST
    REDIS_PORT = int(settings.REDIS_PORT)
    REDIS_DB = int(settings.REDIS_DB)
    REDIS_PASSWORD = settings.REDIS_PASSWORD

    # download secret
    DOWNLOAD_KEY = settings.DOWNLOAD_KEY
    DOWNLOAD_TOKEN_EXPIRE_AT = settings.DOWNLOAD_TOKEN_EXPIRE_AT

    MAX_PREVIEW_SIZE = settings.MAX_PREVIEW_SIZE

    # dataset schema default
    ESSENTIALS_NAME = settings.ESSENTIALS_NAME
    ESSENTIALS_TPL_NAME = settings.ESSENTIALS_TPL_NAME
