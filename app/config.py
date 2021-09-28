import os
import requests
from requests.models import HTTPError
# os.environ['env'] = 'test'
srv_namespace = "service_dataset"
CONFIG_CENTER = "http://10.3.7.222:5062" \
    if os.environ.get('env', "test") == "test" \
    else "http://common.utility:5062"


def vault_factory() -> dict:
    url = CONFIG_CENTER + \
        "/v1/utility/config/{}".format(srv_namespace)
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']


class ConfigClass(object):
    vault = vault_factory()
    env = os.environ.get('env')
    disk_namespace = os.environ.get('namespace')
    version = "0.1.0"
    # we use the subfolder to seperate the file
    # and schema in Minio
    DATASET_FILE_FOLDER = "data"
    DATASET_SCHEMA_FOLDER = "schema"

    # disk mounts
    NFS_ROOT_PATH = "./"
    VRE_ROOT_PATH = "/vre-data"
    ROOT_PATH = {
        "vre": "/vre-data"
    }.get(os.environ.get('namespace'), "/data/vre-storage")

    # minio
    MINIO_OPENID_CLIENT = vault['MINIO_OPENID_CLIENT']
    MINIO_ENDPOINT = vault['MINIO_ENDPOINT']
    MINIO_HTTPS = False
    KEYCLOAK_URL = vault['KEYCLOAK_URL']
    MINIO_TEST_PASS = vault['MINIO_TEST_PASS']
    MINIO_TMP_PATH = ROOT_PATH + '/tmp/'
    MINIO_ACCESS_KEY = vault['MINIO_ACCESS_KEY']
    MINIO_SECRET_KEY = vault['MINIO_SECRET_KEY']

    NEO4J_SERVICE = vault['NEO4J_SERVICE']+"/v1/neo4j/"
    NEO4J_SERVICE_V2 = vault['NEO4J_SERVICE']+"/v2/neo4j/"
    QUEUE_SERVICE = vault['QUEUE_SERVICE']+"/v1/"
    CATALOGUING_SERVICE_V2 = vault['CATALOGUING_SERVICE']+"/v2/"
    COMMON_SERVICE = vault['UTILITY_SERVICE']+"/v1/"
    ENTITYINFO_SERVICE = vault['ENTITYINFO_SERVICE']+"/v1/"
    CATALOGUING_SERVICE_V1 = vault['CATALOGUING_SERVICE']+"/v1/"
    ELASTIC_SEARCH_SERVICE = vault['ELASTIC_SEARCH_SERVICE']+"/"
    UTILITY_SERVICE = vault['UTILITY_SERVICE']+"/v1/"
    gm_queue_endpoint = vault['gm_queue_endpoint']
    gm_username = vault['gm_username']
    gm_password = vault['gm_password']
    DATA_UTILITY_SERVICE = vault['DATA_OPS_UTIL'] + "/v1/"
    DATA_UTILITY_SERVICE_v2 = vault['DATA_OPS_UTIL'] + "/v2/"
    SEND_MESSAGE_URL = vault['SEND_MESSAGE_URL'] + "/v1/send_message"

    RDS_HOST = vault['RDS_HOST']
    RDS_PORT = vault['RDS_PORT']
    RDS_DBNAME = vault['RDS_DBNAME']
    RDS_USER = vault['RDS_USER']
    RDS_PWD = vault['RDS_PWD']
    RDS_SCHEMA_DEFAULT = vault['RDS_SCHEMA_DEFAULT']
    OPS_DB_URI = f"postgresql://{RDS_USER}:{RDS_PWD}@{RDS_HOST}/{RDS_DBNAME}"

    # Redis Service
    REDIS_HOST = vault['REDIS_HOST']
    REDIS_PORT = int(vault['REDIS_PORT'])
    REDIS_DB = int(vault['REDIS_DB'])
    REDIS_PASSWORD = vault['REDIS_PASSWORD']

    # download secret
    DOWNLOAD_KEY = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT = 5

    MAX_PREVIEW_SIZE = 500000

    # dataset schema default
    ESSENTIALS_NAME = "essential.schema.json"
    ESSENTIALS_TPL_NAME = "Essential"
