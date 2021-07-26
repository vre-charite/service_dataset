import os


class ConfigClass(object):
    env = os.environ.get('env', 'test')
    version = "0.1.0"

    # we use the subfolder to seperate the file
    # and schema in Minio
    DATASET_FILE_FOLDER = "data"
    DATASET_SCHEMA_FOLDER = "schema"
    # minio config
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "minio.minio:9000"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://keycloak.utility:8080"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"

    if env == "test":
        # minio config
        MINIO_ENDPOINT = "10.3.7.220"
        MINIO_HTTPS = False
        KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY


    # # Minio config
    # MINIO_OPENID_CLIENT = "react-app"
    # if env == "staging":
    #     MINIO_ENDPOINT = "minio.minio:9000"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://10.3.7.240:80"
    #     MINIO_TEST_PASS = "IndocStaging2021!"
    # else:
    #     MINIO_ENDPOINT = "10.3.7.220"
    #     MINIO_HTTPS = False
    #     # KEYCLOAK_URL = "http://keycloak.utility:8080"
    #     KEYCLOAK_URL = "http://10.3.7.220"  # for local test ONLY
    #     MINIO_TEST_PASS = "admin"

    if env == 'test':
        NEO4J_SERVICE = "http://10.3.7.216:5062/v1/neo4j/"
        NEO4J_SERVICE_V2 = "http://10.3.7.216:5062/v2/neo4j/"
        QUEUE_SERVICE = "http://10.3.7.214:6060/v1/"
        CATALOGUING_SERVICE_V2 = "http://10.3.7.237:5064/v2/"
        COMMON_SERVICE = "http://10.3.7.222:5062/v1/"
        ENTITYINFO_SERVICE = "http://10.3.7.228:5066/v1/"
        CATALOGUING_SERVICE_V1 = "http://10.3.7.237:5064/v1/"
        ELASTIC_SEARCH_SERVICE = "http://10.3.7.219:9200/"
        UTILITY_SERVICE = "http://10.3.7.222:5062/v1/"
        gm_queue_endpoint = '10.3.7.232'
        gm_username = 'greenroom'
        gm_password = 'indoc101'
    elif env == 'charite':
        NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"
        NEO4J_SERVICE_V2 = "http://neo4j.utility:5062/v2/neo4j/"
        QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"
        CATALOGUING_SERVICE_V2 = "http://cataloguing.utility:5064/v2/"
        COMMON_SERVICE = "http://common.utility:5062/v1/"
        ENTITYINFO_SERVICE = "http://entityinfo.utility:5066/v1/"
        CATALOGUING_SERVICE_V1 = "http://cataloguing.utility:5064/v1/"
        ELASTIC_SEARCH_SERVICE = "http://elasticsearch-master.utility:9200/"
        UTILITY_SERVICE = "http://common.utility:5062/v1/"
        gm_queue_endpoint = 'message-bus-greenroom.greenroom'
        gm_username = 'greenroom'
        gm_password = 'rabbitmq-jrjmfa9svvC'
    else:
        NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"
        NEO4J_SERVICE_V2 = "http://neo4j.utility:5062/v2/neo4j/"
        QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"
        CATALOGUING_SERVICE_V2 = "http://cataloguing.utility:5064/v2/"
        COMMON_SERVICE = "http://common.utility:5062/v1/"
        ENTITYINFO_SERVICE = "http://entityinfo.utility:5066/v1/"
        CATALOGUING_SERVICE_V1 = "http://cataloguing.utility:5064/v1/"
        ELASTIC_SEARCH_SERVICE = "http://elasticsearch-master.utility:9200/"
        UTILITY_SERVICE = "http://common.utility:5062/v1/"
        gm_queue_endpoint = 'message-bus-greenroom.greenroom'
        gm_username = 'greenroom'
        gm_password = 'indoc101'

    MAX_PREVIEW_SIZE = 500000
