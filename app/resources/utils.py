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

from ..config import ConfigClass
import requests
import os
import json


def get_files_recursive(folder_geid, all_files=[]):
    query = {
        "start_label": "Folder",
        "end_labels": ["File", "Folder"],
        "query": {
            "start_params": {
                "global_entity_id": folder_geid,
            },
            "end_params": {
            }
        }
    }
    resp = requests.post(ConfigClass.NEO4J_SERVICE_V2 +
                         "relations/query", json=query)
    for node in resp.json()["results"]:
        if "File" in node["labels"]:
            all_files.append(node)
        else:
            get_files_recursive(node["global_entity_id"], all_files=all_files)
    return all_files


def get_related_nodes(dataset_geid):
    query = {
        "start_label": "Dataset",
        "end_labels": ["File", "Folder"],
        "query": {
            "start_params": {
                "global_entity_id": dataset_geid,
            },
            "end_params": {
            }
        }
    }
    resp = requests.post(ConfigClass.NEO4J_SERVICE_V2 +
                         "relations/query", json=query)

    return resp.json()["results"]


def http_query_node(primary_label, query_params={}):
    '''
    primary_label i.e. Folder, File, Container
    '''
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/query".format(primary_label)
    response = requests.post(node_query_url, json=payload)
    return response


def get_node_relative_path(dataset_code, location):
    return location.split(dataset_code)[1]


json_data = {
    "BIDSVersion": "1.0.0",
    "Name": "False belief task",
    "Authors": ["Moran, J.M.", "Jolly, E.", "Mitchell, J.P."],
    "ReferencesAndLinks": ["Moran, J.M. Jolly, E., Mitchell, J.P. (2012). Social-cognitive deficits in normal aging. J Neurosci, 32(16):5553-61. doi: 10.1523/JNEUROSCI.5511-11.2012"]
}


def make_temp_folder(files):
    for file in files:
        if not os.path.exists(os.path.dirname(file['file_path'])):
            os.makedirs(os.path.dirname(file['file_path']))
            extension = os.path.splitext(file['file_path'])[1]

            if extension == '.json':
                with open(file['file_path'], 'wb') as outfile:
                    json.dump(json_data, outfile)
            else:
                f = open(file['file_path'], "wb")
                f.seek(file['file_size'])
                f.write(b"\0")
                f.close()
        else:
            if not os.path.exists(file['file_path']):
                extension = os.path.splitext(file['file_path'])[1]

                if extension == '.json':
                    with open('data.txt', 'w') as outfile:
                        json.dump(json_data, outfile)
                else:
                    f = open(file['file_path'], "wb")
                    f.seek(file['file_size'])
                    f.write(b"\0")
                    f.close()
