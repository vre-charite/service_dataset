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

############ DEPRECATE THIS FILE

from ..config import ConfigClass
import requests

def lock_resource(resource_key):
    '''
    lock resource
    '''
    url = ConfigClass.DATA_UTILITY_SERVICE + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.post(url, json=post_json)
    return response

def check_lock(resource_key):
    '''
    get resource lock
    '''
    url = ConfigClass.DATA_UTILITY_SERVICE + 'resource/lock'
    params = {
        "resource_key": resource_key
    }
    response = requests.get(url, params=params)
    return response

def unlock_resource(resource_key):
    '''
    unlock resource
    '''
    url = ConfigClass.DATA_UTILITY_SERVICE + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.delete(url, json=post_json)
    return response