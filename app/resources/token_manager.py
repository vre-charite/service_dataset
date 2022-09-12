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

import jwt
from app.config import ConfigClass


def verify_token(token):
    '''
    verify download token with the download key
    '''
    try:
        res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
        return True, res
    except jwt.ExpiredSignatureError:
        return False, "expired"
    except Exception as e:
        return False, "invalid"


def generate_token(payload: dict):
    '''
    generate jwt token with the download key
    '''
    return jwt.encode(payload, key=ConfigClass.DOWNLOAD_KEY, algorithm='HS256').decode('utf-8')

