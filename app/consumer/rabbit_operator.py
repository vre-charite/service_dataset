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

import pika
import json
from ..config import ConfigClass


class RabbitConnection:
    # This class used to initiate Queue connection
    def __init__(self):
        pass

    def init_connection(self):
        try:
            credentials = pika.PlainCredentials(
                ConfigClass.gm_username,
                ConfigClass.gm_password)
            self._instance = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfigClass.gm_queue_endpoint,
                    heartbeat=180,
                    credentials=credentials)
            )
            return self._instance
        except Exception as e:
            raise

    def close_connection(self):
        self._instance.close()

    def get_current_connection(self):
        return self._instance
