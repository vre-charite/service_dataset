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

import logging
from logging.handlers import RotatingFileHandler
import os


class Logger(object):

    def __init__(self, name='log'):
        if not os.path.exists("./tests/logs/"):
            os.makedirs("./tests/logs/")
        if os.path.isfile("./tests/logs/" + name):
            os.remove("./tests/logs/" + name)
        self.name = name
        self.logger = logging.getLogger(name)
        level = logging.INFO

        self.logger.setLevel(level)
        fh = RotatingFileHandler("./tests/logs/"+name, maxBytes=500000, backupCount=1)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                                      '%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
