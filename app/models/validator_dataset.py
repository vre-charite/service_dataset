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

import re
from app.config import ConfigClass

class DatasetValidator:
    
    @staticmethod
    def get(key):
        return {
            "code": DatasetValidator.code,
            "title": DatasetValidator.title,
            "authors": DatasetValidator.authors,
            "type": DatasetValidator.type,
            "modality": DatasetValidator.modality,
            "collection_method": DatasetValidator.collection_methods,
            "license": DatasetValidator.license,
            "tags": DatasetValidator.tags,
            "description": DatasetValidator.desc
        }.get(key, lambda v: v is not None)

    @staticmethod
    def code(value: str):
        project_code_pattern = re.compile(ConfigClass.DATASET_CODE_REGEX)
        is_match = re.search(project_code_pattern, value)

        return True if is_match else False

    @staticmethod
    def title(value):
        if len(value) > 100:
            return False
        return True

    @staticmethod
    def authors(value):

        # 1. type validation
        if not isinstance(value, list):
            print(value)
            return False

        # 2. non-duplicate validtion
        if len(set(value)) != len(value):
            return False

        # 3. each of collection cannot be empty
        # AND the length should be less or equal than 20
        # AND the type of each authors should be string
        for val in value:
            if len(val.replace(" ", "")) == 0:
                return False
            elif len(val) > 50:
                return False
            elif type(val) != str:
                return False

        # 4. total number of authors should be less or equal than 10
        if len(value) > 10:
            return False
        return True

    @staticmethod
    def desc(value):
        if len(value) > 5000:
            return False
        return True

    @staticmethod
    def modality(value: list):
        allowed = [
            "anatomical approach",
            "neuroimaging",
            "microscopy",
            "histological approach",
            "neural connectivity",
            "molecular expression characterization",
            "multimodal approach", 
            "electrophysiology",
            "behavioral approach",
            "molecular expression approach",
            "cell population imaging",
            "physiological approach", 
            "morphological approach", 
            "cell morphology", 
            "cell counting",
            "cell population characterization",
            "computational modeling"]
            
        for e in value:
            if e not in allowed:
                return False
        return True

    @staticmethod
    def type(value):
        allowed = ["GENERAL", "BIDS"]
        if value not in allowed:
            return False
        return True

    @staticmethod
    def tags(value):
        if not isinstance(value, list):
            return False
        if len(set(value)) != len(value):
            return False
        for val in value:
            if " " in val:
                return False
            elif type(val) != str:
                return False

        if len(value) > 10:
            return False

        return True

    @staticmethod
    def collection_methods(value):

        # 1. type validation
        if not isinstance(value, list):
            return False

        # 2. non-duplicate validtion
        if len(set(value)) != len(value):
            return False

        # 3. each of collection cannot be empty
        # AND the length should be less or equal than 20
        # AND the type of each authors should be string
        for val in value:
            if len(val.replace(" ", "")) == 0:
                return False
            elif len(val) > 20:
                return False
            elif type(val) != str:
                return False

        
        # 4. total number of collection should be less or equal than 10
        if len(value) > 10:
            return False

        return True

    @staticmethod
    def license(value):
        if len(value) > 20:
            return False
            
        return True