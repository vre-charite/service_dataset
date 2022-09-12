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

from fastapi_sqlalchemy import db
from sqlalchemy import Column, String, Date, DateTime, Integer, Boolean, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from app.config import ConfigClass
from datetime import datetime

Base = declarative_base()


class BIDSResult(Base):
    __tablename__ = 'bids_results'
    __table_args__ = {"schema": ConfigClass.RDS_SCHEMA_DEFAULT}
    id = Column(Integer, unique=True, primary_key=True)
    dataset_geid = Column(String())
    created_time = Column(DateTime(), default=datetime.utcnow)
    updated_time = Column(DateTime(), default=datetime.utcnow)
    validate_output = Column(JSON())

    def __init__(self, dataset_geid, created_time, updated_time, validate_output):
        self.dataset_geid = dataset_geid
        self.created_time = created_time
        self.updated_time = updated_time
        self.validate_output = validate_output

    def to_dict(self):
        result = {}
        for field in ["id", "dataset_geid", "created_time", "updated_time", "validate_output"]:
            if field == "created_time" or field == "updated_time":
                result[field] = str(
                    getattr(self, field).isoformat()[:-3] + 'Z')
            elif field == "validate_output":
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result
