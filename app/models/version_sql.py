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
from sqlalchemy import Column, String, Date, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from app.config import ConfigClass

from datetime import datetime

Base = declarative_base()

class DatasetVersion(Base):
    __tablename__ = 'dataset_version'
    __table_args__ = {"schema":ConfigClass.RDS_SCHEMA_DEFAULT}
    id = Column(Integer, unique=True, primary_key=True)
    dataset_code = Column(String())
    dataset_geid = Column(String())
    version = Column(String())
    created_by = Column(String())
    created_at = Column(DateTime(), default=datetime.utcnow)
    location = Column(String())
    notes = Column(String())

    def __init__(self, dataset_code, dataset_geid, version, created_by, location, notes):
        self.dataset_code = dataset_code
        self.dataset_geid = dataset_geid 
        self.version = version
        self.created_by = created_by 
        self.location = location
        self.notes = notes 

    def to_dict(self):
        result = {}
        for field in ["id", "dataset_code", "dataset_geid", "version", "created_by", "created_at", "location", "notes"]:
            if field == "created_at":
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            else:
                result[field] = str(getattr(self, field))
        return result


