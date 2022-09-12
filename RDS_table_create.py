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

from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite 
from sqlalchemy import create_engine

from app.models.schema_sql import DatasetSchemaTemplate, DatasetSchema, Base
from app.config import ConfigClass

engine = create_engine(ConfigClass.OPS_DB_URI, echo = True)

if __name__ == "__main__":
    print(CreateTable(DatasetSchemaTemplate.__table__).compile(dialect=postgresql.dialect()))
    print(CreateTable(DatasetSchema.__table__).compile(dialect=postgresql.dialect()))

    Base.metadata.create_all(bind=engine)