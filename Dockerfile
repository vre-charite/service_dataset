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

FROM python:3.7-buster
ARG MINIO_USERNAME
ARG MINIO_PASSWORD

ARG PIP_USERNAME
ARG PIP_PASSWORD

ENV MINIO_USERNAME=$MINIO_USERNAME
ENV MINIO_PASSWORD=$MINIO_PASSWORD
ENV TZ=America/Toronto

WORKDIR /usr/src/app

ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && apt-get update && \
apt-get install -y vim-tiny less && ln -s /usr/bin/vim.tiny /usr/bin/vim && rm -rf /var/lib/apt/lists/*

COPY kubernetes/mc /usr/local/bin
RUN chmod +x /usr/local/bin/mc
COPY . .

RUN PIP_USERNAME=$PIP_USERNAME PIP_PASSWORD=$PIP_PASSWORD pip install -r requirements.txt -r internal_requirements.txt && chmod +x gunicorn_starter.sh
#CMD ["./gunicorn_starter.sh"]
# TODO: remove the minio credentials from here and put kubernetes folder into dockerignore file again
CMD ["sh", "-c", "mc alias set minio http://minio.minio:9000 $MINIO_USERNAME $MINIO_PASSWORD && ./gunicorn_starter.sh"]
