FROM python:3.7-buster
USER root
WORKDIR /usr/src/app

# set timezone
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y less
COPY kubernetes/mc /usr/local/bin
RUN chmod +x /usr/local/bin/mc
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x gunicorn_starter.sh
#CMD ["./gunicorn_starter.sh"]
CMD ["sh", "-c", "mc alias set minio http://minio.minio:9000 indoc-minio Trillian42! && ./gunicorn_starter.sh"]
