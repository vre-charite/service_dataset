FROM python:3.7-buster

WORKDIR /usr/src/app

ENV TZ=America/Toronto

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && apt-get update && \
apt-get install -y vim-tiny less && ln -s /usr/bin/vim.tiny /usr/bin/vim && rm -rf /var/lib/apt/lists/*

COPY kubernetes/mc /usr/local/bin
RUN chmod +x /usr/local/bin/mc

COPY . .

RUN pip install --no-cache-dir -r requirements.txt && chmod +x gunicorn_starter.sh

#CMD ["./gunicorn_starter.sh"]
# TODO: remove the minio credentials from here and put kubernetes folder into dockerignore file again
CMD ["sh", "-c", "mc alias set minio http://minio.minio:9000 indoc-minio Trillian42! && ./gunicorn_starter.sh"]
