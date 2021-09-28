FROM 10.32.42.225:5000/python:3.7-buster
USER root
WORKDIR /usr/src/app
COPY kubernetes/mc /usr/local/bin
RUN chmod +x /usr/local/bin/mc
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt --proxy="http://proxy.charite.de:8080/"
COPY . .
RUN chmod +x gunicorn_starter.sh
#CMD ["./gunicorn_starter.sh"]
# CMD ["python","app.py"]
CMD ["sh", "-c", "mc alias set minio http://minio.minio:9000 indoc-minio Trillian42! && ./gunicorn_starter.sh"]
