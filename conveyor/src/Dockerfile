FROM alpine:3.19

COPY . /app

WORKDIR /app

ENV AWS_ACCESS_KEY_ID= \
    AWS_SECRET_ACCESS_KEY= \
    AWS_REGION= \
    DATE_FROM= \
    DATE_TO=

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ingest_api_data.py"]
