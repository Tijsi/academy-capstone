FROM public.ecr.aws/datamindedacademy/capstone:v3.4.1-hadoop-3.3.6-v1


COPY . /app

WORKDIR /app

ENV AWS_ACCESS_KEY_ID= \
    AWS_SECRET_ACCESS_KEY= \
    AWS_REGION=

USER 0

RUN python3 -m pip install -r requirements.txt

CMD ["python3", "pull_data.py"]
