FROM python:3.9-alpine

COPY . /app

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pull_data_pl.py"]
