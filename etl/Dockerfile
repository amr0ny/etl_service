FROM python:3.12

WORKDIR /opt/etl
COPY requirements.txt requirements.txt

RUN chmod +x .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]