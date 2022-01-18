FROM python:3.8-slim

COPY . .

RUN pip install checksumdir mysql-connector-python pandas

CMD [ "python", "-u", "./main.py" ]

