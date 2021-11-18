FROM python:3

COPY . .

RUN pip install checksumdir
RUN pip install mysqlclient
RUN pip install sqlalchemy
RUN pip install pandas

CMD [ "python", "-u", "./main.py" ]

