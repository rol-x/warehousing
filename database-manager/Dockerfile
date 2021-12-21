FROM python:3

COPY . .

RUN pip install checksumdir
RUN pip install mysql-connector-python
RUN pip install pandas

CMD [ "python", "-u", "./main.py" ]

