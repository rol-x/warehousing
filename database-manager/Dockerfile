FROM python:3

COPY . .

RUN pip install pandas
RUN pip install checksumdir
RUN pip install mysql-connector-python

CMD [ "python", "-u", "./main.py" ]

