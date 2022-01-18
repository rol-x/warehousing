FROM python:3

COPY . .

RUN pip install mysql-connector-python
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install mysqlclient
RUN pip install matplotlib
RUN pip install seaborn

CMD [ "python", "-u", "./main.py" ]
