FROM python:3

COPY . .

RUN pip install pandas
RUN pip install checksumdir

EXPOSE 80

CMD [ "python", "-u", "./main.py" ]

