FROM python:3

COPY ./etl ./etl
COPY main.py .

RUN pip install pandas
RUN pip install beautifulSoup4
RUN pip install selenium

EXPOSE 4444

CMD [ "python", "-u", "./main.py" ]

