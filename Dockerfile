FROM python:3

ADD . /

RUN pip install pandas
RUN pip install beautifulSoup4
RUN pip install selenium

EXPOSE 4444

CMD [ "python", "-u", "./main.py" ]

