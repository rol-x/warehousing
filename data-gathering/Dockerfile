FROM python:3

COPY . .

RUN pip install beautifulSoup4
RUN pip install checksumdir
RUN pip install selenium
RUN pip install pandas

CMD [ "python", "-u", "./main.py" ]

