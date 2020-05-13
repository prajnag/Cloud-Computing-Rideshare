FROM python:3.6
COPY . /app
WORKDIR /app
RUN apt-get -y update
RUN apt-get install -y sqlite3 libsqlite3-dev
RUN pip3 install -r requirements.txt   
CMD  python3 slave.py
