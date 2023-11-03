FROM python:3.10
RUN apt-get update && apt-get -y install cron nano
COPY requirements.txt /opt/asx/requirements.txt
WORKDIR /opt/asx
RUN pip install -r requirements.txt
COPY . /opt/asx
COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
COPY README.md README.md
RUN echo $PYTHONPATH
# run crond as main process of container
CMD ["cron", "-f"]



