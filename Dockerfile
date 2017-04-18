FROM python:3-alpine
RUN apk update
RUN apk --update add --virtual build-dependencies \
      python3-dev python-dev libffi-dev openssl-dev build-base && \
    pip install --upgrade pip cffi cryptography && \
    apk del build-dependencies && \
    apk add --no-cache bash git && \
    rm -rf /var/cache/apk/*

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
#RUN git clone https://github.com/Azure/azure-cli/
# Temporary until https://github.com/Azure/azure-cli/issues/2548 is fixed
#RUN cd ./azure-cli && git checkout 41ecccd 
#RUN python ./azure-cli/scripts/dev_setup.py
COPY . /app/
WORKDIR /app
#For some reason libffi needs to be reinstalled at the end, otherwise issues will appear
RUN apk update
RUN apk add libffi-dev





