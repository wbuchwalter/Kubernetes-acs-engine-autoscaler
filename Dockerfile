FROM python:3-alpine
RUN apk update
RUN apk --update add --virtual build-dependencies \
    python3-dev python-dev libffi-dev openssl-dev build-base && \
    pip install --upgrade pip cffi cryptography && \
    apk del build-dependencies && \
    apk add --no-cache bash git && \
    rm -rf /var/cache/apk/*

COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r /app/requirements.txt
#For some reason libffi needs to be reinstalled at the end, otherwise issues will appear
RUN apk update
RUN apk add libffi-dev

COPY ./ /app/






