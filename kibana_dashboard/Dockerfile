
FROM alpine:3.6

# install wget and unzip
RUN apk update && apk add bash curl && rm -rf /var/cache/apk/*

# create data directory
RUN mkdir -p /tmp/load/

# Move the config into the container
COPY ./export.ndjson /tmp/load/export.ndjson

# Move the script into the container
COPY ./load_ndjson.sh /tmp/load/load_ndjson.sh

# give everybody access
RUN chmod -R 777 /tmp/load/*
