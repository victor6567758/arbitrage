#!/bin/bash
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/msg_processing.py -d
