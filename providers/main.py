import argparse
import json
import logging
import os
import signal
import time
import yaml

from kafka.errors import NoBrokersAvailable, KafkaError

import aex_provider
import binance_provider
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format='%(asctime)s [%(module)s] - [%(message)s]')

parser.add_argument("-p", "--provider", help="Provider")
parser.add_argument("-c", "--config", help="Config yaml path")
args = parser.parse_args()

provider_g = None

KAFKA_HOST = os.environ.get("KAFKA_HOST")

RETRY_PERIOD = 5
KAFKA_TOPIC = 'OHCL'


def signal_handler(sig, frame):
    logging.log(logging.INFO, 'Stopping...')
    provider_g.shutdown()

    os._exit(os.EX_OK)
    # sys.exit()


def main_start():
    logging.log(logging.INFO, 'Starting')

    provider_name = args.provider.lower()

    provider_config = read_provider_config(read_config_file(args.config), provider_name)

    producer = connect_kafka()

    def send_lambda(name, id, data):
        send_message(producer, KAFKA_TOPIC, data)

    global provider_g
    if provider_name == 'binance':
        provider_g = binance_provider.create_from_configuration(provider_name, provider_config, send_lambda)
    elif provider_name == 'aex':
        provider_g = aex_provider.create_from_configuration(provider_name, provider_config, send_lambda)
    else:
        raise Exception('Invalid provider', provider_name)

    signal.signal(signal.SIGINT, signal_handler)
    provider_g.start()

    signal.pause()


def read_config_file(config_path):
    logging.info('Trying to read configuration from {}'.format(config_path))
    with open(config_path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def read_provider_config(config_data, provider_name):
    provider_config = config_data['providers'].get(provider_name)
    if not provider_config:
        raise Exception('Invalid provider {}'.format(provider_name))
    return provider_config


def connect_kafka():
    producer = None
    connected = False
    while not connected:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_HOST],
                value_serializer=lambda v: json.dumps(v.to_dict()).encode('utf-8'),
                request_timeout_ms=1000000,
                api_version_auto_timeout_ms=1000000)
            connected = True
        except NoBrokersAvailable as e:
            logging.exception(f"Error connection to {KAFKA_HOST}. Retrying in {RETRY_PERIOD}")
            time.sleep(RETRY_PERIOD)

    logging.info(f"Successfully connected to {KAFKA_HOST}")
    return producer


def send_message(producer, topic, message):
    try:
        logging.info("{}, {}, '{}', {}, {}, {}, {}, {}".format(message.datetime, message.instrument.provider_id,
                                                               message.instrument.coin + message.instrument.market,
                                                               message.open, message.high, message.low, message.close,
                                                               message.volume))
        result = producer.send(topic, message)
        result.get(timeout=10)
    except KafkaError as err:
        logging.log(logging.ERROR, "Kafka error %s", err)


if __name__ == '__main__':
    main_start()
