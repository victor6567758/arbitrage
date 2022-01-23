import argparse
import json
import logging
import os
import signal
import time

from kafka.errors import NoBrokersAvailable, KafkaError

from aex_provider import AexProvider
from binance_provider import BinanceProvider
from candle import Instrument
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format='%(asctime)s [%(module)s] - [%(message)s]')

parser.add_argument("-p", "--provider", help="Provider")
args = parser.parse_args()

provider = None

KAFKA_HOST = 'kafka1:19092'
RETRY_PERIOD = 5

BINANCE_PROVIDER_ID = 1
BINANCE_PROVIDER_NAME = 'BINANCE_PROVIDER'

AEX_PROVIDER_ID = 2
AEX_PROVIDER_NAME = 'AEX_PROVIDER_NAME'

KAFKA_TOPIC='OHCL'


def signal_handler(sig, frame):
    logging.log(logging.INFO, 'Stopping...')
    provider.shutdown()

    os._exit(os.EX_OK)
    # sys.exit()


def main_start():
    logging.log(logging.INFO, 'Starting')

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

    if args.provider.lower() == 'binance':
        provider = BinanceProvider(lambda name, id, data: send_message(producer, KAFKA_TOPIC, data), BINANCE_PROVIDER_ID,
                                   BINANCE_PROVIDER_NAME,
                                   Instrument('SYS', 'USDT', BINANCE_PROVIDER_ID, lambda x, y: x + y))
    elif args.provider.lower() == 'aex':
        provider = AexProvider(lambda name, id, data: send_message(producer, KAFKA_TOPIC, data), AEX_PROVIDER_ID,
                               AEX_PROVIDER_NAME,
                               Instrument('SYS', 'USDT', AEX_PROVIDER_ID, lambda x, y: (x + '_' + y).lower()))
    else:
        raise Exception('Invalid provider')

    signal.signal(signal.SIGINT, signal_handler)
    provider.start()

    logging.log(logging.INFO, 'Press Ctrl-C to exit')
    signal.pause()


def send_message(producer, topic, message):
    try:
        logging.info('Sending message: %s', message)
        result = producer.send(topic, message)
        result.get(timeout=10)
    except KafkaError as err:
        logging.log(logging.ERROR, "Kafka error %s", err)


if __name__ == '__main__':
    main_start()
