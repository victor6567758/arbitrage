import logging
import os
import signal
import argparse
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

from aex_provider import AexProvider
from binance_provider import BinanceProvider

parser = argparse.ArgumentParser()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format='%(asctime)s [%(module)s] - [%(message)s]')

parser.add_argument("-p", "--provider", help="Provider")
args = parser.parse_args()

provider = None

KAFKA_HOST = os.environ.get("KAFKA_HOST")
RETRY_PERIOD = 5

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
                request_timeout_ms=1000000,
                api_version_auto_timeout_ms=1000000)
            connected = True
        except NoBrokersAvailable as e:
            logging.exception("Error connection to {KAFKA_HOST}. Retrying in {RETRY_PERIOD}")
            time.sleep(RETRY_PERIOD)

    logging.info(f"Successfully connected to {KAFKA_HOST}")

    if args.provider.lower() == 'binance':
        provider = BinanceProvider()
    elif args.provider.lower() == 'aex':
        provider = AexProvider()
    else:
        raise Exception('Invalid provider')

    signal.signal(signal.SIGINT, signal_handler)
    provider.start()

    logging.log(logging.INFO, 'Press Ctrl-C to exit')
    signal.pause()

def send_message(producer, topic, message):
    try:
        logging.info('Send message: %s', message)
        result = producer.send(topic, bytes(message, 'utf-8'))
        result.get(timeout=10)
    except KafkaError as err:
        logging.log(logging.ERROR, "Kafka error {0}".format(err))

if __name__ == '__main__':
    main_start()
