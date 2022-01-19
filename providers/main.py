import logging
import os
import signal

from aex_provider import AexProvider
from binance_provider import BinanceProvider

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "DEBUG"))
binance_provider = BinanceProvider()
aex_provider = AexProvider()


def signal_handler(sig, frame):
    logging.log(logging.INFO, 'Stopping...')
    #binance_provider.shutdown()
    aex_provider.shutdown()

    os._exit(os.EX_OK)
    # sys.exit()


def main_start():
    signal.signal(signal.SIGINT, signal_handler)
    aex_provider.start()
    #binance_provider.start()

    logging.log(logging.INFO, 'Press Ctrl-C to exit')
    signal.pause()


if __name__ == '__main__':
    main_start()
