import logging
from abc import abstractmethod


class BaseProvider:
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    def process_ohlc(self, candle):
        logging.log(logging.INFO, 'Processing candle {}'.format(candle))