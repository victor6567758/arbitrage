import logging
from abc import abstractmethod


class BaseProvider:

    def __init__(self, senderLambda, id, name, instrument):
        self.senderLambda = senderLambda
        self.id = id
        self.name = name
        self.instrument = instrument

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    def get_name(self):
        return self.name

    def get_id(self):
        return self.id

    def get_instrument(self):
        return self.instrument

    def process_ohlc(self, candle):
        if self.senderLambda:
            self.senderLambda(self.get_name(), self.get_id(), candle)
        else:
            logging.log(logging.INFO, 'Sender not implemented for candle {}'.format(candle))