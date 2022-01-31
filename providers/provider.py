import logging
from abc import abstractmethod

from candle import Instrument


class BaseProvider:

    def __init__(self, sender_lambda, id: int, name: str, instruments: list, internal_instrument_name_lambda):
        self.sender_lambda = sender_lambda
        self.id = id
        self.name = name
        self.instruments = instruments
        self.internal_instrument_name_lambda = internal_instrument_name_lambda
        self.name_instrument_dict = dict()

        for instrument in instruments:
            self.name_instrument_dict[self.generate_internal_name(instrument)] = instrument

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    def get_name_instrument_dict(self):
        return self.name_instrument_dict

    def check_instruments_internal(self, full_trading_set, instruments_set):
        if not instruments_set.issubset(full_trading_set):
            raise Exception('Not all passed symbols can be processed by a provider')

    def generate_internal_name(self, instrument: Instrument) -> str:
        return self.internal_instrument_name_lambda(instrument.coin, instrument.market)

    def get_name(self):
        return self.name

    def get_id(self):
        return self.id

    def get_instruments(self) -> list:
        return self.instruments

    def process_ohlc(self, candle):
        if self.sender_lambda:
            self.sender_lambda(self.get_name(), self.get_id(), candle)
        else:
            logging.log(logging.INFO, 'Sender not implemented for candle {}'.format(candle))