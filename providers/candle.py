import json
from datetime import datetime


class Instrument:
    def __init__(self, coin, market, provider_id, instrument_generator):
        self.coin = coin
        self.market = market
        self.provider_id = provider_id
        self.instrument_generator = instrument_generator
        self.internal_instrument = coin + market;

    def to_string(self):
        return "Instrument: {}/{}, provider id: {}".format(self.coin, self.market, self.provider_id)

    def instrument(self):
        return self.instrument_generator(self.coin, self.market)

    def __repr__(self):
        return self.to_string()

    def __str__(self):
        return self.to_string()


class Candle:
    def __init__(self):
        self.instrument = None
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = -0.0
        self.volume = 0
        self.datetime = -1

    def __init__(self, instrument, close_time, open, high, low, close, volume):
        self.instrument = instrument
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.datetime = close_time

    def to_string(self):
        return "instrument: {}, open: {}, high: {}, low: {}, close: {}, volume: {}, datetime: {}".format(
            self.instrument, self.open, self.high, self.low, self.close, self.volume,
            datetime.utcfromtimestamp(self.datetime / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    def __repr__(self):
        return self.to_string()

    def __str__(self):
        return self.to_string()

    def to_json(self):
        return json.dumps(self).encode('utf-8')

    def to_dict(self):
        return {'p': self.instrument.provider_id, 's': self.instrument.internal_instrument,
                't': self.datetime, 'o': self.open, 'h': self.high, 'l': self.low, 'c': self.close, 'v': self.volume}
