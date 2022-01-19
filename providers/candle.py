from datetime import datetime


class Instrument:
    def __init__(self, instrument, provider_id):
        self.instrument = instrument
        self.provider_id = provider_id

    def to_string(self):
        return "instrument: {}, provider id: {}".format(self.instrument, self.provider_id)

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
