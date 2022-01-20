import logging
import os

from binance import Client, ThreadedWebsocketManager

from candle import Candle, Instrument
from provider import BaseProvider

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
BINANCE_PROVIDER_ID = 1
INSTRUMENT = Instrument('SYS', 'USDT', BINANCE_PROVIDER_ID, lambda x, y: x + y)


class BinanceProvider(BaseProvider):
    def __init__(self):

        self.client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.twm = ThreadedWebsocketManager()

    def start(self):
        logging.log(logging.INFO, 'Started')

        if not self.check_instrument():
            raise Exception('Cannot find symbol {}'.format(INSTRUMENT))

        klines = self.client.get_historical_klines(INSTRUMENT.instrument(), Client.KLINE_INTERVAL_1MINUTE,
                                                   "1 day ago UTC")
        self.from_klines(klines)

        self.twm.start()
        self.twm.start_kline_socket(callback=self.handle_socket_message, symbol=INSTRUMENT.instrument())

    def shutdown(self):
        logging.log(logging.INFO, 'Finishing')

        try:
            self.client.close_connection()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: client disconnect: {0}".format(err))

        try:
            self.twm.stop()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: twm stop: {0}".format(err))

        try:
            self.twm.join(timeout=5)
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: tread join: {0}".format(err))

    def from_klines(self, klines):
        for kline in klines:
            candle = Candle(INSTRUMENT, kline[6], float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]),
                            float(kline[5]))
            self.process_ohlc(candle)

    def handle_socket_message(self, msg):
        if msg['e'] == 'kline':
            data = msg['k']
            if data['i'] == '1m' and data['x'] and data['s'] == INSTRUMENT.instrument():
                candle = Candle(INSTRUMENT, data['T'], float(data['o']), float(data['h']), float(data['l']),
                                float(data['c']), float(data['n']))
                self.process_ohlc(candle)

    def check_instrument(self):
        prices = self.client.get_all_tickers()
        for p in prices:
            if p['symbol'] == INSTRUMENT.instrument():
                return True
        return False
