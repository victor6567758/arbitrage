import os
import signal
import sys
import time
import logging

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

from candle import Candle, Instrument

API_KEY='BrsWGZTLE5nW4bGuH98vl8Y9s9tHmm4Q8xCP0AK9CTVyVG5r8VHJu1kx2TJAHyXj'
API_SECRET='5qc90s9yif0gAyJqLWtfR9Kp0Gmn529pfgdjFIUiDxsnqeHjB9NTlHyhDRasV0rH'
BINANCE_PROVIDER_ID=1
INSTRUMENT = Instrument('SYSUSDT', BINANCE_PROVIDER_ID)

class BinanceProvider:
    def __init__(self):

        self.client = Client(API_KEY, API_SECRET)
        self.twm = ThreadedWebsocketManager()

    def start(self):
        logging.log(logging.INFO, 'Started')

        prices = self.client.get_all_tickers()
        if not self.check_instrument(prices):
            raise Exception('Cannot find symbol {}'.format(INSTRUMENT))

        klines = self.client.get_historical_klines(INSTRUMENT.instrument, Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC")
        self.from_klines(klines)

        self.twm.start()
        self.twm.start_kline_socket(callback=self.handle_socket_message, symbol=INSTRUMENT.instrument)

    def shutdown(self):
        logging.log(logging.INFO, 'Finishing')

        self.client.close_connection()
        self.twm.stop()
        self.twm.join(timeout=5)
        os._exit(os.EX_OK)
        #sys.exit()

    def from_klines(self, klines):
        for kline in klines:
            candle = Candle(INSTRUMENT, kline[6], float(kline[1]), float(kline[2]),float(kline[3]),float(kline[4]), float(kline[5]))
            self.process_ohlc(candle)

    def process_ohlc(self, candle):
        logging.log(logging.INFO, 'Processing candle {}'.format(candle))

    def handle_socket_message(self, msg):
        if msg['e'] == 'kline':
            data = msg['k']
            if data['i'] == '1m' and data['x']:
                candle = Candle(Instrument(data['s'], BINANCE_PROVIDER_ID), data['T'], float(data['o']), float(data['h']), float(data['l']), float(data['c']), float(data['n']))
                self.process_ohlc(candle)

    def check_instrument(self, prices):
        for p in prices:
            if p['symbol'] == INSTRUMENT.instrument:
                return True
        return False

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
binance_provider = BinanceProvider()

def signal_handler(sig, frame):
    logging.log(logging.INFO, 'Stopping...')
    binance_provider.shutdown()

def main_start():

    signal.signal(signal.SIGINT, signal_handler)
    binance_provider.start()

    logging.log(logging.INFO, 'Press Ctrl-C')
    signal.pause()

if __name__ == '__main__':
    main_start()
