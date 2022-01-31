import logging

from binance import Client, ThreadedWebsocketManager

from candle import Candle, Instrument
from provider import BaseProvider


class BinanceProvider(BaseProvider):
    def __init__(self, sender_lambda, id, name, instrument, auth_dict, instrument_generator):
        super(BinanceProvider, self).__init__(sender_lambda, id, name, instrument, instrument_generator)
        self.auth_dict = auth_dict
        self.client = Client(self.auth_dict['api_key'], self.auth_dict['api_secret'])
        self.twm = ThreadedWebsocketManager()

    def start(self):
        logging.log(logging.INFO, 'Started')

        if not self.check_instrument():
            raise Exception('Cannot find symbol {}'.format(self.get_instrument()))

        # klines = self.client.get_historical_klines(self.get_instrument().instrument(), Client.KLINE_INTERVAL_1MINUTE,
        #                                   "1 day ago UTC")
        # self.from_klines(klines)

        self.twm.start()
        self.twm.start_kline_socket(callback=self.handle_socket_message,
                                    symbol=self.generate_internal_name(self.get_instrument()))

    def shutdown(self):
        logging.log(logging.INFO, 'Finishing')

        try:
            self.client.close_connection()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: client disconnect: %s", err)

        try:
            self.twm.stop()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: twm stop: %s", err)

        try:
            self.twm.join(timeout=5)
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: tread join: %s", err)

    def from_klines(self, klines):
        for kline in klines:
            candle = Candle(self.get_instrument(), kline[6] / 1000.0, float(kline[1]), float(kline[2]), float(kline[3]),
                            float(kline[4]),
                            float(kline[5]))
            self.process_ohlc(candle)

    def handle_socket_message(self, msg):
        if msg['e'] == 'kline':
            data = msg['k']
            if data['i'] == '1m' and data['x'] and data['s'] == self.generate_internal_name(self.get_instrument()):
                candle = Candle(self.get_instrument(), data['T'] / 1000.0, float(data['o']), float(data['h']),
                                float(data['l']),
                                float(data['c']), float(data['n']))
                self.process_ohlc(candle)

    def check_instrument(self):
        prices = self.client.get_all_tickers()
        for p in prices:
            if p['symbol'] == self.generate_internal_name(self.get_instrument()):
                return True
        return False


def create_from_configuration(provider_name, provider_config, send_lambda):
    provider_id = provider_config['provider_id']
    symbol_first = provider_config['symbols'][0]

    return BinanceProvider(
        send_lambda,
        provider_id,
        provider_name,
        Instrument(symbol_first['coin'], symbol_first['market'], provider_id),
        {'api_key': provider_config['api_key'], 'api_secret': provider_config['api_secret']},
        lambda x, y: x + y
    )
