import hashlib
import json
import logging
import threading
import time

import websocket

from candle import Instrument, Candle
from provider import BaseProvider
from util import read_request_helper

REST_API = 'https://api.aex.zone'
WS_API = 'wss://api.aex.zone/wsv3'

TIME_PERIOD = '1min'


class AexProvider(BaseProvider):

    def __init__(self, sender_lambda, id: int, name: str, instruments: list, auth_dict: dict, instrument_generator):
        super(AexProvider, self).__init__(sender_lambda, id, name, instruments, instrument_generator)

        self.auth_dict = auth_dict
        self.ping_thread = None
        # websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(WS_API,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def start(self):
        logging.log(logging.INFO, 'Started')

        self.check_instruments()

        # self.load_klines()
        self.ws.run_forever()

    def on_message(self, ws, message):
        logging.log(logging.DEBUG, message)
        if message == 'pong':
            return

        message_json = json.loads(message)
        if self.process_rt_kline(message_json):
            return

        cmd = message_json['cmd']
        code = message_json.get('code')
        if cmd == 99:
            # login is ok
            if code == 20000:
                logging.log(logging.INFO, 'Logged in Ok')
                self.subscribe_symbol_command()
            else:
                logging.log(logging.ERROR, 'Cannot logging, trying to shutdown')
                self.shutdown()

        elif cmd == 2 and message_json['action'] == 'sub' and code == 20000:
            logging.log(logging.INFO, 'Subscribed for %s', message_json['symbol'])

    def on_error(self, ws, error):
        logging.log(logging.ERROR, error)

    def on_close(self, ws, close_status_code, close_msg):
        logging.log(logging.INFO, 'Closing WS')

    def on_open(self, ws):
        logging.log(logging.INFO, 'Opening WS')
        self.login_command()

        def ping_thread(*args):
            logging.log(logging.INFO, "Ping thread is starting...")
            while True:
                try:
                    ws.send('ping')
                    logging.log(logging.DEBUG, "Ping sent")
                    time.sleep(15)
                except Exception as err:
                    logging.log(logging.ERROR, "ping thread error: {0}".format(err))
                    break
            ws.close()
            logging.log(logging.INFO, "Ping thread is terminating...")

        self.ping_thread = threading.Thread(name='ping-thread', target=ping_thread)
        self.ping_thread.start()

    def process_rt_kline(self, message_json):
        kline = message_json.get('kline')
        if kline:
            symbol = message_json['symbol']
            idx = symbol.index('@' + TIME_PERIOD)
            symbol = symbol[0:idx]

            if kline['i'] == TIME_PERIOD:
                cur_instrument = self.get_name_instrument_dict().get(symbol)
                if cur_instrument:
                    candle = Candle(
                        cur_instrument,
                        int(kline['t']),
                        float(kline['o']),
                        float(kline['h']),
                        float(kline['l']),
                        float(kline['c']),
                        float(kline['v'])
                    )
                    self.process_ohlc(candle)
            return True
        return False

    def check_instruments(self):
        instr_json = read_request_helper(REST_API + '/v3/allpair.php')

        self.check_instruments_internal(
            set([x['coin'].upper() + x['market'].upper() for x in instr_json['data']]),
            set([x.coin + x.market for x in self.get_instruments()])
        )

    def login_command(self):
        command = {'cmd': 99, 'action': 'login', 'key': self.auth_dict['api_key'], 'time': int(time.time())}
        to_hash = command['key'] + '_' + self.auth_dict['user_id'] + '_' + self.auth_dict['api_secret'] + '_' + str(
            command['time'])
        command['md5'] = hashlib.md5(to_hash.encode('utf-8')).hexdigest()

        command_str = json.dumps(command)
        self.ws.send(command_str)

    def subscribe_symbol_command(self):
        logging.log(logging.INFO, 'Subscribing to symbols...')

        for instrument in self.get_instruments():
            command_str = '{"cmd": 2, "action": "sub", "symbol": "' + \
                          self.generate_internal_name(instrument) + '@' + TIME_PERIOD + '"}'
            self.ws.send(command_str)

    # def load_klines(self):
    #     kines_json = read_request_helper(REST_API + '/v3/kLine.php' + '?mk_type=' +
    #                                      self.get_instrument().market + '&coinname=' + self.get_instrument().coin +
    #                                      '&cycle=' + TIME_PERIOD)
    #
    #     for kline in kines_json['data']:
    #         candle = Candle(self.get_instrument(), kline['t'], float(kline['o']), float(kline['h']),
    #                         float(kline['l']), float(kline['c']), float(kline['v']))
    #
    #         self.process_ohlc(candle)

    def shutdown(self):
        try:
            self.ws.close()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: ws close: %s", err)

        try:
            if self.ping_thread:
                self.ping_thread.join()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: thread join: %s", err)

        logging.log(logging.INFO, 'Shutdown competed')


def create_from_configuration(provider_name, provider_config, send_lambda):
    provider_id = provider_config['provider_id']
    symbol_first = provider_config['symbols']

    instruments = [Instrument(x['coin'], x['market'], provider_id) for x in symbol_first]

    return AexProvider(
        send_lambda,
        provider_id,
        provider_name,
        instruments,
        {'api_key': provider_config['api_key'], 'api_secret': provider_config['api_secret'],
         'user_id': provider_config['user_id']},
        lambda x, y: (x + '_' + y).lower()
    )
