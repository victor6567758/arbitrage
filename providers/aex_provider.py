import hashlib
import json
import logging
import os
import threading
import time
import urllib.request

import websocket

from candle import Instrument, Candle
from provider import BaseProvider

REST_API = 'https://api.aex.zone'
WS_API = 'wss://api.aex.zone/wsv3'

AEX_API_KEY = os.environ.get("AEX_API_KEY")
AEX_API_SECRET = os.environ.get("AEX_API_SECRET")
AEX_USER_ID = os.environ.get("AEX_USER_ID")
AEX_PROVIDER_ID = 2

TIME_PERIOD = '1min'

INSTRUMENT = Instrument('SYS', 'USDT', AEX_PROVIDER_ID, lambda x, y: (x + '_' + y).lower())

class AexProvider(BaseProvider):

    def __init__(self):

        self.ping_thread = None
        #websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(WS_API,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def start(self):
        logging.log(logging.INFO, 'Started')

        if not self.check_instrument():
            raise Exception('Cannot find symbol {}'.format(INSTRUMENT))

        self.load_klines()
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
        if cmd == 99 and code == 20000:
            # login is ok
            logging.log(logging.INFO, 'Logged in Ok')
            self.subscribe_symbol_command()
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
            while True:
                try:
                    ws.send('ping')
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

            if kline['i'] == TIME_PERIOD and INSTRUMENT.instrument() == symbol:
                candle = Candle(INSTRUMENT, kline['t'] * 1000, float(kline['o']),
                                float(kline['h']), float(kline['l']), float(kline['c']), float(kline['v']))

                self.process_ohlc(candle)
            return True
        return False

    def check_instrument(self):
        r = urllib.request.urlopen(REST_API + '/v3/allpair.php')
        encoding = r.info().get_content_charset('utf-8')
        instr_json = json.loads(r.read().decode(encoding))

        for instr in instr_json['data']:
            if instr['coin'].upper() == INSTRUMENT.coin and instr['market'].upper() == INSTRUMENT.market:
                return True

        return False

    def login_command(self):
        command = {'cmd': 99, 'action': 'login', 'key': AEX_API_KEY, 'time': int(time.time())}
        to_hash = command['key'] + '_' + AEX_USER_ID + '_' + AEX_API_SECRET + '_' + str(command['time'])
        command['md5'] = hashlib.md5(to_hash.encode('utf-8')).hexdigest()

        command_str = json.dumps(command)
        self.ws.send(command_str)

    def subscribe_symbol_command(self):
        logging.log(logging.INFO, 'Subscribing to symbols...')
        command_str = '{"cmd": 2, "action": "sub", "symbol": "' + INSTRUMENT.instrument() + '@' + TIME_PERIOD + '"}'
        self.ws.send(command_str)

    def load_klines(self):
        r = urllib.request.urlopen(REST_API + '/v3/kLine.php' + '?mk_type=' + INSTRUMENT.market + '&coinname=' + INSTRUMENT.coin +
                                   '&cycle=' + TIME_PERIOD)
        encoding = r.info().get_content_charset('utf-8')
        kines_json = json.loads(r.read().decode(encoding))

        for kline in kines_json['data']:
            candle = Candle(INSTRUMENT, kline['t'] * 1000, float(kline['o']), float(kline['h']),
                            float(kline['l']), float(kline['c']), float(kline['v']))

            self.process_ohlc(candle)

    def shutdown(self):
        try:
            self.ws.close()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: ws close: {0}".format(err))

        try:
            if self.ping_thread:
                self.ping_thread.join()
        except Exception as err:
            logging.log(logging.ERROR, "Shutdown error: thread join: {0}".format(err))

        logging.log(logging.INFO, 'Shutdown competed')
