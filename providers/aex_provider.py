import json
import logging

from candle import Instrument, Candle
import websocket
import _thread
import time
import urllib.request
import hashlib

from provider import BaseProvider

REST_API='https://api.aex.zone'
WS_API='wss://api.aex.zone/wsv3'

API_KEY='cf44d37068aa9c0cae6501371694f470'
API_SECRET='cdf24d8241401fcd7b487460032b06d0ebbc898d0727aa0d26665685e0f606f1'
USER_ID='988521'
AEX_PROVIDER_ID=2
INSTR1='SYS'
INSTR2='USDT'

INSTRUMENT = Instrument(INSTR1 + '_'+ INSTR2, AEX_PROVIDER_ID)


class AexProvider(BaseProvider):

    def __init__(self):

        websocket.enableTrace(True)
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
            logging.log(logging.INFO, 'Subscribed for %s', message_json['symbol'] )

    def on_error(self, ws, error):
        logging.log(logging.ERROR, error)

    def on_close(self, ws, close_status_code, close_msg):
        logging.log(logging.INFO, 'Closing WS')

    def on_open(self, ws):
        logging.log(logging.INFO, 'Opening WS')
        self.login_command()

    def process_rt_kline(self, message_json):
        kline = message_json.get('kline')
        if kline:
            symbol = message_json['symbol']
            idx = symbol.index('@1min')
            symbol = symbol[0:idx]

            if kline['i'] == '1min':
                candle = Candle(Instrument(symbol, AEX_PROVIDER_ID), kline['t'] * 1000, float(kline['o']), float(kline['h']),
                                float(kline['l']), float(kline['c']), float(kline['v']))

                self.process_ohlc(candle)
            return True
        return False

    def check_instrument(self):
        r = urllib.request.urlopen(REST_API + '/v3/allpair.php')
        encoding = r.info().get_content_charset('utf-8')
        instr_json = json.loads(r.read().decode(encoding))

        for instr in instr_json['data']:
            if instr['coin'].upper() == INSTR1 and instr['market'].upper() == INSTR2:
                return True

        return False

    def login_command(self):
        command = {}
        command['cmd'] = 99
        command['action'] = 'login'
        command['key'] = API_KEY
        command['time'] = int(time.time())

        to_hash = command['key'] + '_'+ USER_ID + '_' + API_SECRET + '_'+ str(command['time'])
        command['md5'] = hashlib.md5(to_hash.encode('utf-8')).hexdigest()

        command_str = json.dumps(command)
        self.ws.send(command_str)

    def subscribe_symbol_command(self):
        logging.log(logging.INFO, 'Subscribing to symbols...')
        command_str = '{"cmd": 2, "action": "sub", "symbol": "' + INSTRUMENT.instrument.lower() + '@1min"}'
        self.ws.send(command_str)

    def load_klines(self):
        r = urllib.request.urlopen(REST_API + '/v3/kLine.php' + '?mk_type=' + INSTR2 + '&coinname=' + INSTR1 +
                                   '&cycle=' + '1min' )
        encoding = r.info().get_content_charset('utf-8')
        kines_json = json.loads(r.read().decode(encoding))

        for kline in kines_json['data']:
            candle = Candle(INSTRUMENT, kline['t'] * 1000, float(kline['o']), float(kline['h']),
                            float(kline['l']), float(kline['c']), float(kline['v']))

            self.process_ohlc(candle)

    def shutdown(self):
        self.ws.close()
