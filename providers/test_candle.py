import json
import time
from unittest import TestCase


from candle import Candle, Instrument


class TestCandle(TestCase):
    def test_to_string(self):
        time_val = time.time()
        candle = Candle(Instrument('SYS', 'USDT', 1, lambda x, y: x + y),
                        time.time(), 1.1, 2.3, 0.4, 2.2, 20)
        json_candle_str = json.dumps(candle.to_dict()).encode('utf-8')
        json_candle = json.loads(json_candle_str)

        self.assertAlmostEqual(json_candle['t'], time_val, 3)
        self.assertEqual(json_candle['o'], 1.1)
        self.assertEqual(json_candle['h'], 2.3)
        self.assertEqual(json_candle['l'], 0.4)
        self.assertEqual(json_candle['c'], 2.2)
        self.assertEqual(json_candle['v'], 20)
        self.assertEqual(json_candle['s'], 'SYSUSDT')
        self.assertEqual(json_candle['p'], 1)
