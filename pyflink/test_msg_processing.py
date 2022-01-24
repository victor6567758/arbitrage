from unittest import TestCase

from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from msg_processing import provider_id_to_name, AEX_PROVIDER_NAME, BINANCE_PROVIDER_NAME, transform_function


class Test(TestCase):
    def test_processing(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)

        t_env.get_config().get_configuration().set_string('parallelism.default', '1')

        src_tab = t_env.from_elements([
            (1642980060000, 2, 'SYSUSDT', 0.9351, 0.9389, 0.9346, 0.9346, 285),
            (1642980120000, 2, 'SYSUSDT', 0.9346, 0.9351, 0.9338, 0.9341, 100),
            (1642980180000, 2, 'SYSUSDT', 0.9341, 0.9326, 0.9284, 0.9289, 200),
            (1642980060000, 1, 'SYSUSDT', 0.9233, 0.9233, 0.9149, 0.915, 10),
            (1642980120000, 1, 'SYSUSDT', 0.9172, 0.9207, 0.9143, 0.9207, 20),
            (1642980180000, 1, 'SYSUSDT', 0.9185, 0.9209, 0.917, 0.9207, 30)
        ], ['t', 'p', 's', 'o', 'h', 'l', 'c', 'v'])

        t_env.register_function('provider_id_to_name', provider_id_to_name)
        execute_result = transform_function(src_tab).to_pandas()

        execute_values = execute_result.to_dict('records')
        aex_row = [x for x in execute_values if x['provider'] == AEX_PROVIDER_NAME][0]
        binance_row = [x for x in execute_values if x['provider'] == BINANCE_PROVIDER_NAME][0]

        self.assertEqual(aex_row['global_volume'], 585)
        self.assertEqual(binance_row['global_volume'], 60)
