import os
from unittest import TestCase

from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from msg_processing import setup_udf, transform_function_arbitrage, read_provider_config

SYMBOL = 'SYSUSDT'
JAR_PATH = 'file://' + os.path.abspath("./test_libs")
TEST_OUTPUT = 'file://' + os.path.abspath("./test_output")


def init_config(t_env):
    t_env.get_config().get_configuration().set_string('parallelism.default', '1')
    t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")
    read_provider_config('../config/config.yml')

def create_temp_src_table(t_env):
    create_source_ddl = """
               CREATE TABLE ohcl_msg_test (
                   t BIGINT,
                   p INTEGER,
                   s VARCHAR,
                   o DOUBLE,
                   h DOUBLE,
                   l DOUBLE,
                   c DOUBLE,
                   v DOUBLE,
                   new_t as TO_TIMESTAMP(FROM_UNIXTIME(t)),
                   WATERMARK FOR new_t AS new_t - INTERVAL '10' SECONDS
               ) WITH (
                 'connector' = 'filesystem',
                 'path' = 'resources/test_aggregation.csv',
                 'format' = 'csv'
               )
               """
    t_env.execute_sql(create_source_ddl)


def create_sink_table(t_env):
    sink_csv = """
                    CREATE TABLE es_sink (
                        binance_provider STRING,
                        symbol STRING,
                        close_time TIMESTAMP(3),
                        binance_avr_close DOUBLE,
                        aex_provider STRING,
                        aex_avr_close DOUBLE,
                        binance_aex_diff_avr_close DOUBLE,
                        close_time_msec BIGINT,
                        PRIMARY KEY(binance_provider, aex_provider, symbol, close_time) NOT ENFORCED
                    ) with (
                        'connector' = 'print'
                    )
            """
    t_env.execute_sql(sink_csv)


class Test(TestCase):

    def test_aggregation(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        init_config(t_env)

        create_temp_src_table(t_env)
        setup_udf(t_env)
        create_sink_table(t_env)

        table_result = transform_function_arbitrage(t_env.from_path('ohcl_msg_test')).execute_insert('es_sink')
        table_result.wait()
        # execute_result = transform_function_arbitrage(t_env.from_path('ohcl_msg_test')).execute()

        # table_result = execute_result
        # with table_result.collect() as results:
        #     for result in results:
        #         row = str(result)
        #         print(row)

        t = 2
