import os
from unittest import TestCase

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import FileSystem, Schema, Rowtime, Csv
from pyflink.table.udf import udf

from msg_processing import AEX_PROVIDER_NAME, BINANCE_PROVIDER_NAME, transform_function_simple, \
    BINANCE_PROVIDER_ID, AEX_PROVIDER_ID, setup_udf, transform_function_arbitrage
from util import convert_unix_to_datetime

SYMBOL = 'SYSUSDT'
JAR_PATH = 'file://' + os.path.abspath("./test_libs")

@udf(input_types=[DataTypes.BIGINT()], result_type=DataTypes.TIMESTAMP(3))
def msc_to_timestamp(msec):
    return convert_unix_to_datetime(msec)


def test_config(t_env):
    t_env.get_config().get_configuration().set_string('parallelism.default', '1')
    t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")
    class_paths = "{0}/flink-json-1.13.1.jar;{0}/flink-csv-1.13.1-sql-jar.jar;" \
                  "{0}/flink-csv-1.13.1.jar;" \
                  "{0}/flink-connector-filesystem_2.12-1.11.6.jar;"\
                  "{0}/flink-connector-files-1.13.1.jar".format(JAR_PATH)


    t_env.get_config().get_configuration().set_string("pipeline.jars", class_paths)
    #t_env.get_config().get_configuration().set_string("pipeline.classpaths", class_paths)


def create_temp_src_table_2(t_env):
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


def create_temp_src_table(t_env):
    t_env.connect(FileSystem().path(
        str(os.path.join(os.path.dirname(__file__), 'resources', 'test_aggregation.csv')))
    ).with_format(
        Csv(field_delimiter=',')
    ).with_schema(
        Schema()
            .field("new_t", DataTypes.TIMESTAMP(3))
            .field("p", DataTypes.INT())
            .field("s", DataTypes.STRING())
            .field("o", DataTypes.DOUBLE())
            .field("h", DataTypes.DOUBLE())
            .field("l", DataTypes.DOUBLE())
            .field("c", DataTypes.DOUBLE())
            .field("v", DataTypes.DOUBLE())
            .rowtime(
            Rowtime()
                .timestamps_from_field("new_t")
                .watermarks_periodic_ascending()
        )
    ).in_append_mode().create_temporary_table(
        "ohcl_msg_test"
    )


class Test(TestCase):
    def test_processing(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        test_config(t_env)

        src_tab = t_env.from_elements([
            (1642980060, BINANCE_PROVIDER_ID, SYMBOL, 0.9351, 0.9389, 0.9346, 0.9346, 285),
            (1642980120, BINANCE_PROVIDER_ID, SYMBOL, 0.9346, 0.9351, 0.9338, 0.9341, 100),
            (1642980180, BINANCE_PROVIDER_ID, SYMBOL, 0.9341, 0.9326, 0.9284, 0.9289, 200),

            (1642980060, AEX_PROVIDER_ID, SYMBOL, 0.9233, 0.9233, 0.9149, 0.915, 10),
            (1642980120, AEX_PROVIDER_ID, SYMBOL, 0.9172, 0.9207, 0.9143, 0.9207, 20),
            (1642980180, AEX_PROVIDER_ID, SYMBOL, 0.9185, 0.9209, 0.917, 0.9207, 30)
        ], ['t', 'p', 's', 'o', 'h', 'l', 'c', 'v'])

        setup_udf(t_env)
        execute_result = transform_function_simple(src_tab).to_pandas()

        execute_values = execute_result.to_dict('records')
        aex_row = [x for x in execute_values if x['provider'] == AEX_PROVIDER_NAME][0]
        binance_row = [x for x in execute_values if x['provider'] == BINANCE_PROVIDER_NAME][0]

        self.assertEqual(aex_row['global_volume'], 60)
        self.assertEqual(binance_row['global_volume'], 585)

    def test_aggregation(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        test_config(t_env)

        create_temp_src_table_2(t_env)
        setup_udf(t_env)

        sink_csv = """
                CREATE TABLE es_sink_2 (
                    ltmstmp BIGINT,
                    provider_list MULTISET<STRING>,
                    symbol VARCHAR,
                    avr_close_list MULTISET<DOUBLE>,
                    text_time as TO_TIMESTAMP(FROM_UNIXTIME(ltmstmp)),
                    PRIMARY KEY(ltmstmp, symbol) NOT ENFORCED
                ) with (
                    'connector' = 'filesystem',           -- required: specify the connector
                    'path' = 'file:///tmp/output',  -- required: path to a directory
                    'format' = 'CSV'
                )
        """

        t_env.execute_sql(sink_csv)

        execute_result = transform_function_arbitrage(t_env.from_path('ohcl_msg_test')).execute_insert("es_sink_2")

        # table_result = execute_result
        # with table_result.collect() as results:
        #     for result in results:
        #         row = str(result)
        #         print(row)

        t = 0
