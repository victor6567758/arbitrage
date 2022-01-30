import os
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.table.window import Tumble
from pyflink.table import expressions as expr


BINANCE_PROVIDER_ID = 1
BINANCE_PROVIDER_NAME = 'BINANCE_PROVIDER'

AEX_PROVIDER_ID = 2
AEX_PROVIDER_NAME = 'AEX_PROVIDER'

providers = {BINANCE_PROVIDER_ID: BINANCE_PROVIDER_NAME, AEX_PROVIDER_ID: AEX_PROVIDER_NAME}


@udf(input_types=[DataTypes.INT()], result_type=DataTypes.STRING())
def provider_id_to_name(id):
    return providers[id]


@udf(input_types=[DataTypes.TIMESTAMP(3)], result_type=DataTypes.BIGINT())
def timestamp_to_unix(tmstmp):
    return int(time.mktime(tmstmp.timetuple()))


def setup_udf(t_env):
    t_env.create_temporary_function('provider_id_to_name', provider_id_to_name)
    t_env.create_temporary_function('timestamp_to_unix', timestamp_to_unix)


def processing_arbitrage():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    # t_env.get_config().get_configuration().set_string("pipeline.jars",
    #                                                       "file:///opt/flink/lib/flink-sql-connector-kafka_2.12-1.14.3.jar;"
    #                                                       "file:///opt/flink/lib/flink-sql-connector-elasticsearch7_2.12-1.14.3.jar;"
    #                                                       "file:///opt/flink/lib/flink-connector-kafka_2.12-1.14.3.jar;"
    #                                                       "file:///opt/flink/lib/flink-connector-elasticsearch7_2.12-1.14.3.jar"
    #                                                  )

    setup_udf(t_env)

    create_kafka_source_ddl = """
            CREATE TABLE ohcl_msg(
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
              'connector' = 'kafka',
              'topic' = 'OHCL',
              'properties.bootstrap.servers' = 'kafka1:19092',
              'scan.startup.mode' = 'earliest-offset',
              'value.format' = 'json'
            )
            """
    t_env.execute_sql(create_kafka_source_ddl)

    create_es_sink_ddl = """
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
                    'connector' = 'elasticsearch-7',
                    'hosts' = 'http://elasticsearch:9200',
                    'index' = 'arbitrage-sink-v4',
                    'sink.flush-on-checkpoint' = 'true',
                    'document-id.key-delimiter' = '$',
                    'sink.bulk-flush.max-size' = '42mb',
                    'sink.bulk-flush.max-actions' = '32',
                    'sink.bulk-flush.interval' = '1000',
                    'sink.bulk-flush.backoff.delay' = '1000',
                    'format' = 'json'
                )
        """

    t_env.execute_sql(create_es_sink_ddl)

    tab_result = transform_function_arbitrage(t_env.from_path('ohcl_msg'))
    tab_result.execute_insert("es_sink")
    #execute_result = transform_function_arbitrage(t_env.from_path('ohcl_msg')).execute()



def transform_function_arbitrage(src_tab):
    agg_binance = src_tab.where(src_tab.p == BINANCE_PROVIDER_ID).window(
        Tumble.over("5.minutes").on("new_t").alias("w")
    ).group_by("p, s, w").select(
        """
            provider_id_to_name(p) as provider, s as symbol, 
                AVG(c) AS avr_close, w.end AS last_timestamp
        """
    ).alias("b_provider", "b_symbol", "b_avr_close", "b_last_timestamp")

    agg_aex = src_tab.where(src_tab.p == AEX_PROVIDER_ID).window(
        Tumble.over("5.minutes").on("new_t").alias("w")
    ).group_by("p, s, w").select(
        """
            provider_id_to_name(p) as provider, s as symbol, 
                AVG(c) AS avr_close, w.end AS last_timestamp
        """
    ).alias("a_provider", "a_symbol", "a_avr_close", "a_last_timestamp")

    joined_tab = agg_binance.join(agg_aex, agg_binance.b_last_timestamp == agg_aex.a_last_timestamp).where(
        agg_binance.b_symbol == agg_aex.a_symbol
    ).select(
        agg_binance.b_provider,
        agg_binance.b_symbol,
        agg_binance.b_last_timestamp,
        agg_binance.b_avr_close,

        agg_aex.a_provider,
        agg_aex.a_avr_close,
        agg_binance.b_avr_close - agg_aex.a_avr_close,
    ).alias(
        "binance_provider",
        "symbol",
        "close_time",
        "binance_avr_close",

        "aex_provider",
        "aex_avr_close",
        "binance_aex_diff_avr_close"
    )

    return joined_tab.add_columns(expr.call("timestamp_to_unix", joined_tab.close_time).alias("close_time_msec"))


if __name__ == '__main__':
    processing_arbitrage()
