from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table.window import Tumble

BINANCE_PROVIDER_ID = 1
BINANCE_PROVIDER_NAME = 'BINANCE_PROVIDER'

AEX_PROVIDER_ID = 2
AEX_PROVIDER_NAME = 'AEX_PROVIDER'

providers = {BINANCE_PROVIDER_ID: BINANCE_PROVIDER_NAME, AEX_PROVIDER_ID: AEX_PROVIDER_NAME}


@udf(input_types=[DataTypes.INT()], result_type=DataTypes.STRING())
def provider_id_to_name(id):
    return providers[id]


def processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

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
                WATERMARK FOR new_t AS new_t - INTERVAL '5' MINUTES
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'OHCL',
              'connector.properties.bootstrap.servers' = 'kafka1:19092',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink (
                provider VARCHAR PRIMARY KEY,
                global_volume DOUBLE
            ) with (
                'connector.type' = 'elasticsearch',
                'connector.version' = '7',
                'connector.hosts' = 'http://elasticsearch:9200',
                'connector.index' = 'ohcl',
                'connector.document-type'='ohcl-diff',
                'update-mode' = 'upsert',
                'connector.flush-on-checkpoint' = 'true',
                'connector.key-delimiter' = '$',
                'connector.key-null-literal' = 'n/a',
                'connector.bulk-flush.max-size' = '42mb',
                'connector.bulk-flush.max-actions' = '32',
                'connector.bulk-flush.interval' = '1000',
                'connector.bulk-flush.backoff.delay' = '1000',
                'format.type' = 'json'

            )
    """

    # create_es_sink_ddl = """
    #             CREATE TABLE es_sink (
    #                 provider VARCHAR PRIMARY KEY,
    #                 global_volume DOUBLE
    #             ) with (
    #                 'connector' = 'print'
    #             )
    #     """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)
    t_env.register_function('provider_id_to_name', provider_id_to_name)

    transform_function(t_env.from_path('ohcl_msg')).execute_insert("es_sink")
    # table_result = transform_function(t_env.from_path('ohcl_msg')).execute_insert("es_sink")
    # table_result.wait()


def transform_function(src_tab):
    return src_tab.select("provider_id_to_name(p) as provider, v") \
        .group_by("provider") \
        .select("provider, sum(v) as global_volume")


def processing2():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

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
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'OHCL',
              'connector.properties.bootstrap.servers' = 'kafka1:19092',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink (
                provider VARCHAR PRIMARY KEY,
                avr_close DOUBLE,
                first_timestamp TIMESTAMP(3),
                last_timestamp TIMESTAMP(3)
            ) with (
                'connector.type' = 'elasticsearch',
                'connector.version' = '7',
                'connector.hosts' = 'http://elasticsearch:9200',
                'connector.index' = 'ohcl',
                'connector.document-type'='ohcl-diff',
                'update-mode' = 'upsert',
                'connector.flush-on-checkpoint' = 'true',
                'connector.key-delimiter' = '$',
                'connector.key-null-literal' = 'n/a',
                'connector.bulk-flush.max-size' = '42mb',
                'connector.bulk-flush.max-actions' = '32',
                'connector.bulk-flush.interval' = '1000',
                'connector.bulk-flush.backoff.delay' = '1000',
                'format.type' = 'json'

            )
    """



    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)
    t_env.register_function('provider_id_to_name', provider_id_to_name)

    t_env.from_path("ohcl_msg").window(
        Tumble.over("5.minutes").on("new_t").alias("w")
    ).group_by("p, w").select(
        "provider_id_to_name(p) as provider, AVG(c) AS avr_close, w.start as first_timestamp, w.end AS last_timestamp"
    ).execute_insert(
        "es_sink"
    )


if __name__ == '__main__':
    processing2()
