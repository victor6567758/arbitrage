from datetime import datetime
from unittest import TestCase

from pyflink.common import Row, WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table import expressions as expr
from pyflink.table.descriptors import FileSystem, Json, Schema, Rowtime, Kafka, Csv, OldCsv
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.table.window import Tumble

from msg_processing import provider_id_to_name, AEX_PROVIDER_NAME, BINANCE_PROVIDER_NAME, transform_function, \
    BINANCE_PROVIDER_ID, AEX_PROVIDER_ID
from util import convert_date_to_unix, convert_unix_to_datetime

SYMBOL = 'SYSUSDT'


@udf(input_types=[DataTypes.BIGINT()], result_type=DataTypes.TIMESTAMP(3))
def msc_to_timestamp(msec):
    return convert_unix_to_datetime(msec)


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return int(value[0])

# https://stackoverflow.com/questions/69945836/pyflink-a-group-window-expects-a-time-attribute-for-grouping-in-a-stream-enviro


def create_temp_table(t_env):

    t_env.connect(
        Csv()
    ).with_format(
        OldCsv().schema(
            DataTypes.ROW(
                [
                    DataTypes.FIELD("t", DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD("p", DataTypes.INT()),
                    DataTypes.FIELD("s", DataTypes.STRING()),
                    DataTypes.FIELD("o", DataTypes.DOUBLE()),
                    DataTypes.FIELD("h", DataTypes.DOUBLE()),
                    DataTypes.FIELD("l", DataTypes.DOUBLE()),
                    DataTypes.FIELD("c", DataTypes.DOUBLE()),
                    DataTypes.FIELD("v", DataTypes.DOUBLE()),
                ]
            )
        )
    ).with_schema(
        Schema()
            .field("t", DataTypes.TIMESTAMP(3))
            .field("p", DataTypes.INT())
            .field("s", DataTypes.STRING())
            .field("o", DataTypes.DOUBLE())
            .field("h", DataTypes.DOUBLE())
            .field("l", DataTypes.DOUBLE())
            .field("c", DataTypes.DOUBLE())
            .field("v", DataTypes.DOUBLE())
            .rowtime(
            Rowtime()
                .timestamps_from_field("event_timestamp")
                .watermarks_periodic_bounded(60000)
        )
    ).in_append_mode().create_temporary_table(
        "input_ohlc"
    )

class Test(TestCase):
    def test_processing(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        t_env.get_config().get_configuration().set_string('parallelism.default', '1')

        src_tab = t_env.from_elements([
            (1642980060, BINANCE_PROVIDER_ID, SYMBOL, 0.9351, 0.9389, 0.9346, 0.9346, 285),
            (1642980120, BINANCE_PROVIDER_ID, SYMBOL, 0.9346, 0.9351, 0.9338, 0.9341, 100),
            (1642980180, BINANCE_PROVIDER_ID, SYMBOL, 0.9341, 0.9326, 0.9284, 0.9289, 200),

            (1642980060, AEX_PROVIDER_ID, SYMBOL, 0.9233, 0.9233, 0.9149, 0.915, 10),
            (1642980120, AEX_PROVIDER_ID, SYMBOL, 0.9172, 0.9207, 0.9143, 0.9207, 20),
            (1642980180, AEX_PROVIDER_ID, SYMBOL, 0.9185, 0.9209, 0.917, 0.9207, 30)
        ], ['t', 'p', 's', 'o', 'h', 'l', 'c', 'v'])

        t_env.register_function('provider_id_to_name', provider_id_to_name)
        execute_result = transform_function(src_tab).to_pandas()

        execute_values = execute_result.to_dict('records')
        aex_row = [x for x in execute_values if x['provider'] == AEX_PROVIDER_NAME][0]
        binance_row = [x for x in execute_values if x['provider'] == BINANCE_PROVIDER_NAME][0]

        self.assertEqual(aex_row['global_volume'], 60)
        self.assertEqual(binance_row['global_volume'], 585)

    def test_aggregation(self):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        t_env.get_config().get_configuration().set_string('parallelism.default', '1')

        src_tab = t_env.from_elements(
            [
                (convert_date_to_unix(datetime(2022, 1, 20, 18, 0, 0)), BINANCE_PROVIDER_ID, SYMBOL,
                 0.9351, 0.9389, 0.9346, 0.9346, 280.0),
                (convert_date_to_unix(datetime(2022, 1, 20, 18, 0, 1)), BINANCE_PROVIDER_ID, SYMBOL,
                 0.9346, 0.9351, 0.9338, 0.9341, 100.0),
                (convert_date_to_unix(datetime(2022, 1, 20, 18, 0, 2)) , BINANCE_PROVIDER_ID, SYMBOL,
                 0.9341, 0.9326, 0.9284, 0.9289, 200.0),

                (convert_date_to_unix(datetime(2022, 1, 20, 17, 59, 59)), AEX_PROVIDER_ID, SYMBOL,
                 0.9352, 0.9370, 0.9356, 0.9365, 280.0),
                (convert_date_to_unix(datetime(2022, 1, 20, 17, 59, 59)), AEX_PROVIDER_ID, SYMBOL,
                 0.9365, 0.9380, 0.9356, 0.9372, 100.0),
                (convert_date_to_unix(datetime(2022, 1, 20, 17, 59, 59)), AEX_PROVIDER_ID, SYMBOL,
                 0.9372, 0.9380, 0.9336, 0.9365, 150.0),

                (convert_date_to_unix(datetime(2022, 1, 20, 18, 1, 59)), AEX_PROVIDER_ID, SYMBOL,
                 0.9482, 0.9510, 0.9282, 0.9300, 100.0),
                (convert_date_to_unix(datetime(2022, 1, 20, 18, 1, 59)), AEX_PROVIDER_ID, SYMBOL,
                 0.9301, 0.9500, 0.9382, 0.9400, 160.0)
            ],
            DataTypes.ROW(
                [
                    DataTypes.FIELD("t", DataTypes.BIGINT()),
                    DataTypes.FIELD("p", DataTypes.INT()),
                    DataTypes.FIELD("s", DataTypes.STRING()),
                    DataTypes.FIELD("o", DataTypes.DOUBLE()),
                    DataTypes.FIELD("h", DataTypes.DOUBLE()),
                    DataTypes.FIELD("l", DataTypes.DOUBLE()),
                    DataTypes.FIELD("c", DataTypes.DOUBLE()),
                    DataTypes.FIELD("v", DataTypes.DOUBLE())

                ]
            )
        )

        t_env.register_function('msc_to_timestamp', msc_to_timestamp)

        def map_function(r: Row) -> Row:
            return Row(convert_unix_to_datetime(r.t / 1000), r.p, r.s, r.o, r.h, r.l, r.c, r.v)

        identity = udf(map_function,
                       result_type=DataTypes.ROW(
                           [
                               DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP(3)),
                               DataTypes.FIELD("p", DataTypes.INT()),
                               DataTypes.FIELD("s", DataTypes.STRING()),
                               DataTypes.FIELD("o", DataTypes.DOUBLE()),
                               DataTypes.FIELD("h", DataTypes.DOUBLE()),
                               DataTypes.FIELD("l", DataTypes.DOUBLE()),
                               DataTypes.FIELD("c", DataTypes.DOUBLE()),
                               DataTypes.FIELD("v", DataTypes.DOUBLE())
                           ]
                       ))

        src_tab = src_tab.map(identity).alias('rowtime', 'p', 's', 'o', 'h', 'l', 'c', 'v')
        #src_tab = src_tab.add_columns(expr.call(msc_to_timestamp, src_tab.t).alias('rowtime'))


        # src_tab = src_tab.assign_timestamps_and_watermarks(
        #     WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
        #         .with_timestamp_assigner(MyTimestampAssigner()))

        # src_tab = src_tab.window(Tumble.over("5.min").on("rowtime").alias("w")) \
        #     .group_by("w") \
        #     .select(col("w").start, col("w").end, col("w").rowtime)

        # type_info=Types.ROW([
        #     Types.BIG_INT(), Types.INT(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
        #     Types.DOUBLE(), Types.DOUBLE()
        # ]), schema=Schema.new_builder() \
        #     .column_by_expression("rowtime", "CAST(f0 AS TIMESTAMP(3))") \
        #     .column("f1", DataTypes.STRING()) \
        #     .column("f2", DataTypes.STRING()) \
        #     .watermark("rowtime", "rowtime - INTERVAL '5' MINUTE") \
        #     .build())

        t = 0
