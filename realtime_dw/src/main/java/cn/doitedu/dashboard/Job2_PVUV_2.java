package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时看板 指标计算任务
 *   今天开始至当前时间，累计的pv数、uv数、会话数，每 5分钟更新一次
 *     1, 2023-06-04 00:00:00,2023-06-04 10:05:00, 3259345,200203,178235
 *     2, 2023-06-04 00:00:00,2023-06-04 10:10:00, 3259345,200203,178235
 *     3, 2023-06-04 00:00:00,2023-06-04 10:15:00, 3259345,200203,178235
 */
public class Job2_PVUV_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1.建表映射dwd层宽表
        tenv.executeSql(
                "CREATE TABLE dwd_kafka (  " +
                        "    user_id    string,  " +
                        "    session_id    string,  " +
                        "    event_id    string,  " +
                        "    event_time    bigint,  " +
                        "    row_time as to_timestamp_ltz(event_time, 3),  " +
                        "    watermark for row_time as row_time - interval '0' second  " +
                        "  ) WITH (  " +
                        "     'connector' = 'kafka',  " +
                        "     'topic' = 'dwd_events',  " +
                        "     'properties.bootstrap.servers' = 'hadoop102:9092',  " +
                        "     'properties.group.id' = 'goo1',  " +
                        "     'scan.startup.mode' = 'latest-offset',  " +
                        "     'value.format'='json',  " +
                        "     'value.json.fail-on-missing-field'='false',  " +
                        "     'value.fields-include' = 'EXCEPT_KEY'  " +
                        ")"
        );
        // 2.建表映射mysql中的流量看板表2
        tenv.executeSql(
                " CREATE TABLE dashboard_traffic_2 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   pv_amt   BIGINT,                      "
                        +"   uv_amt   BIGINT,                      "
                        +"   ses_amt   BIGINT                      "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://hadoop102:3306/wz_test',     "
                        +"    'table-name' = 'dashboard_traffic_2',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'hadoop'                            "
                        +" )   "
        );

        // 3.计算指标进行存储
        tenv.executeSql(
                " INSERT INTO dashboard_traffic_2           " +
                        " select                                " +
                        "   window_start,                       " +
                        "   window_end,                         " +
                        "   sum(if(event_id='page_load', 1, 0)) as pv_amt, " +
                        "   count(distinct user_id) as uv_amt, " +
                        "   count(distinct session_id)          " +
                        " from TABLE( " +
                        " CUMULATE(TABLE dwd_kafka, DESCRIPTOR(row_time), INTERVAL '5' MINUTE, INTERVAL '24' HOUR)  " +
                        " ) " +
                        " GROUP BY " +
                        "   window_start, " +
                        "   window_end "
                );

    }
}
