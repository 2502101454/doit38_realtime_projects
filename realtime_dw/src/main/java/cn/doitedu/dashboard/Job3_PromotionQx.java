package cn.doitedu.dashboard;

import cn.doitedu.udfs.GetNull;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时看板 指标计算任务
 *   当天累计至当前的每个页面、省份的pv数、uv数、新用户数，每5分钟更新一次
 *     2023-06-04 00:00:00,2023-06-04 10:05:00, page_1, shanxi, 3259345,200203,8235
 *     2023-06-04 00:00:00,2023-06-04 10:05:00, page_1, beijing,178235, 171235,1235
 */
public class Job3_PromotionQx {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1.建表映射dwd层宽表
        tenv.executeSql(
                "CREATE TABLE dwd_kafka (  " +
                        "    user_id    bigint,  " +
                        "    event_id    string,  " +
                        "    properties    map<string, string>,  " +
                        "    event_time    bigint,  " +
                        "    register_time    timestamp(3),  " +
                        "    register_province    string,  " +
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

//        tenv.executeSql("select * from dwd_kafka").print();

        // 2.建表映射mysql中的七夕活动页面表
        tenv.executeSql(
                " CREATE TABLE dashboard_promotion_qx_1 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   promotion_page_name  string,             "
                        +"   province  string,             "
                        +"   pv_amt   BIGINT,                      "
                        +"   uv_amt   BIGINT,                     "
                        +"   new_uv_amt   BIGINT                     "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://hadoop102:3306/wz_test',     "
                        +"    'table-name' = 'dashboard_promotion_qx_1',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'hadoop'                            "
                        +" )   "
        );

        tenv.createTemporaryFunction("getNull", GetNull.class);
        // 3.sql计算
        tenv.executeSql(
                "insert into dashboard_promotion_qx_1                                                                                                             " +
                        "with tmp as (                                                                                                                   " +
                        "    select                                                                                                                      " +
                        "        user_id,                                                                                                                " +
                        "        event_id,                                                                                                               " +
                        "        case                                                                                                                    " +
                        "            when regexp(properties['url'], '/mall/promotion/qxlx.*?') then '七夕拉新'                                           " +
                        "            when regexp(properties['url'], '/mall/promotion/qxcd.*?') then '七夕促单'                                           " +
                        "            when regexp(properties['url'], '/mall/promotion/qxjh.*?') then '七夕激活'                                           " +
                        "            else ''                                                                                                             " +
                        "        end as promotion_page_name,                                                                                             " +
                        "        if (date_format(register_time, 'yyyy-MM-dd') < date_format(row_time, 'yyyy-MM-dd'), getNull(), user_id) as new_user_id,    " +
                        "        register_province,                                                                                                       " +
                        "        row_time                                                                                                       " +
                        "    from dwd_kafka                                                                                                              " +
                        "    where event_id = 'page_load'                                                                                                " +
                        "    and (                                                                                                                       " +
                        "        regexp(properties['url'], '/mall/promotion/qxlx.*?') or                                                                 " +
                        "        regexp(properties['url'], '/mall/promotion/qxcd.*?') or                                                                 " +
                        "        regexp(properties['url'], '/mall/promotion/qxjh.*?')                                                                    " +
                        "    )                                                                                                                           " +
                        ")                                                                                                                               " +
                        "select                                                                                                                          " +
                        "    window_start,                                                                                                               " +
                        "    window_end,                                                                                                                 " +
                        "    promotion_page_name,                                                                                                        " +
                        "    register_province,                                                                                                          " +
                        "    sum(1) as pv_amt,                                                                                                           " +
                        "    count(distinct user_id) as uv_amt,                                                                                          " +
                        "    count(distinct new_user_id) as new_uv_amt                                                                                   " +
                        "from TABLE(                                                                                                                     " +
                        "   CUMULATE(TABLE tmp, DESCRIPTOR(row_time), INTERVAL '5' MINUTE, INTERVAL '24' HOUR)                                     " +
                        ")                                                                                                                               " +
                        "group by window_start, window_end, promotion_page_name, register_province                                                          "
        ).print();

    }
}
