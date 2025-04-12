package cn.doitedu.dashboard;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description: 实时看板 指标计算任务 每 5分钟内，各页面类型中 访问人数（uv）最多的前10个页面的pv
 **/
public class Job4_PVUV_3 {
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
                        "    page_type    string,  " +
                        "    properties    map<string, string>,  " +
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
        // 2.建表映射mysql中的流量看板表3
        tenv.executeSql(
                " CREATE TABLE dashboard_traffic_3 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   page_type   string,                      "
                        +"   page_url   string,                      "
                        +"   uv_amt   BIGINT,                      "
                        +"   PRIMARY KEY (window_start, window_end, page_type, page_url) not ENFORCED             "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://hadoop102:3306/wz_test',     "
                        +"    'table-name' = 'dashboard_traffic_3',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'hadoop'                            "
                        +" )   "
        );

        tenv.executeSql(
                " CREATE TABLE dashboard_traffic_4 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   page_type   string,                      "
                        +"   page_url   string,                      "
                        +"   uv_amt   BIGINT,                      "
                        +"   PRIMARY KEY (window_start, window_end, page_type, page_url) not ENFORCED             "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://hadoop102:3306/wz_test',     "
                        +"    'table-name' = 'dashboard_traffic_4',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = 'hadoop'                            "
                        +" )   "
        );
        // 3.计算指标进行存储
        tenv.executeSql(
//                "insert into dashboard_traffic_3 " +
                "create temporary view topn_view as  " +
                "SELECT                                                                                                         "+
                "	window_start,                                                                                               "+
                "	window_end,                                                                                                 "+
                "	page_type,                                                                                                  "+
                "	page_url,                                                                                                   "+
                "	uv_amt                                                                                                      "+
                "FROM (                                                                                                         "+
                "	SELECT                                                                                                      "+
                "		window_start,                                                                                           "+
                "		window_end,                                                                                             "+
                "		page_type,                                                                                              "+
                "		page_url,                                                                                               "+
                "		uv_amt,                                                                                                 "+
                "		ROW_NUMBER() OVER(PARTITION BY page_type ORDER BY uv_amt DESC) as rn                                    "+
                "	FROM (                                                                                                      "+
                "		select                                                                                                  "+
                "			window_start,                                                                                       "+
                "			window_end,                                                                                         "+
                "			page_type,                                                                                          "+
                "			regexp_extract(properties['url'], '^(.*?\\.html)') as page_url,                                        "+
                "			count(DISTINCT user_id) as uv_amt                                                                   "+
                "		FROM TABLE (                                                                                            "+
                "			TUMBLE(TABLE dwd_kafka, DESCRIPTOR(row_time), INTERVAL '5' MINUTES )                                 "+
                "		)                                                                                                       "+
                "		GROUP BY window_start, window_end, page_type, regexp_extract(properties['url'], '^(.*?\\.html)')           "+
                "	) as o1                                                                                                     "+
                ") as o2                                                                                                        "+
                "WHERE rn < 10 ");

//        tenv.executeSql("select * from topn_view").print();
        tenv.executeSql("insert into dashboard_traffic_3 select * from topn_view");
        tenv.executeSql("insert into dashboard_traffic_4 select * from topn_view");

    }
}
