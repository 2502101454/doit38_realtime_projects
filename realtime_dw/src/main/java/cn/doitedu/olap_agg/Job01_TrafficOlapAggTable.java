package cn.doitedu.olap_agg;

import cn.doitedu.udfs.TimeTrunkFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job01_TrafficOlapAggTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * DWD层数据结构
         * {"user_id":14,"username":"王江","session_id":"s04","event_id":"page_load","event_time":1714789200000,
         * "lat":39.89313025958401,"lng":116.42883455189471,"release_channel":"小米应用市场","device_type":"iphon6s",
         * "properties":{"itemId":"item002","url":"/content/article/2354.html?a=3"},"register_phone":"18361581841","user_status":1,
         * "register_time":"2018-11-12 14:22:55","register_gender":0,"register_birthday":"1998-03-18",
         * "register_province":"辽宁","register_city":"松原","register_job":"村支书",
         * "register_source_type":3,"gps_province":"北京市","gps_city":"北京市","gps_region":"东城区","page_type":"文章页",
         * "page_service":"内容服务"}
         *
         * OLAP分析的维度：
         * 终端维度(设备类型、发布渠道)；
         * 地理维度(省市区)；
         * 新老用户维度(register_time、event_time 判断)；
         * 页面维度(页面类型、页面url、页面服务)
         * 时间维度(1分钟、5分钟、10分钟、半小时、1小时、1天)
         *
         * 需求隐藏维度：去重类计数维度(user_id、session_id)；
         */
        // 1.建表映射dwd层宽表)
        tenv.executeSql(
                "CREATE TABLE dwd_kafka (  " +
                        // 隐藏维度
                        "    user_id    bigint,  " +
                        "    session_id    string,  " +
                        "    event_id    string,  " +
                        // 页面维度
                        "    properties    map<string, string>,  " +
                        "    page_type   string,  " +
                        "    page_service   string,  " +
                        // 终端维度
                        "    release_channel    string,  " +
                        "    device_type    string, " +
                        // 地理维度
                        "    gps_province    string,  " +
                        "    gps_city    string,  " +
                        "    gps_region    string,  " +
                        // 新老用户维度
                        "    register_time    string,  " +
                        // 时间维度
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

        tenv.createTemporaryFunction("time_trunk", TimeTrunkFunction.class);
        // 2. flink预聚合（窗口聚合）：分析师维度 + 去重类指标维度
        tenv.executeSql(
                        "create temporary view tmp_res as  " +
                "with tmp as (                                                                                               " +
                        "	select                                                                                                   " +
                        // 终端                                                                                              " +
                        "		release_channel,                                                                                     " +
                        "		device_type,                                                                                         " +
                        // 地理                                                                                              " +
                        "		gps_province,                                                                                        " +
                        "		gps_city,                                                                                            " +
                        "		gps_region,                                                                                          " +
                        // 新老用户                                                                                          " +
                        "		if (date_format(register_time, 'yyyy-MM-dd') = date_format(row_time, 'yyyy-MM-dd'), 1, 0) as is_new, " +
                        // 页面                                                                                              " +
                        "		regexp_extract(properties['url'], '^(.*?\\.html)') as page_url,                                       " +
                        "		page_type,                                                                                           " +
                        "		page_service,                                                                                        " +
                        // 时间                                                                                              " +
                        "		row_time,                                                                                            " +
                        // 去重类                                                                                            " +
                        "		user_id,                                                                                             " +
                        "		session_id                                                                                           " +
                        "	from dwd_kafka                                                                                           " +
                        ")                                                                                                           " +
                        "                                                                                                            " +
                        "SELECT                                                                                                      " +
                        // 时间维度（从分钟到天，dt刚好是doris表的分区字段）
                        "	to_date(date_format(window_start,  'yyyy-MM-dd')) as dt,                                                 " +
                        "	time_trunk(window_start, 1) as time_trunk_1m,                                                                 " +
                        "	time_trunk(window_start, 5) as time_trunk_5m,                                                                 " +
                        "	time_trunk(window_start, 10) as time_trunk_10m,                                                               " +
                        "	time_trunk(window_start, 30) as time_trunk_30m,                                                               " +
                        "	time_trunk(window_start, 60) as time_trunk_1h,                                                                " +
                        "	                                                                                                         " +
                        // 地理
                        "	gps_province,                                                                                            " +
                        "	gps_city,                                                                                                " +
                        "	gps_region,                                                                                              " +
                        // 终端
                        "	device_type,                                                                                             " +
                        "	release_channel,                                                                                         " +
                        // 页面
                        "	page_url,                                                                                                " +
                        "	page_type,                                                                                               " +
                        "	page_service,                                                                                            " +
                        // 新老用户
                        "	is_new,                                                                                                  " +
                        "	                                                                                                         " +
                        // 指标
                        "	sum(1) as pv,                                                                                            " +
                        // 去重类'指标'，写入flink doris table 后做sink 二次映射
                        "	user_id,                                                                                                 " +
                        "	session_id                                                                                               " +
                        "	                                                                                                         " +
                        "FROM TABLE (                                                                                                " +
                        "	TUMBLE(TABLE tmp, descriptor(row_time), INTERVAL '1' MINUTE)                                             " +
                        ")                                                                                                           " +
                        "GROUP BY window_start, window_end, release_channel,device_type,gps_province,gps_city,                       " +
                        "gps_region,is_new,page_url,page_type,page_service,user_id,session_id                                        "
        );


//        tenv.executeSql("select * from tmp_res").print();
        // 3. Flink Sink Doris
        tenv.executeSql(
                "CREATE TABLE sink_doris(     \n" +
                        "    dt            date,      \n" +
                        "    time_trunk_1m string,    \n" +
                        "    time_trunk_5m string,    \n" +
                        "    time_trunk_10m string,   \n" +
                        "    time_trunk_30m string,    \n" +
                        "    time_trunk_1h string,    \n" +
                        "    province string,\n" +
                        "    city string,\n" +
                        "    region string,\n" +
                        "    device_type string,\n" +
                        "    release_channel string,\n" +
                        "    page_url string,\n" +
                        "    page_type string,\n" +
                        "    page_service string,\n" +
                        "    is_new  int ,\n" +
                        "\n" +
                        "    pv  bigint,\n" +
                        "    uv  bigint,  \n" + // 写入的是user_id
                        "    session_cnt  string   \n" + // 写入的session_id
                        ") WITH (                                         \n" +
                        "   'connector' = 'doris',                        \n" +
                        "   'fenodes' = 'hadoop102:8030',                   \n" +
                        "   'table.identifier' = 'dws.tfc_overview_olap_agg',  \n" +
                        "   'username' = 'root',                          \n" +
                        "   'password' = '',                          \n" +
                        "   'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "', \n" +
                        "   'sink.properties.columns' = 'dt,time_trunk_1m,time_trunk_5m,time_trunk_10m,time_trunk_30m,time_trunk_1h," +
                        "       province,city,region,device_type,release_channel,page_url,page_type,page_service," +
                        "       is_new,pv,uv,session_cnt,uv=to_bitmap(uv),session_cnt=hll_hash(session_cnt) ' " +
                        ")"
        );


        // sink查询结果到目标表
        tenv.executeSql("insert into sink_doris select * from tmp_res");


    }
}
