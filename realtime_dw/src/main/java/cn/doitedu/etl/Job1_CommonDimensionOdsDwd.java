package cn.doitedu.etl;

import cn.doitedu.udfs.GeoHashUDF;
import cn.doitedu.udfs.Map2JsonStrUDF;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 用户行为日志数据，公共维度退维
 *   本job的主要任务：
 *      1. 从kafka的 ods_events 中读取用户行为数据
 *      2. 对读取到的行为数据去关联hbase中的各个维表（用户注册信息，页面信息，地域信息）
 *      3. 将关联好的结果写入kafka的 dwd_events  和  doris的dwd层表
 */
public class Job1_CommonDimensionOdsDwd {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 关于建表语句的注释，在DDL.sql中记录了
        // 1. 建表映射  kafka中行为日志原始数据topic，没用事件时间语义字段开窗啥的也就不用创建它
        tenv.executeSql(
                "CREATE TABLE events_source (  " +
                        "    username    string,  " +
                        "    session_id    string,  " +
                        "    event_id    string,  " +
                        "    action_time    bigint,  " + // ods层的action_time，到dwd层就叫做event_time
                        "    lat    double,  " +
                        "    lng    double,  " +
                        "    release_channel    string,  " +
                        "    device_type    string,  " +
                        "    properties     map<string, string>,  " +
                        // 添加处理时间语义的字段(直接这么写就行，不用像事件时间语义字段那么麻烦)，后续用于处理时间窗口、look up join
                        "    proc_time  as proctime()  " +
                        "  ) WITH (  " +
                        "     'connector' = 'kafka',  " +
                        "     'topic' = 'ods_events',  " +
                        "     'properties.bootstrap.servers' = 'hadoop102:9092',  " +
                        "     'properties.group.id' = 'goo1',  " +
                        "     'scan.startup.mode' = 'latest-offset',  " +
                        "     'value.format'='json',  " +
                        "     'value.json.fail-on-missing-field'='false',  " +
                        "     'value.fields-include' = 'EXCEPT_KEY'  " +
                        ")"
        );
//        tenv.executeSql("select * from events_source").print();

        // 2. 建表映射  kafka中行为日志dwd 目标topic, 用于写入时，表就无需定义watermark了
        tenv.executeSql(
                "  CREATE TABLE dwd_kafka(                                "
                        +"     user_id           BIGINT,                     "
                        +"     username          string,                     "
                        +"     session_id        string,                     "
                        +"     event_id          string,                     "
                        +"     event_time        bigint,                     "
                        +"     lat               double,                     "
                        +"     lng               double,                     "
                        +"     release_channel   string,                     "
                        +"     device_type       string,                     "
                        +"     properties        map<string,string>,         "
                        +"     register_phone    STRING,                     "
                        +"     user_status       INT,                        "
                        +"     register_time     TIMESTAMP(3),               "
                        +"     register_gender   INT,                        "
                        +"     register_birthday DATE,                       "
                        +"     register_province STRING,                     "
                        +"     register_city        STRING,                  "
                        +"     register_job         STRING,                  "
                        +"     register_source_type INT,                     "
                        +"     gps_province STRING,                          "
                        +"     gps_city     STRING,                          "
                        +"     gps_region   STRING,                          "
                        +"     page_type    STRING,                          "
                        +"     page_service STRING                           "
                        +" ) WITH (                                          "
                        +"  'connector' = 'kafka',                           "
                        +"  'topic' = 'dwd_events',                         "
                        +"  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        +"  'properties.group.id' = 'testGroup',             "
                        +"  'scan.startup.mode' = 'earliest-offset',         "
                        +"  'value.format'='json',                           "
                        +"  'value.json.fail-on-missing-field'='false',      "
                        +"  'value.fields-include' = 'EXCEPT_KEY')           "
        );


        // 3. 建表映射  doris中行为日志dwd目标表
        tenv.executeSql(
                " CREATE TABLE dwd_doris  (         "
                        + "     gps_province         VARCHAR(100),   "
                        + "     gps_city             VARCHAR(100),   "
                        + "     gps_region           VARCHAR(100),   "
                        + "     dt                   DATE,          "
                        + "     user_id              BIGINT,           "
                        + "     username             VARCHAR(100),   "
                        + "     session_id           VARCHAR(100),   "
                        + "     event_id             VARCHAR(100),   "
                        + "     event_time           bigint,        "
                        + "     lat                  DOUBLE,        "
                        + "     lng                  DOUBLE,        "
                        + "     release_channel      VARCHAR(100),   "
                        + "     device_type          VARCHAR(100),   "
                        + "     properties           VARCHAR(100),   "  // doris中不支持Map类型
                        + "     register_phone       VARCHAR(100),   "
                        + "     user_status          INT,           "
                        + "     register_time        TIMESTAMP(3),  "
                        + "     register_gender      INT,           "
                        + "     register_birthday    DATE,          "
                        + "     register_province    VARCHAR(100),   "
                        + "     register_city        VARCHAR(100),   "
                        + "     register_job         VARCHAR(100),   "
                        + "     register_source_type INT        ,   "
                        + "     page_type            VARCHAR(100),   "
                        + "     page_service         VARCHAR(100)    "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'hadoop102:8030',         "
                        + "    'table.identifier' = 'dwd.user_events_detail',  "
                        + "    'username' = 'root',                "
                        + "    'password' = '',                "
                        + "    'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'"
                        + " ) "  // 'sink.label-prefix'：防止测试时反复重启导致事务label已存在，因为doris的每批写入都会计做一次事务，需要有label标识
        );

        // 4. 建表映射 hbase中的维表：注册信息表
        // flink table中 第一个字段固定映射为rowkey (hbase中的rowkey没有名字的，只是纯字面值)
        // 类似于 orm api，定义table的schema 和 数据row_dict之间的映射，hbase中的一个KV 列族、列名都可以映射上，就差rowkey了
        tenv.executeSql(
                " create table user_hbase(                        "+
                        "    username STRING,                        "+
                        "    f ROW<                                 "+
                        "       id BIGINT,                           "+
                        " 	    phone STRING,                        "+
                        " 	    status INT,                          "+
                        " 	    create_time TIMESTAMP(3),            "+
                        "       gender INT,                          "+
                        " 	    birthday DATE,                       "+
                        " 	    province STRING,                     "+
                        " 	    city STRING,                         "+
                        " 	    job STRING,                          "+
                        " 	    source_type INT>                     "+
                        " ) WITH(                                    "+
                        "     'connector' = 'hbase-2.2',             "+
                        "     'table-name' = 'dim_user_info',        "+
                        "     'zookeeper.quorum' = 'hadoop102:2181'  "+
                        " )                                          "
        );

        // 5.建表映射hbase中的维表：页面信息表
        tenv.executeSql(
                "CREATE TABLE page_hbase (\n"
                + "    url_prefix STRING,\n"
                + "    f ROW< \n"
                + "          page_service STRING,\n"
                + "          page_type STRING>\n"
                + ") WITH (\n"
                + "    'connector' = 'hbase-2.2',\n"
                + "    'lookup.async' = 'true',    \n"  // 开启lookup异步查询
                + "    'lookup.cache' = 'PARTIAL', \n"  // 开启lookup缓存
                + "    'lookup.partial-cache.max-rows' = '100000',   \n" // lookup缓存的最大行数
                + "    'lookup.partial-cache.expire-after-access' = '10 m',  \n"  // 开启lookup缓存的生命时长，且在访问后会计时重置
                + "    'lookup.partial-cache.cache-missing-key' = 'true',   \n"  // 缓存缺失维度值的key
                + "    'table-name' = 'dim_page_info',\n"
                + "    'zookeeper.quorum' = 'hadoop102:2181'\n"
                + ")");

        // 6.建表映射hbase中的维表：地理信息表
        tenv.executeSql(
                "create table geo_hbase(\n" +
                        "    geohash string,\n" +
                        "    f ROW<\n" +
                        "        p string,\n" +
                        "        c string,\n" +
                        "        r string>\n" +
                        ") with (\n" +
                        "      'connector' = 'hbase-2.2',\n"
                        + "    'lookup.async' = 'true',    \n"  // 开启lookup异步查询
                        + "    'lookup.cache' = 'PARTIAL', \n"  // 开启lookup缓存
                        + "    'lookup.partial-cache.max-rows' = '100000',   \n" // lookup缓存的最大行数
                        + "    'lookup.partial-cache.expire-after-access' = '10 m',  \n"  // 开启lookup缓存的生命时长，且在访问后会计时重置
                        + "    'lookup.partial-cache.cache-missing-key' = 'true',   \n"  // 缓存缺失维度值的key
                        + "    'table-name' = 'dim_geo_area',\n" +
                        "      'zookeeper.quorum' = 'hadoop102:2181'\n" +
                        ")\n"
        );
//        tenv.executeSql("select * from geo_hbase limit 30").print();

        // 7. 进行关联
        tenv.createTemporaryFunction("geo", GeoHashUDF.class);
        //
        tenv.executeSql(
                " CREATE TEMPORARY VIEW wide_view AS                                                                                                              "
                        // 延迟重试的hint,  flink-1.16.0增强的新特性：lookup查询支持延迟重试
                        +" SELECT /*+ LOOKUP('table'='user_hbase', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='5s','max-attempts'='3') */   "
                        +" u.f.id as user_id,                                                                                                                              "
                        +" e.username,                                                                                                                                     "
                        +" e.session_id,                                                                                                                                   "
                        +" e.event_id,                                                                                                                          "
                        +" e.action_time,                                                                                                                    "
                        +" e.lat,                                                                                                                                    "
                        +" e.lng,                                                                                                                                    "
                        +" e.release_channel,                                                                                                                              "
                        +" e.device_type,                                                                                                                                  "
                        +" e.properties,                                                                                                                                   "
                        +" u.f.phone as register_phone,                                                                                                                    "
                        +" u.f.status as user_status,                                                                                                                      "
                        +" u.f.create_time as register_time,                                                                                                               "
                        +" u.f.gender as register_gender,                                                                                                                  "
                        +" u.f.birthday as register_birthday,                                                                                                              "
                        +" u.f.province as register_province,                                                                                                              "
                        +" u.f.city as register_city,                                                                                                                      "
                        +" u.f.job as register_job,                                                                                                                        "
                        +" u.f.source_type as register_source_type,                                                                                                        "
                        +" g.f.p as gps_province ,                                                                                                                         "
                        +" g.f.c as gps_city,                                                                                                                              "
                        +" g.f.r as gps_region,                                                                                                                            "
                        +" p.f.page_type ,                                                                                                                            "
                        +" p.f.page_service                                                                                                                          "
                        +" FROM events_source AS e                                                                                                                         "
                        +" LEFT JOIN user_hbase FOR SYSTEM_TIME AS OF e.proc_time AS u ON e.username = u.username                                                         "
                        // geohash，反转关联hbase中的rowkey（因为hbase中就是反转存储的，避免热点问题）
                        +" LEFT JOIN geo_hbase FOR SYSTEM_TIME AS OF e.proc_time AS g ON REVERSE(geo(e.lat,e.lng)) = g.geohash                                             "
                        +" LEFT JOIN page_hbase FOR SYSTEM_TIME AS OF e.proc_time AS p ON regexp_extract(e.properties['url'],'(^.*/).*?') = p.url_prefix                   "
        );
        // print后，下面的kafka、doris就收不到数据了。。。奇怪!
//         tenv.executeSql("select * from wide_view").print();

        // 8.将关联成功的结果，写入kafka的目标topic映射表
        tenv.executeSql("insert into dwd_kafka select * from wide_view");

        // 9.将关联成功的结果，写入doris的目标映射表
        tenv.createTemporaryFunction("toJson", Map2JsonStrUDF.class);
        tenv.executeSql("INSERT INTO dwd_doris                                     "
                + " SELECT                                                                         "
                + "     gps_province         ,                                                     "
                + "     gps_city             ,                                                     "
                + "     gps_region           ,                                                     "
                + "     TO_DATE(DATE_FORMAT(TO_TIMESTAMP_LTZ(action_time, 3),'yyyy-MM-dd')) as dt,  "
                + "     user_id              ,                                                     "
                + "     username             ,                                                     "
                + "     session_id           ,                                                     "
                + "     event_id             ,                                                     "
                + "     action_time           ,                                                     "
                + "     lat                  ,                                                     "
                + "     lng                  ,                                                     "
                + "     release_channel      ,                                                     "
                + "     device_type          ,                                                     "
                + "     toJson(properties) as properties     ,                                     " // doris不支持map类型
                + "     register_phone       ,                                                     "
                + "     user_status          ,                                                     "
                + "     register_time        ,                                                     "
                + "     register_gender      ,                                                     "
                + "     register_birthday    ,                                                     "
                + "     register_province    ,                                                     "
                + "     register_city        ,                                                     "
                + "     register_job         ,                                                     "
                + "     register_source_type ,                                                     "
                + "     page_type            ,                                                     "
                + "     page_service                                                               "
                + " FROM   wide_view                                                               "
        );

    }

}
