package cn.doitedu.olap_agg;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job03_VideoPlayAnalysisOlapAgg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 映射表，从dwd层读取数据
        // 1.建表映射dwd层宽表)
        tenv.executeSql(
                "CREATE TABLE dwd_kafka (  " +
                        "    user_id    bigint,  " +
                        "    session_id    string,  " +
                        "    event_id    string,  " +
                        "    properties    MAP<string, string>,  " +
                        "    video_id as cast(properties['video_id'] as bigint) ,  " +
                        "    play_id as properties['play_id'],  " +
                        "    event_time    bigint,  " +
                        // 地理 代表其它维度
                        "    province    string,  " +
                        "    city    string,  " +
                        "    region    string,  " +
                        "    row_time as to_timestamp_ltz(event_time, 3),  " +
                        "    watermark for row_time as row_time " +
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
        // HBase Video信息维度表
        tenv.executeSql(
                " CREATE TABLE hbase_video_info(    "+
                        "  id bigint,                        "+
                        "  f ROW<                            "+
                        "       video_name    string,           "+
                        "       video_type    string,           "+
                        "       video_album   string,          "+
                        "       video_author  string,         "+
                        " 	    video_timelong bigint        "+
                        " 	>,                               "+
                        "   primary key(id) not enforced	 "+
                        " ) WITH (                            "+
                        "  'connector' = 'hbase-2.2',            "+
                        "  'table-name' = 'dim_video_info',       "+
                        "  'zookeeper.quorum' = 'hadoop102:2181'   "+
                        " )                                      "
        );

        // tenv.executeSql("select * from hbase_video_info").print();
        // 创建doris聚合表的映射表 (其它维度、video专属维度、聚合字段)
        tenv.executeSql(
                " create table doris_sink (                                  "+
                        "      dt                       date                       "+
                        "     ,user_id                  bigint                       "+
                        "     ,session_id               string                       "+
                        "     ,province                 string                       "+
                        "     ,city                     string                       "+
                        "     ,region                   string                       "+
                        "     ,video_id                 bigint                       "+
                        "     ,video_title              string                       "+
                        "     ,video_type               string                       "+
                        "     ,video_album              string                       "+
                        "     ,video_author             string                       "+
                        "     ,video_time_amt           bigint                       "+
                        "     ,play_id                  string                       "+
                        "     ,part_no                  bigint                       "+
                        "     ,partial_start_time       bigint                       "+
                        "     ,partial_end_time         bigint                       "+
                        " ) WITH (                                                   "+
                        "    'connector' = 'doris',                                  "+
                        "    'fenodes' = 'hadoop102:8030',                             "+
                        "    'table.identifier' = 'dws.video_play_analysis_olap',    "+
                        "    'username' = 'root',                                    "+
                        "    'password' = '',                                    "+
                        "    'sink.label-prefix' ='doris_label" + System.currentTimeMillis() + "' \n" +
                        " )                                                          "
        );


        // 2.明细预处理：过滤播放事件，对一个播放周期内的多个段进行打标记
        tenv.executeSql( "create temporary view detail_processed as          "+
        // tenv.executeSql(
                "with tmp as (                                                        "+
                "	select                                                            "+
                "	  user_id,                                                        "+
                "	  session_id,                                                     "+
                "	  event_id,                                                       "+
                "	  video_id,                                                       "+
                "	  play_id,                                                        "+
                "	  event_time,                                                     "+
                "	  row_time,                                                       "+
                "	  province,                                                       "+
                "	  city,                                                           "+
                "	  region,                                                         "+
                "	  if (event_id = 'video_resume', 1, 0) as flag                   "+
                "	from dwd_kafka                                                    "+
                "	where event_id in ('video_play','video_hb','video_pause',         "+
                "   'video_resume','video_stop')                                      "+
                "),                                                                    "+
                "play_fragment as ( select                                             "+
                "	*,                                                                "+
                "	sum(flag) over(PARTITION BY play_id order by row_time) as part_no "+
                "from tmp)                                                           " +

                "select                                                                                              "+
                "	*,                                                                                               "+
                "	FIRST_VALUE(event_time) over(PARTITION BY play_id, part_no ORDER by row_time) as part_start_time "+
                "from play_fragment                                                                                "
        );
        // tenv.executeSql("select * from detail_processed").print();
        // 3.窗口预聚合
        tenv.executeSql("create temporary view wind_pre_agg as " +
                " SELECT                                                       "+
                        " 	window_start,                                               "+
                        " 	window_end,                                                 "+
                        " 	user_id,                                                    "+
                        " 	session_id,                                                 "+
                        " 	video_id,                                                   "+
                        " 	play_id,                                                    "+
                        " 	part_no,                                                    "+
                        " 	part_start_time,                                            "+
                        " 	province,                                                   "+
                        " 	city,                                                       "+
                        " 	region,                                                     "+
                        " 	proctime() as pt,          "+ // 相当于聚合中的一个常量，用于look-up join Hbase
                        " 	max(event_time) as part_end_time                            "+
                        " FROM TABLE(                                                   "+
                        " TUMBLE(TABLE detail_processed, DESCRIPTOR(row_time), INTERVAL '1' MINUTE)  "+
                        " )                                                             "+
                        " GROUP BY                                                      "+
                        " 	window_start,                                               "+
                        " 	window_end,                                                 "+
                        " 	user_id,                                                    "+
                        " 	session_id,                                                 "+
                        " 	video_id,                                                   "+
                        " 	play_id,                                                    "+
                        " 	part_no,                                                    "+
                        " 	part_start_time,                                            "+
                        " 	province,                                                   "+
                        " 	city,                                                       "+
                        " 	region                                                      "
        );
        // 4.利用part_start_time转date进行doris 分区，窗口聚合，求max(action_time)做为end_time
        tenv.executeSql("insert into doris_sink " +
                "SELECT                                                 "+
                "   to_date(date_format(to_timestamp_ltz(a.part_start_time,3),'yyyy-MM-dd')) dt, "+
                "	a.user_id,                                          "+
                "	a.session_id,                                       "+
                "	a.province,                                         "+
                "	a.city,                                             "+
                "	a.region,                                           "+
                "	a.video_id,                                         "+
                "	b.video_name as video_title,                        "+
                "	b.video_type,                                       "+
                "	b.video_album,                                      "+
                "	b.video_author,                                     "+
                "	b.video_timelong as video_time_amt,                 "+
                "	a.play_id,                                          "+
                "	a.part_no,                                          "+
                "	a.part_start_time,                                  "+
                "	a.part_end_time                                     "+
                "from wind_pre_agg as a                                 "+
                "left join hbase_video_info                             "+
                "FOR SYSTEM_TIME AS OF a.pt as b on a.video_id = b.id   "
        );

    }
}
