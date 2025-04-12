package cn.doitedu.sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job03_VideoInfo_To_Hbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // cdc映射业务库中的 cms_video 表
        tenv.executeSql(
                " create table  cms_video_mysql(   "+
                        " id bigint,                        "+
                        " video_name string,                "+
                        " video_type string,                "+
                        " video_album string,               "+
                        " video_author string,              "+
                        " video_timelong bigint,            "+
                        " primary key (id) not enforced     "+
                        " ) with (                          "+
                        "     'connector' = 'mysql-cdc',    "+
                        "     'hostname' = 'hadoop102',       "+
                        "     'port' = '3306',              "+
                        "     'username' = 'root',          "+
                        "     'password' = 'hadoop',          "+
                        "     'database-name' = 'wz_test',   "+
                        "     'table-name' = 'cms_video',   "+
                        "     'server-time-zone' = 'Etc/UTC'   "+
                        " )                                 "
        );

        // 建表，映射hbase重点 物理表： cms_video
        tenv.executeSql(
                " CREATE TABLE hbase_sink (         "+
                        "  id bigint,                        "+
                        "  f ROW<                            "+
                        "       video_name string,           "+
                        "       video_type string,           "+
                        "       video_album string,          "+
                        "       video_author string,         "+
                        " 	    video_timelong bigint            "+
                        " 	>,                                   "+
                        "   primary key(id) not enforced	 "+
                        " ) WITH (                               "+
                        "  'connector' = 'hbase-2.2',            "+
                        "  'table-name' = 'dim_video_info',       "+
                        "  'zookeeper.quorum' = 'hadoop102:2181'   "+
                        " )                                      "
        );

        tenv.executeSql("insert into hbase_sink " +
                "select id,row(video_name,video_type,video_album,video_author,video_timelong) as f " +
                "from cms_video_mysql");


    }
}
