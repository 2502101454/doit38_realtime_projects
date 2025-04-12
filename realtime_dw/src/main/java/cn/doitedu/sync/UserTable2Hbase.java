package cn.doitedu.sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserTable2Hbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 因为我们的程序中用到了状态，我们要做状态的容错，所以要开启checkpoint:
        // 1.做状态快照；2.保证分布式快照的一致性 以便重启后状态是和对齐的
        // 我们要做EOS = CheckpointingMode.EXACTLY_ONCE + HBase自身的幂等性
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/doit38_realtime_project/ckp/");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1.创建一个逻辑表 user_mysql，映射mysql中的表，用cdc连接器
        tenv.executeSql(
                "CREATE TABLE user_mysql (    " +
                        "      id BIGINT," +
                        "      username STRING,                        " +
                        "      phone STRING,                           " +
                        "      status int,                             " +
                        "      create_time TIMESTAMP(3),               " +
                        "      gender int,                             " +
                        "      birthday DATE,                          " +
                        "      province STRING,                        " +
                        "      city STRING,                            " +
                        "      job STRING ,                            " +
                        "      source_type INT ,                       " +
                        "     PRIMARY KEY (id) NOT ENFORCED            " +
                        "     ) WITH (                                 " +
                        "     'connector' = 'mysql-cdc',               " +
                        "     'hostname' = 'hadoop102'   ,             " +
                        "     'port' = '3306'          ,               " +
                        "     'username' = 'root'      ,               " +
                        "     'password' = 'hadoop'      ,           " +
                        "     'database-name' = 'wz_test',             " +
                        "     'table-name' = 'ums_member',              " +
                        "     'server-time-zone' = 'Etc/UTC'              " +
                        ")"
        );

        // cdc表可以只取mysql一行中的部分字段，但如果其他字段被修改，照样产生binlog；
        // cdc这边依然会收到并产生-U、+U，只是这两组的字段是没有任何变化的
        // tenv.executeSql("select * from user_mysql").print();

        // 2.创建一个逻辑表 user_hbase, 映射hbase表, 用hbase connector
        // default:dim_user_info
        tenv.executeSql(
                     "create table user_hbase(               " +
                        "  username STRING,                     " +
                        "  f ROW<                             " +
                        "     id BIGINT,                             " +
                       "      phone STRING,                           " +
                       "      status int,                             " +
                       "      create_time TIMESTAMP(3),               " +
                       "      gender int,                             " +
                       "      birthday DATE,                          " +
                       "      province STRING,                        " +
                       "      city STRING,                            " +
                       "      job STRING ,                            " +
                       "      source_type INT >                       " +
                       " ) WITH (                                     " +
                       "    'connector' = 'hbase-2.2',                " +
                       "    'table-name' = 'dim_user_info',           " +
                       "    'zookeeper.quorum' = 'hadoop102:2181'     " +
                       ")"

        ) ;

        // 3.将cdc得到的一行，变成flink hbase表的一行，最后写入物理层HBase就是多个kv了
        tenv.executeSql("insert into user_hbase select username,row(id, phone, status, create_time, gender," +
                " birthday, province, city, job, source_type) as f from user_mysql");
    }
}

