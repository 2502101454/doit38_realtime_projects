-- Doris DDL
-- 如果没有dwd库，则先创建库
CREATE DATABASE dwd;

-- 然后建表
DROP TABLE IF EXISTS dwd.user_events_detail;

CREATE TABLE dwd.user_events_detail (
  -- GPS 维度
  gps_province         VARCHAR(100),
  gps_city             VARCHAR(100),
  gps_region           VARCHAR(100),

  dt                    DATE,
  user_id               BIGINT,
  username              varchar(100),
  session_id            varchar(100),
  event_id              varchar(100),
  event_time            BIGINT, -- 来自ods层的action_time
  lat                   DOUBLE,
  lng                   DOUBLE,
  release_channel       varchar(100),
  device_type           varchar(100),
  properties            VARCHAR(200),

  -- 用户信息维度
  register_phone        varchar(100),
  user_status           INT,
  register_time         DATETIME,
  register_gender       INT,
  register_birthday     DATE,
  register_province     varchar(100),
  register_city         varchar(100),
  register_job          varchar(100),
  register_source_type  INT,

  -- 页面信息维度
  page_type             varchar(100),
  page_service          varchar(100)
)
-- 我们是明细数据，允许有重复的key
DUPLICATE KEY (gps_province, gps_city, gps_region, dt, user_id, username, session_id)
-- 和前缀索引有关，该Key就和HBase的rowKey一样，基于key的查询会加速，因为走了索引，但是要注意，
-- 索引生效的前提是查询条件从头连续地包含key，不能跳过，如果只查user_id，这样是不生效的

PARTITION BY RANGE (dt)
()
DISTRIBUTED BY HASH (session_id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true", -- 这个动态不是基于数据的分区字段动态创建新分区，而是针对表，每日自动切分新分区
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",   -- 保留历史分区的最大个数
  "dynamic_partition.end" = "3",      -- 预创建未来分区的个数
  "dynamic_partition.create_history_partition" = "false",   -- 首次建表时是否需要创建历史分区
  "dynamic_partition.history_partition_num" = "8",  -- 创建历史分区的个数
  "dynamic_partition.prefix" = "p",    -- 分区名前缀
  "dynamic_partition.buckets" = "1"     -- 动态创建的分区的分桶个数可以单独指定
);


CREATE DATABASE dws;


CREATE TABLE `tfc_overview_olap_agg` (
         `dt` date NULL,
         `time_trunk_1m` varchar(40) NULL,
         `time_trunk_5m` varchar(40) NULL,
         `time_trunk_10m` varchar(40) NULL,
         `time_trunk_30m` varchar(40) NULL,
         `time_trunk_1h` varchar(40) NULL,
         `province` varchar(40) NULL,
         `city` varchar(40) NULL,
         `region` varchar(40) NULL,
         `device_type` varchar(40) NULL,
         `release_channel` varchar(40) NULL,
         `page_url` varchar(40) NULL,
         `page_type` varchar(40) NULL,
         `page_service` varchar(40) NULL,
         `is_new` int(11) NULL,
         `pv` bigint(20) SUM NULL,
         `uv` bitmap BITMAP_UNION NULL,
         `session_cnt` hll HLL_UNION NULL
) ENGINE=OLAP
    AGGREGATE KEY(`dt`, `time_trunk_1m`, `time_trunk_5m`, `time_trunk_10m`, `time_trunk_30m`, `time_trunk_1h`, `province`, `city`, `region`, `device_type`, `release_channel`, `page_url`, `page_type`, `page_service`, `is_new`)
COMMENT 'OLAP'
PARTITION BY RANGE(`dt`)
(

)
DISTRIBUTED BY HASH(`time_trunk_1m`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true", -- 这个动态不是基于数据的分区字段动态创建新分区，而是针对表，每日自动切分新分区
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",   -- 保留历史分区的最大个数
  "dynamic_partition.end" = "3",      -- 预创建未来分区的个数
  "dynamic_partition.create_history_partition" = "false",   -- 首次建表时是否需要创建历史分区
  "dynamic_partition.history_partition_num" = "8",  -- 创建历史分区的个数
  "dynamic_partition.prefix" = "p",    -- 分区名前缀
  "dynamic_partition.buckets" = "1"     -- 动态创建的分区的分桶个数可以单独指定
);

create table dws.page_access_timelong_olap(
     dt                       date
    ,user_id                  bigint
    ,session_id               varchar(200)
    ,province                 varchar(200)
    ,city                     varchar(200)
    ,region                   varchar(200)
    ,url                      varchar(200)
    ,page_start_time          bigint
    ,page_end_time            bigint  MAX

)
AGGREGATE
KEY(dt,user_id,session_id,province,city,region,url,page_start_time)
PARTITION BY RANGE(dt)
(
)
DISTRIBUTED BY HASH(session_id) BUCKETS 1
PROPERTIES
(
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",   -- 保留历史分区的最大个数
    "dynamic_partition.end" = "3",   -- 预创建未来分区的个数
    "dynamic_partition.create_history_partition" = "false",   -- 首次建表时是否需要创建历史分区
    "dynamic_partition.history_partition_num" = "8",  -- 创建历史分区的个数
    "dynamic_partition.prefix" = "p",  -- 分区名前缀
    "dynamic_partition.buckets" = "1"
);


create table dws.video_play_analysis_olap(
     dt                       date
    ,user_id                  bigint
    ,session_id               varchar(200)
    ,province                 varchar(200)
    ,city                     varchar(200)
    ,region                   varchar(200)
    ,video_id                 bigint
    ,video_title              varchar(200)
    ,video_type               varchar(200)
    ,video_album              varchar(200)
    ,video_author             varchar(200)
    ,video_time_amt           bigint
    ,play_id                  varchar(200)
    ,part_no                  bigint
    ,partial_start_time       bigint
    ,partial_end_time         bigint  MAX
)
AGGREGATE
KEY(dt, user_id,session_id,province,city,region,video_id,video_title,video_type,video_album,video_author,video_time_amt,
play_id,part_no,partial_start_time)
PARTITION BY RANGE(dt)
(
)
DISTRIBUTED BY HASH(play_id) BUCKETS 1
PROPERTIES
(
      "replication_num" = "1",
      "dynamic_partition.enable" = "true",
      "dynamic_partition.time_unit" = "DAY",
      "dynamic_partition.start" = "-90",   -- 保留历史分区的最大个数
      "dynamic_partition.end" = "3",   -- 预创建未来分区的个数
      "dynamic_partition.create_history_partition" = "false",   -- 首次建表时是否需要创建历史分区
      "dynamic_partition.history_partition_num" = "8",  -- 创建历史分区的个数
      "dynamic_partition.prefix" = "p",  -- 分区名前缀
      "dynamic_partition.buckets" = "1"
);


-- Flink table DDL
-- ods
CREATE TABLE events_source (
    -- 物理字段
    username    string,
    session_id    string,
    event_id    string,
    action_time    bigint, --ods层的action_time 到dwd层叫event_time
    lat    double,
    lng    double,
    release_channel    string,
    device_type    string,
    properties     map<string, string>,

    -- 表达式字段
    proc_time  as proctime(),  --添加处理时间语义的字段(直接这么写就行，不用像事件时间语义字段那么麻烦)，后续用于处理时间窗口、look up join
    event_time as to_timestamp_ltz(action_time, 3),

    -- 定义watermark statement，声明table的event time attribute为event_time字段(时间之根在于action_time)
    watermark for event_time as event_time - interval '0' second
  ) WITH (
     'connector' = 'kafka',
     'topic' = 'ods_events',
     'properties.bootstrap.servers' = 'hadoop102:9092',
     'properties.group.id' = 'goo1',
     'scan.startup.mode' = 'latest-offset',
     'value.format'='json',
     'value.json.fail-on-missing-field'='false',
     'value.fields-include' = 'EXCEPT_KEY' -- 只取kafka消息中 value部分字段
)

-- dwd
-- 作为写入表的时候不需要定义watermark了
create table dwd_kafka (
    user_id     bigint,
    username    string,
    session_id    string,
    event_id    string,
    event_time    bigint, -- 来自ods层的action_time
    lat    double,
    lng    double,
    release_channel    string,
    device_type    string,
    properties     map<string, string>,

    register_phone string,
    user_status int,
    register_time timestamp(3),
    register_gender int,
    register_birthday date,
    register_province string,
    register_city string,
    register_job string,
    register_source_type int,

    gps_province string,
    gps_city string,
    gps_region string,

    page_type string,
    page_service string
 )
 with (
     'connector' = 'kafka',
     'topic' = 'dwd_events',
     'properties.bootstrap.servers' = 'hadoop102:9092',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'earliest-offset',
     'value.format'='json',
     'value.json.fail-on-missing-field'='false',
     'value.fields-include' = 'EXCEPT_KEY'
 )

CREATE TABLE dwd_doris  (
       gps_province         VARCHAR(16),
       gps_city             VARCHAR(16),
       gps_region           VARCHAR(16),
       dt                   DATE,
       user_id              BIGINT,
       username             VARCHAR(20),
       session_id           VARCHAR(20),
       event_id             VARCHAR(10),
       event_time           bigint, -- 来自ods层的action_time
       lat                  DOUBLE,
       lng                  DOUBLE,
       release_channel      VARCHAR(20),
       device_type          VARCHAR(20),
       properties           VARCHAR(40),     // doris中不支持Map类型
       register_phone       VARCHAR(20),
       user_status          INT,
       register_time        TIMESTAMP(3),
       register_gender      INT,
       register_birthday    DATE,
       register_province    VARCHAR(20),
       register_city        VARCHAR(20),
       register_job         VARCHAR(20),
       register_source_type INT        ,
       page_type            VARCHAR(20),
       page_service         VARCHAR(20)
   ) WITH (
      'connector' = 'doris',
      'fenodes' = 'hadoop102:8030',
      'table.identifier' = 'dwd.user_events_detail',
      'username' = 'root',
      'password' = '',
      'sink.label-prefix' = 'doris_label  System.currentTimeMillis()  '   // 在测试时反复运行防止label已存在
   ) -- doris每次启动客户端写入数据 都算一次事务，会指定一个事务标识 ，下次重启程序写入是新事务了，标识不能重复


-- dim
-- hbase中rowkey是没有名称的，在flink table中第一个字段默认就是rowkey
create table user_hbase(
    username string,
    f ROW<
        id BIGINT,
        phone STRING,
        status INT,
        create_time TIMESTAMP(3),
        gender INT,
        birthday DATE,
        province STRING,
        city STRING,
        job STRING,
        source_type INT>
) with (
      'connector' = 'hbase-2.2',
      'table-name' = 'dim_user_info',
      'zookeeper.quorum' = 'hadoop102:2181'
)

create table page_hbase (
    url_prefix string,
    f ROW<
        page_service string,
        page_type string>
) with (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_page_info',
    'zookeeper.quorum' = 'hadoop102:2181'
)

create table geo_hbase(
    geohash string,
    f ROW<
        p string,
        c string,
        r string>
) with (
      'connector' = 'hbase-2.2',
      'table-name' = 'dim_geo_area',
      'zookeeper.quorum' = 'hadoop102:2181'
)

