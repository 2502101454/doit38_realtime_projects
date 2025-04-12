package cn.doitedu.olap_agg;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

public class Job02_PageAccessTimeLongOlapAggTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 构建kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("dwd_events")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("gxg")
                .setClientIdPrefix("doitedu-c")
                .build();


        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk-dwd");
        // 输入行为打点的字段虽然多，但这里只取部分转json
        DataStream<PageAccess> beanStream = stream.map(json -> JSON.parseObject(json, PageAccess.class));
//        beanStream.print(">>>");
        // 对页面访问流进行加工兼容
        SingleOutputStreamOperator<PageAccess> markedStream = beanStream.keyBy(PageAccess::getSession_id).process(new KeyedProcessFunction<String, PageAccess, PageAccess>() {
            /**
             * 1.Flink维护状态：取pageLoad的事件(page、eventTime、其它地域设备等维度) ，再拼上页面的page_load_time即为eventTime
             * 2.针对流中事件顺序做判断
             *  a.遇到pageLoad事件就从读取状态中的上一次pageLoad
             *   i.如果状态为空，则以当前pageload覆盖状态，进行输出
             *   ii.如果状态不为空，取出一条上一次的page访问记录 修改其eventTime为当前pageLoad的eventTime，进行输出；以当前pageload覆盖状态，进行输出；
             *  b.遇到wakeup事件 就以wakeup事件覆盖状态进行输出
             *  c.遇到其它事件, 只从状态中取page_load_time拼接后输出（状态为空时拼接为空）
             */
            ValueState<PageAccess> lastPageState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<PageAccess> desc = new ValueStateDescriptor<>("last_page", PageAccess.class);
                desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(30)).updateTtlOnReadAndWrite().build());

                lastPageState = getRuntimeContext().getState(desc);
            }

            @Override
            public void processElement(PageAccess currentPage, Context ctx, Collector<PageAccess> out) throws Exception {
                PageAccess lastPage = lastPageState.value();

                if (currentPage.getEvent_id().equals("page_load")) {
                    if (lastPage != null) {
                        // 修改状态对象的字段值后，状态那边也会同步更新
                        lastPage.setEvent_time(currentPage.event_time);
                        lastPage.setEvent_id("fake");
                        out.collect(lastPage);
                    }
                    currentPage.setPage_load_time(currentPage.event_time);
                    lastPageState.update(currentPage);
                    out.collect(currentPage);
                } else if (currentPage.getEvent_id().equals("wake_up")) {
                    currentPage.setPage_load_time(currentPage.event_time);
                    lastPageState.update(currentPage);
                    out.collect(currentPage);
                } else {
                    currentPage.setPage_load_time(lastPage == null ? null : lastPage.getPage_load_time());
                    out.collect(currentPage);
                }
            }
        }).filter(page -> page.getProperties().get("url") != null);


        tenv.createTemporaryView("marked", markedStream, Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("session_id", DataTypes.STRING())
                        .column("event_time", DataTypes.BIGINT())
                        .column("page_load_time", DataTypes.BIGINT())
                        .column("event_id", DataTypes.STRING())
                        .column("properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .columnByExpression("url", "properties['url']")
                        .column("province", DataTypes.STRING())
                        .column("city",DataTypes.STRING())
                        .column("region",DataTypes.STRING())
                        .columnByExpression("rt", "TO_TIMESTAMP_LTZ(event_time, 3)")
                        .watermark("rt", "rt")
                .build());


        // 创建doris的映射表，粒度为 一次页面访问的时间区间
        tenv.executeSql(
                " CREATE TABLE doris_sink  (                                 "+
                        "      dt                 date                                "+
                        "     ,user_id            bigint                              "+
                        "     ,session_id         string                              "+
                        "     ,province           string                              "+
                        "     ,city               string                              "+
                        "     ,region             string                              "+
                        "     ,url                string                              "+
                        "     ,page_start_time    bigint                              "+
                        "     ,page_end_time      bigint                              "+
                        " ) WITH (                                                    "+
                        "    'connector' = 'doris',                                   "+
                        "    'fenodes' = 'hadoop102:8030',                              "+
                        "    'table.identifier' = 'dws.page_access_timelong_olap',    "+
                        "    'username' = 'root',                                     "+
                        "    'password' = '',                                     "+
                        "    'sink.label-prefix' ='doris_label" + System.currentTimeMillis() + "' \n" +
                        " )                                                           "
        );

        // flink 窗口聚合，粒度为 一次页面访问的时间区间
        tenv.executeSql("insert into doris_sink " +
                "select                                                       "+
                "   to_date(date_format(to_timestamp_ltz(page_load_time,3),'yyyy-MM-dd')) dt, "+
                "	user_id,                                                  "+
                "	session_id,                                               "+
                "	province,                                                 "+
                "	city,                                                     "+
                "	region,                                                   "+
                "	url ,                                                     "+
                "	page_load_time,                                           "+
                "	max(event_time) as page_end_time                          "+
                "from TABLE(                                                  "+
                "	TUMBLE(TABLE marked, DESCRIPTOR(rt), INTERVAL '1' MINUTE) "+
                ")                                                            "+
                "GROUP BY window_start,                                       "+
                "	window_end,                                               "+
                "	user_id,                                                  "+
                "	session_id,                                               "+
                "	province,                                                 "+
                "	city,                                                     "+
                "	region,                                                   "+
                "	url,                                                      "+
                "	page_load_time                                            ");//.print();

//        tenv.executeSql("select * from marked").print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageAccess {

        private Long user_id;
        private String session_id;
        private Long event_time;
        private Long page_load_time;
        private String event_id;
        private Map<String, String> properties;
        private String province;
        private String city;
        private String region;


    }

}
