package cn.doitedu.olap_agg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OverAscendingTimeTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        // 显式声明为本地运行环境，并开启webUIU (getExecutionEnvironment内部会自动判断是本地还是集群); 默认端口是随机的，我们指定
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);

        // 通过source算子，把socket数据源 加载为一个dataStream数据流
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<UserWZ> map = source.map(new MapFunction<String, UserWZ>() {
            @Override
            public UserWZ map(String line) throws Exception {
                System.out.println("接收到数据：" + line);
                String[] fields = line.split(",");
                // 解析输入 id, name, gender, age, timestamp
                return new UserWZ(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), Long.parseLong(fields[4]));
            }
        });


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.createTemporaryView("beans", map, Schema.newBuilder()
                .column("user_id", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .column("gender", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("actionTime", DataTypes.BIGINT())
                .columnByExpression("action_time", "actionTime")
                .columnByExpression("rt", "TO_TIMESTAMP_LTZ(actionTime, 0)")
                .watermark("rt", "rt - interval '10' second")
                .build());

        // Flink sql的排序只支持使用时间字段，data stream API上面都没有排序算子
        // 这里的表现也感觉就是在开窗口, 攒批排序，过时不候！
//        tenv.executeSql("select * from beans order by rt").print();

        // over() 开窗:
        // - 时钟的推进是由全局所有key共同推进的
        //- 对于丢数据而言，是站在全局时钟角度来看，只要w(t)触发窗口计算了，后面再来的<=t的数据，不看key是谁，都会丢掉；
        //- 对于允许计算的数据而言，它归入的计算窗口是其key对应的分组，这也符合SQL，partition over的语义;
        tenv.executeSql("create temporary view over_wind as " +
                        "select                                                           " +
                         "  *,                                                             " +
                         "  sum(age) over(partition by gender order by rt) as sum_age      " +
                         "from beans                                                       "
        );

        // 下游再开时间窗口
        // 1.顺序是，水位线先从source端发出，触发上游的窗口，进行输出计算结果；
        // 2.下游窗口是基于上游输出计算结果开的，属于另一个维度了，和key没关系了，水位线也从source进行传递，决定是否触发下游窗口计算；
        tenv.executeSql(" select                                        "+
                " 	window_start,                                                "+
                " 	window_end,                                                  "+
                " 	gender,                                                      "+
                " 	count(DISTINCT user_id) as uv,                               "+
                " 	sum(1) as pv,                                                "+
                " 	sum(action_time) as sum_action_time                          "+
                " from TABLE(                                                    "+
                " 	TUMBLE(TABLE over_wind, descriptor(rt), INTERVAL '5' SECOND) "+
                " )                                                              "+
                " group BY                                                       "+
                " window_start,                                                  "+
                " window_end,                                                    "+
                " gender                                                         ")
                .print();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserWZ {
        public String user_id;
        public String name;
        public String gender;
        public int age;
        public long actionTime;
    }
}
