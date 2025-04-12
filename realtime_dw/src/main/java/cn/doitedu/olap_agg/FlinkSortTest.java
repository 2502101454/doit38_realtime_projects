package cn.doitedu.olap_agg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSortTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        // 显式声明为本地运行环境，并开启webUIU (getExecutionEnvironment内部会自动判断是本地还是集群); 默认端口是随机的，我们指定
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);

        // 通过source算子，把socket数据源 加载为一个dataStream数据流
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<OverAscendingTimeTest.UserWZ> map = source.map(new MapFunction<String, OverAscendingTimeTest.UserWZ>() {
            @Override
            public OverAscendingTimeTest.UserWZ map(String line) throws Exception {
                System.out.println("接收到数据：" + line);
                String[] fields = line.split(",");
                // 解析输入 id, name, gender, age, timestamp
                return new OverAscendingTimeTest.UserWZ(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), Long.parseLong(fields[4]));
            }
        });

//        map.keyBy(userWZ -> userWZ.gender).orderBy(userWZ -> userWZ.age).print();

    }
}
