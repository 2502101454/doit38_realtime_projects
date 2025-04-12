package cn.doitedu.udfs;

import com.alibaba.fastjson.JSON;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

public class Map2JsonStrUDF extends ScalarFunction {

    public String eval(Map<String,String> properties){
        // 传入null 返回null
        return JSON.toJSONString(properties);
    }
}

