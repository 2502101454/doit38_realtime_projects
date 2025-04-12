package cn.doitedu.udfs;

import ch.hsr.geohash.GeoHash;
import org.apache.flink.table.functions.ScalarFunction;

public class GeoHashUDF extends ScalarFunction {
    /**
     * eval 方法就是UDF的模板方法
     * 需求：接收一个gps坐标，返回它的geoHash码
     * 细节，注意参数为null的情况，Double可以接收null，但double不能
     */
    public String eval(Double lat, Double lon) {
        String geoHash = null;
        try {
            geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 5);
        }catch (Exception e) {
        }
        return geoHash;
    }
}
