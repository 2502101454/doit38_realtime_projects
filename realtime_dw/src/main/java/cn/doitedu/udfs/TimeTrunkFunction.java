package cn.doitedu.udfs;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeTrunkFunction extends ScalarFunction {


    public static String eval(LocalDateTime localDateTime, Integer trunk_unit) {
        long epochMilli = localDateTime.toInstant(ZoneOffset.of(String.valueOf(ZoneOffset.ofHours(8)))).toEpochMilli();

        long interval = trunk_unit * 60 * 1000;

        long trunked = (epochMilli / interval) * interval;

        return DateFormatUtils.format(trunked,"yyyy-MM-dd HH:mm:ss");
    }

}
