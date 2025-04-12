package cn.doitedu;

import ch.hsr.geohash.GeoHash;

/**
 * Hello world!
 *
 */
public class GeoHashDemo
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        // 北京市
        String s = GeoHash.geoHashStringWithCharacterPrecision(39.92998577808024, 116.39564503787867, 5);
        // 北京东城区
        String a = GeoHash.geoHashStringWithCharacterPrecision(39.93857401298612, 116.42188470126446, 5);
        // 北京西城区
        String b = GeoHash.geoHashStringWithCharacterPrecision(39.93428014370851, 116.37319010401802, 5);
        // 北京房山区
        String s1 = GeoHash.geoHashStringWithCharacterPrecision(39.72675262079634, 115.86283631290442, 5);
        // 廊坊大厂
        String s2 = GeoHash.geoHashStringWithCharacterPrecision(39.886547, 116.989574, 5);
        // 天津市
        String s3 = GeoHash.geoHashStringWithCharacterPrecision(39.143929903310074, 117.21081309155257, 5);
        // 乌鲁木齐
        String s4 = GeoHash.geoHashStringWithCharacterPrecision(43.84038034721766, 87.56498774111579, 5);
        System.out.println(s); // wx4g0 北京
        System.out.println(s1); // wx46h 房山
        System.out.println(s2); // wx54y 廊坊
        System.out.println(s3); // wwgqe 天津
        System.out.println(s4); // tzy32 乌鲁木齐 (从北京一直到西安。。都是w开头的，感觉大半个中国都是w开头)
        System.out.println(a + "东城:西城" + b); // wx4g1东城:西城wx4g0
    }

}
