package com.bizseer.bigdata.kafka.test;

import lombok.Getter;
import scala.Tuple2;

import java.util.*;

public class DateUtils {
    public enum TimeMillis {

        /**
         * 秒/分钟/小时/天
         */
        SEC_MILLIS(1000L),
        MINUTE_MILLIS(60 * 1000L),
        HOUR_MILLIS(60 * 60 * 1000L),
        DAY_MILLIS(60 * 60 * 40 * 1000L);
        @Getter
        private final Long millis;

        TimeMillis(Long millis) {
            this.millis = millis;
        }

    }

    /**
     * 获取最近时间粒度毫秒值
     */
    public static Long getLatelyMinutesMillis(Long timestamp, TimeMillis millisType) {
        return timestamp - (timestamp + TimeZone.getDefault().getRawOffset()) % millisType.getMillis();
    }

    /**
     * 获取最近一个自然时间粒度时间戳
     *
     * @param datumTime 基准时间,毫秒
     * @param timestamp 时间,毫秒
     * @param aggregate 聚合粒度 毫秒
     * @return 最近粒度时间戳
     */
    public static Long getNaturalAggregate(long datumTime, long timestamp, long aggregate) {
        return timestamp - ((timestamp - datumTime) % (aggregate));
    }

    /**
     * @param startTime               开始时间戳
     * @param endTime                 结束时间戳
     * @param segmentationGranularity 切分粒度(毫秒)
     * @return List<>切分后的时间段</>
     */
    public static List<Tuple2<Long, Long>> segmentationTime(Long startTime, Long endTime, Long segmentationGranularity) {

        if (endTime - startTime < segmentationGranularity) {
            return Collections.singletonList(new Tuple2<>(startTime, endTime));
        }
        long segSize = (endTime - startTime) / segmentationGranularity;
        List<Tuple2<Long, Long>> rList = new ArrayList<>();
        long tmpEndTime = endTime;
        for (long i = 1; i <= segSize; i++) {
            tmpEndTime = startTime + i * segmentationGranularity;
            rList.add(new Tuple2<>(startTime + (i - 1) * segmentationGranularity, tmpEndTime));
        }
        if (tmpEndTime < endTime) {
            rList.add(new Tuple2<>(tmpEndTime, endTime));
        }
        return rList;
    }

    /**
     * 获取某个时间的毫秒值
     */
    public static long getMillisByUnit(Integer time, String unit) {
        long reTime = time;
        switch (unit) {
            case "s":
                reTime = time * TimeMillis.SEC_MILLIS.getMillis();
                break;
            case "m":
                reTime = time * TimeMillis.MINUTE_MILLIS.getMillis();
                break;
            case "h":
                reTime = time * TimeMillis.HOUR_MILLIS.getMillis();
                break;
            case "d":
                reTime = time * TimeMillis.DAY_MILLIS.getMillis();
                break;
            default:
        }
        return reTime;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            if (i == 5) {
                continue;
            }
            System.out.println("i====" + i);
        }
    }
}
