package com.sensorsdata.analytics.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    public enum DateFormatEnum {
        YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SSSSSS("yyyy-MM-dd HH:mm:ss SSS"),
        YYYY_MM_DD_HH_MM_SS_0("yyyy-MM-dd HH:mm:ss.0"),
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYYMMDD_HH_MM_SS("yyyy/MM/dd HH:mm:ss"),
        YYYYMMDDHHMMSS("yyyyMMddHHmmss"),
        YYYY_MM_DD_HH_MM("yyyy-MM-dd HH:mm"),
        YYYY_MM_DD("yyyy-MM-dd"),
        YYYYMMDD("yyyyMMdd");

        private final DateTimeFormatter dateTimeFormatter;

        DateFormatEnum(String format) {
            this.dateTimeFormatter = DateTimeFormat.forPattern(format);
        }

        DateTimeFormatter getDateTimeFormatter() {
            return this.dateTimeFormatter;
        }
    }

    public static String format(Date date, DateFormatEnum dateFormatter) {
        DateTime dateTime = new DateTime(date);
        return dateTime.toString(dateFormatter.getDateTimeFormatter());
    }

    public static String format(Date date, DateTimeZone timeZone, DateTimeFormatter dateFormatter) {
        DateTime dateTime = new DateTime(date, timeZone);
        return dateTime.toString(dateFormatter);
    }

    public static String format(long timeStamp, DateFormatEnum dateFormatter) {
        return format(timeStamp, "Asia/Shanghai", dateFormatter.getDateTimeFormatter());
    }

    public static String format(long timeStamp, String timeZoneId, DateTimeFormatter dateFormatter) {
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneId);
        DateTime dateTime = new DateTime(timeStamp, timeZone);
        return dateTime.toString(dateFormatter);
    }

    /**
     * 格式化日期
     *
     * @param time
     * @param dateFormatter
     * @return
     */
    public static long format(String time, DateFormatEnum dateFormatter) {

        return dateFormatter.getDateTimeFormatter().parseMillis(time);
    }

    public static long format(String time) {
        for (DateFormatEnum dateFormat : DateFormatEnum.values()) {
            long result;
            try {
                result = format(time, dateFormat);
                return result;
            } catch (Exception ignored) {
                // Do nothing
            }
        }
        return 0L;
    }

    public static long format(Date date) {
        return DateFormatEnum.YYYY_MM_DD_HH_MM_SS
                .getDateTimeFormatter().parseMillis(format(date, DateFormatEnum.YYYY_MM_DD_HH_MM_SS));
    }


    /**
     * 判断时间是否有效
     *
     * @param value     用于测试的时间字符串
     * @param formatter 指定的时间格式
     * @return value 符合 formatter 格式则返回 true，反之 false
     */
    public static Boolean isValidDate(String value, DateTimeFormatter formatter) {
        try {
            formatter.parseDateTime(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    /**
     * 获取当天的开始时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的开始时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }


    /**
     * 获取Now的开始时间的字符串
     * 格式默认YYYYMMDDHHMMSS
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfNow() {
        return DateTime.now().withTimeAtStartOfDay().toString(DateFormatEnum.YYYYMMDDHHMMSS.getDateTimeFormatter());
    }

    /**
     * 获取Now的结束时间的字符串
     * 格式默认YYYYMMDDHHMMSS
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfNow() {
        return DateTime.now().withTimeAtStartOfDay()
                .plusDays(1).minusSeconds(1).toString(DateFormatEnum.YYYYMMDDHHMMSS.getDateTimeFormatter());
    }

    public static int getDayFromEpoch() {
        Calendar cal = Calendar.getInstance();
        cal.set(1970, 0, 1, 0, 0, 0);
        Calendar now = Calendar.getInstance();
        now.set(Calendar.HOUR_OF_DAY, 0);
        now.set(Calendar.MINUTE, 0);
        now.set(Calendar.SECOND, 0);
        long intervalMilli = now.getTimeInMillis() - cal.getTimeInMillis();
        return (int) (intervalMilli / (24 * 60 * 60 * 1000));
    }

    public static int getDayFromEpoch(long time1, long time2) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(time2));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        Calendar now = Calendar.getInstance();
        now.setTime(new Date(time1));
        now.set(Calendar.HOUR_OF_DAY, 0);
        now.set(Calendar.MINUTE, 0);
        now.set(Calendar.SECOND, 0);
        long intervalMilli = now.getTimeInMillis() - cal.getTimeInMillis();
        return (int) (intervalMilli / (24 * 60 * 60 * 1000));
    }

}
