package com.yuyuda.storm.kafka;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

/**
 * 时间处理类
 */
public class DateUtils {
    private  DateUtils(){};

    private static volatile DateUtils instance;

    public static DateUtils getInstance() {
        if (null != instance) return instance;

        synchronized (DateUtils.class) {
            if (null == instance) instance = new DateUtils();
        }

        return instance;
    }

    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public long getTime(String date) throws ParseException {
        return format.parse(date).getTime();
    }

}
