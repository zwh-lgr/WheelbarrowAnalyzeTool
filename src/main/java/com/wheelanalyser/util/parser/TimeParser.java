package com.wheelanalyser.util.parser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeParser {
    /**
     * 将输入文件中的时间字段转换为LocalDateTime
     * 
     * @param textTime
     * @return
     */
    public static LocalDateTime parseToLocalDateTime(String textTime) {
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime ldt = LocalDateTime.parse(textTime, f);

        return ldt;
    }
}
