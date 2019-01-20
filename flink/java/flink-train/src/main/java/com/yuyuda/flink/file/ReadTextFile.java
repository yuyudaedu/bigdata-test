package com.yuyuda.flink.file;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadTextFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile("D:\\bigdata\\flink\\java\\flink-train\\src\\main\\java\\com\\yuyuda\\flink\\file\\test");
        DataStream<Integer> parsed = text.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });


        parsed.writeAsText("D:\\bigdata\\flink\\java\\flink-train\\src\\main\\java\\com\\yuyuda\\flink\\file\\out");

        env.execute("test");

    }
}
