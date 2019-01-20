package com.yuyuda.storm;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 词频统计功能
 */
public class WordCountStorm {

    /**
     * Spout
     */
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            String dir = "D:\\bigdata\\storm\\data\\wc";
            Collection<File> files = FileUtils.listFiles(new File(dir), new String[]{"txt"}, true);

            for (File file : files) {
                try {
                    List<String> lines = FileUtils.readLines(file, "UTF-8");
                    for (String line : lines) {
                        this.collector.emit(new Values(line));
                    }
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 对数据进行分割Bolt
     */
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑：
         *      line: 对line进行按逗号分割
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");

            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 词频汇总bolt
     */
    public static class CountBolt extends BaseRichBolt {

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        Map<String, Integer> map = new HashMap<>();
        /**
         * 获取单词，进行汇总
         * 1）获取单词
         * 2）进行汇总
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count == null) {
                count = 0;
            }
            count ++;

            map.put(word, count);
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                System.out.println(entry);
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountStorm", new Config(), builder.createTopology());
    }
}
