package com.yuyuda.storm;

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
import org.apache.storm.utils.Utils;

import java.util.Map;

public class LocalSumStormAckerTopology {

    /**
     * Spout需要继承BaseRichSpout类
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会被调用一次
         *
         * @param conf      配置参数
         * @param context   上下文
         * @param collector 数据发射器
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int num = 0;

        /**
         * 会产生数据，eg：从消息队列中获取数据
         * 这个方法是一个死循环， 会一直调用
         */
        @Override
        public void nextTuple() {
            ++ num;
            this.collector.emit(new Values(num), num);
            System.out.println(num);

            Utils.sleep(1000);
        }

        @Override
        public void ack(Object msgId) {
            System.out.println("Process success: " + msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println("Process fail: " + msgId);

            //将失败数据保存下来，或者重新发射
            //dao.save(input)
            //this.collector.emit(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }
    }


    /**
     * 接收数据并处理数据
     */
    public static class SumBolt extends BaseRichBolt {

        private OutputCollector collector;
        /**
         * 初始化方法，只会被执行一次
         * @param stormConf
         * @param context
         * @param collector
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        int sum = 0;

        /**
         * 死循环一个，会一直执行。 职责：获取Spout发送过来的数据，并处理数据
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            Integer number = input.getIntegerByField("number");
            sum += number;

            if (number > 0 && number < 10) {
                this.collector.ack(input);
            } else {
                this.collector.fail(input);
            }

            //工作中
            /*try {
                //TODO.....
                this.collector.ack(input);
            } catch (Exception e) {
                this.collector.fail(input);
            }*/

            System.out.println("最终求出和：" + sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormAckerTopology", new Config(), builder.createTopology());
    }
}
