package com.yuyuda.storm.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogProcessBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            byte[] bytes = input.getBinaryByField("bytes");
            String value = new String(bytes);
            String[] splits = value.split("\t");
            String phone = splits[0];
            String[] lonLat = splits[1].split(",");
            String longitude = lonLat[0];
            String latitude = lonLat[1];
            String date = splits[2].substring(1, splits[2].length() - 1);
            long timestamp = DateUtils.getInstance().getTime(date);
            System.out.println(phone + "-" + longitude + "-" + latitude + "-" + timestamp);
            this.collector.emit(new Values(timestamp, Double.parseDouble(longitude), Double.parseDouble(latitude)));
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "longitude", "latitude"));
    }
}
