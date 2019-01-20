package com.yuyuda.storm.kafka;

import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Exception in thread "main" java.lang.NoClassDefFoundError: kafka/api/OffsetRequest
 * 少kafka_2.11包
 *
 * Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError.
 * Could not initialize class org.apache.log4j.Log4jLoggerFactory
 * 日志包冲突 需要排除
 */
public class StormKafkaTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts zkHosts = new ZkHosts("node1:2181");
        SpoutConfig config = new SpoutConfig(zkHosts, "yyd", "/yyd", UUID.randomUUID().toString());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        KafkaSpout spout = new KafkaSpout(config);
        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID, spout);

        //经过bolt处理完后的数据
        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID, new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        Map<String, Object> dbConfig = new HashMap<>();
        dbConfig.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        dbConfig.put("dataSource.url", "jdbc:mysql://localhost/storm?useSSL=false");
        dbConfig.put("dataSource.user","root");
        dbConfig.put("dataSource.password","123456");
        ConnectionProvider provider = new HikariCPConnectionProvider(dbConfig);

        String tableName = "stat";
        JdbcMapper mapper = new SimpleJdbcMapper(tableName, provider);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(provider, mapper).withTableName(tableName).withQueryTimeoutSecs(30);

        builder.setBolt(JdbcInsertBolt.class.getSimpleName(), jdbcInsertBolt).shuffleGrouping(BOLT_ID);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(), new Config(), builder.createTopology());
    }
}
