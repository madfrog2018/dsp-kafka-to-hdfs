package com.pxene.kafka.consumer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.hadoop.conf.Configuration;

public class Config {

    public Configuration configure() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", 
        		org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        return conf;
    }

    public ConsumerConnector consumerConnector(String zk, String groupid) throws UnknownHostException {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zk);
        properties.put("group.id", groupid);
        properties.put("zookeeper.session.timeout.ms", "2000");
        properties.put("zookeeper.sync.time.ms", "2000");
        properties.put("auto.commit.interval.ms", "5000");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }
    
}
