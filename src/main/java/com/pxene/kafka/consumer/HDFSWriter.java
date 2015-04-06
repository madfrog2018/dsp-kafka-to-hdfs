package com.pxene.kafka.consumer;

import com.google.common.collect.Maps;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HDFSWriter {

    private final Log logger = LogFactory.getLog(HDFSWriter.class);
    
    private ExecutorService pool;
    private List<KafkaStream<byte[], byte[]>> kafkaStreams;

    public void init(String topic, String root, String zk, String groupid, int threads) {
        try {
            pool = Executors.newFixedThreadPool(threads);
            Config config = new Config();
            ConsumerConnector consumerConnector = config.consumerConnector(zk, groupid);
            initKafka(consumerConnector, topic);
            startWriting(root, config);
        } catch (UnknownHostException e) {
            logger.error("******init error : " + e);
        }
    }
    private void initKafka(ConsumerConnector consumerConnector, String topic) {
        Map<String, Integer> topicsToThreads = Maps.newHashMap();
        topicsToThreads.put(topic, 1);
        kafkaStreams = consumerConnector.createMessageStreams(topicsToThreads).get(topic);
    }

    public void startWriting(String root, Config config) {
        logger.info("Start Writing...");
        for (KafkaStream<byte[], byte[]> stream : kafkaStreams) {
            pool.submit(new StreamConsumer(root, stream, config));
        }
    }

}
