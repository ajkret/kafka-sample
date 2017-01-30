package br.com.cinq.kafka.sample.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class BrokerProducerPartitioner implements Partitioner {

    private int numberOfPartitions;
    private int nextPartition;

    public BrokerProducerPartitioner() {
        numberOfPartitions = 10;
        nextPartition = 0;
    }

    @Override
    public void configure(Map<String, ?> configs) {

        if (configs != null) {
            numberOfPartitions = Integer.valueOf(configs.get("num.partitions").toString());
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (nextPartition > numberOfPartitions)
            numberOfPartitions = 0;
        ++nextPartition;

        return nextPartition;
    }

    @Override
    public void close() {
    }
}