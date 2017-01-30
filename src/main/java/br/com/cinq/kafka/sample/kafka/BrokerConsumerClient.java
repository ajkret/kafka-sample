package br.com.cinq.kafka.sample.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import br.com.cinq.kafka.sample.Callback;

public class BrokerConsumerClient implements Runnable {
    Logger logger = LoggerFactory.getLogger(BrokerConsumerClient.class);

    private KafkaConsumer<String, String> consumer;

    private boolean enableAutoCommit = false;

    private Callback callback;

    private Properties properties;

    private String topic;

    private int partition;

    @Override
    public void run() {

        UUID uuid = UUID.randomUUID();
        MDC.put(getConsumer().toString(), uuid.toString());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);
                if (records != null) {
                    for (ConsumerRecord<String, String> record : records) {
                        logger.debug("tid {}, offset = {}, key = {}, value = {}", Thread.currentThread().getName(), record.offset(), record.key(),
                                record.value());
                        callback.receive(record.value());
                    }
                }
                if (!isEnableAutoCommit()) {
                    try {
                        getConsumer().commitSync();
                    } catch (CommitFailedException e) {
                        logger.warn("Commit failed!!! {}", e.getMessage(), e);
                    }
                }
            }
        } catch (TimeoutException e) {
            logger.warn("TimeoutException", e);
        } catch (WakeupException e) {
            logger.warn("Wake up exception", e);
        } finally {
            MDC.remove(consumer.toString());
        }
    }

    /**
     * Return the current consumer. If the consumer is not registered with Kafka,
     * it will register, for All the partitions.
     * @return
     */
    public KafkaConsumer<String, String> getConsumer() {
        if (consumer == null) {
            consumer = new KafkaConsumer<>(properties);

            List<TopicPartition> partitions = new ArrayList<TopicPartition>();
            //            for (PartitionInfo partition : consumer.partitionsFor(getTopic()))
            //                partitions.add(new TopicPartition(getTopic(), partition.partition()));
            //            consumer.assign(partitions);

            // consumer.subscribe(Arrays.asList(getTopic() + "-" + i));

            TopicPartition partition = new TopicPartition(getTopic(), getPartition());
            consumer.assign(Arrays.asList(partition));

        }
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

}
