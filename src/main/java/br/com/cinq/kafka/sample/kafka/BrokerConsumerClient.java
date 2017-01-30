package br.com.cinq.kafka.sample.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.cinq.kafka.sample.Callback;

public class BrokerConsumerClient implements Runnable, ConsumerRebalanceListener {
    Logger logger = LoggerFactory.getLogger(BrokerConsumerClient.class);

    private KafkaConsumer<String, String> consumer;

    private boolean enableAutoCommit = false;

    private Callback callback;

    private Properties properties;

    private String topic;

    private int partition;

    private boolean commitBeforeProcessing;

    private boolean pauseForProcessing;

    private ExecutorService workers;

    @Override
    public void run() {

        // By enabling this, you ignore whatever message that wasn't processed
        // seekPartitionsToEnd();

        try {
            while (true) {
                ConsumerRecords<String, String> records = getConsumer().poll(Integer.MAX_VALUE);
                if (records != null) {

                    // Commit right after processing messages. In case of commit failure,
                    // you just IGNORE the messages received, hoping that the other node in the cluster
                    // receives them
                    if (!isEnableAutoCommit() && isCommitBeforeProcessing()) {
                        try {
                            getConsumer().commitSync();
                        } catch (CommitFailedException e) {
                            logger.warn("Commit failed!!! {}", e.getMessage(), e);

                            seekPartitionsToEnd();
                        }
                    }

                    // Pause queues
                    Set<TopicPartition> partitions = records.partitions();
                    if (isPauseForProcessing()) {
                        logger.debug("Queue paused");
                        getConsumer().pause(partitions);
                    }

                    int count = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        logger.debug("tid {}, offset = {}, key = {}, value = {}", Thread.currentThread()
                                                                                        .getName(), record.offset(), record.key(),
                            record.value());
                        count++;

                        // Process
                        Future<Boolean> future = workers.submit(() -> {
                            callback.receive(record.value());
                            return true;
                        });

                        // Pause for processing enables to call polling for heartbeats and heavy processing
                        if (isPauseForProcessing()) {
                            try {
                                while (true) {
                                    try {
                                        if (future.get(1, TimeUnit.SECONDS) != null) {
                                            break;
                                        }
                                    } catch (java.util.concurrent.TimeoutException e) {
                                        ConsumerRecords<String, String> pollingForHeartbeat = getConsumer().poll(0);
                                        logger.debug("Polling while paused {}", pollingForHeartbeat != null ? pollingForHeartbeat.count() : -1);
                                    }
                                }
                            } catch (InterruptedException | ExecutionException e) {
                                logger.warn("Something went wrong while polling for Hearbeats", e);
                            }
                        }

                        // Save Offsets
                        BrokerConsumer.getOffsets()
                                      .put(new TopicPartition(record.topic(), record.partition()), record.offset());
                    }

                    // Resume
                    if (isPauseForProcessing()) {
                        logger.debug("Queue resumed");
                        getConsumer().resume(partitions);
                    }

                    logger.debug("tid {} processed {} messages", Thread.currentThread()
                                                                       .getName(), count);
                }
                if (!isEnableAutoCommit() && !isCommitBeforeProcessing()) {
                    try {
                        getConsumer().commitSync();
                    } catch (CommitFailedException e) {
                        // You WILL get exceptions due to rebalance, from time to time in clustered
                        // environments.
                        // It is up to you the deal with these situations.
                        // Our Callback.receive() is transactional, but you end up in situations
                        // that you will have to implement two-phase commit to recover from this failure.
                        // After you receive an error during kafka commit either rollback database transaction
                        // OR ignore kafka and seek() the offsets like we did in the example.
                        // Problem is, other nodes in the cluster WILL receive the messages you didn't
                        // commit during rebalance, so if you choose not using two phase commit and
                        // rollback eventual database transactions, you will
                        // have to deal with duplicates.
                        // Another approach is to commitSync() right after poll(). In that case, you
                        // should update the offsets after the messages are processed. In case of commit failure,
                        // you just IGNORE the messages received, hoping that the other node in the cluster
                        // receives the messages :-o
                        logger.warn("Commit failed!!! {}", e.getMessage(), e);

                        seekPartitionsToEnd();
                    }
                }
            }
        } catch (TimeoutException e) {
            logger.warn("TimeoutException", e);
        } catch (WakeupException e) {
            // In this situation it would be better to restart the loop
            logger.warn("Wake up exception", e);
        } catch (RebalanceInProgressException e) {
            logger.warn("Rebalance In Progress", e);
        }
    }

    private void interruptThreadForPollingWhilePaused(Thread pollingWhilePaused) {
        pollingWhilePaused.interrupt();
        try {
            pollingWhilePaused.join(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
        }
    }

    private void seekPartitionsToEnd() {
        if (BrokerConsumer.getOffsets() != null) {
            List<TopicPartition> list = new ArrayList<>();
            for (TopicPartition t : BrokerConsumer.getOffsets()
                                                  .keySet())
                list.add(t);
            try {
                consumer.seekToEnd(list);
            } catch (IllegalStateException e) {
                logger.debug("{}", e.getMessage(), e);
            }
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

            //List<TopicPartition> partitions = new ArrayList<TopicPartition>();
            //            for (PartitionInfo partition : consumer.partitionsFor(getTopic()))
            //                partitions.add(new TopicPartition(getTopic(), partition.partition()));
            //            consumer.assign(partitions);

            consumer.subscribe(Arrays.asList(getTopic()));

            // This will cause the
            //            TopicPartition partition = new TopicPartition(getTopic(), getPartition());
            //            consumer.assign(Arrays.asList(partition));

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

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("Paritions revoked from this node during rebalancing:");
        for (TopicPartition partition : partitions) {
            logger.warn("Revoked from this node {}", partition.toString());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("Paritions assigned to this node during rebalancing:");
        for (TopicPartition partition : partitions) {
            logger.warn("Rebalanced to this node {}", partition.toString());
        }
    }

    public boolean isCommitBeforeProcessing() {
        return commitBeforeProcessing;
    }

    public void setCommitBeforeProcessing(boolean commitBeforeProcessing) {
        this.commitBeforeProcessing = commitBeforeProcessing;
    }

    public boolean isPauseForProcessing() {
        return pauseForProcessing;
    }

    public void setPauseForProcessing(boolean pauseForProcessing) {
        this.pauseForProcessing = pauseForProcessing;
    }

    public ExecutorService getWorkers() {
        return workers;
    }

    public void setWorkers(ExecutorService workers) {
        this.workers = workers;
    }
}
