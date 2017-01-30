package br.com.cinq.kafka.sample.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Callback;
import br.com.cinq.kafka.sample.Consumer;
import kafka.utils.ZkUtils;

/**
 * Implements the loop to receive messages and call back the user operations.
 */
@Profile("!unit")
@Component
@Qualifier("sampleConsumer")
public class BrokerConsumer implements Consumer, DisposableBean, InitializingBean, ApplicationContextAware {

    static Logger logger = LoggerFactory.getLogger(BrokerConsumer.class);

    public static String TXID = "txid";

    private static Map<TopicPartition, Long> offsets;

    ApplicationContext context;

    /** Concurrent threads reading messages */
    @Value("${broker.partitions:5}")
    private int partitions;

    /** Topic for subscribe, if applicable */
    @Value("${broker.topic:karma-sample}")
    private String topic;

    /** Kafka server */
    @Value("${broker.consumer.bootstrapServer:localhost\\:9092}")
    private String bootstrapServer;

    /** Group Id */
    @Value("${broker.group-id:test}")
    private String groupId;

    /** enableAutoCommit */
    @Value("${broker.consumer.enable-auto-commit:true}")
    private boolean enableAutoCommit;

    /** auto.commit.interval.ms */
    @Value("${broker.consumer.auto-commit-interval:1000}")
    private int autoCommitInterval = 1000;

    /** session.timeout.ms */
    @Value("${broker.session-timeout}")
    private int sessionTimeout = 30000;

    /** rebalance.max.retries*/
    @Value("${broker.consumer.rebalanceMaxRetries:4}")
    private int rebalanceMaxRetries;

    /** rebalance.backoff.ms */
    @Value("${broker.consumer.rebalanceBackoff:2000}")
    private int rebalanceBackoff;

    /** Commit right after retrieving messages from Kafka. */
    @Value("${broker.consumer.commit-before-processing:false}")
    private boolean commitBeforeProcessing;

    // Change to max.poll.records
    /** max.partition.fetch.bytes */
    @Value("${broker.consumer.maxPartitionFetchBytes:10240}")
    private int maxPartitionFetchBytes;

    /**receive.buffer.bytes */
    @Value("${broker.consumer.receiveBufferBytes:32768}")
    private int receiveBufferBytes;

    @Value("${broker.consumer.start:true}")
    private boolean automaticStart = true;
    
    @Value("${broker.consumer.pause-for-processing:false}")
    private boolean pauseForProcessing;

    // Since 0.10.x
    @Value("${broker.consumer.max-poll-records:1}")
    private int maxPollRecords;
    
    @Value("${broker.consumer.heartbeat-interval:3000}")
    private int heartbeatInterval;
    
    /** Thread pool to call polling() on paused queues */ 
    ExecutorService workers = null;

    /** Callback class to perform processing */
    private Callback callback;

    /** List of consumers */
    private static List<Thread> consumers = new LinkedList<>();

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
    * Start to receive messages
    */
    public void start() {

        // This will enforce the creation of the topic with the number of partitions we want
        context.getBean(ZkUtils.class);

        if(!isAutomaticStart())
            return;


        // Configure consumers
        logger.info("Connecting to {}", getBootstrapServer());
        logger.info("Auto Commit set to {}", getEnableAutoCommit());

        Properties props = new Properties();
        props.put("bootstrap.servers", getBootstrapServer());
        props.put("group.id", getGroupId());
        props.put("enable.auto.commit", getEnableAutoCommit());
        if (getEnableAutoCommit())
            props.put("auto.commit.interval.ms", getAutoCommitInterval());
        props.put("session.timeout.ms", getSessionTimeout());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        //        props.put("rebalance.max.retries", getRebalanceMaxRetries()); Not supported on org.apache
        //        props.put("rebalance.backoff.ms", getRebalanceBackoff());
        if(getMaxPollRecords()==0)
            props.put("max.partition.fetch.bytes", getMaxPartitionFetchBytes());
        props.put("receive.buffer.bytes", getReceiveBufferBytes());
        props.put("auto.offset.reset", "latest"); // end
        props.put("max.poll.records", getMaxPollRecords());
        props.put("heartbeat.interval.ms", getHeartbeatInterval());

        workers = Executors.newFixedThreadPool(getPartitions());

        for (int i = 0; i < getPartitions(); i++) {
            // Initialise client
            BrokerConsumerClient client = new BrokerConsumerClient();

            client.setEnableAutoCommit(getEnableAutoCommit());
            client.setPartition(i);
            client.setTopic(getTopic());
            client.setCommitBeforeProcessing(getCommitBeforeProcessing());
            client.setProperties(props);
            client.setPauseForProcessing(isPauseForProcessing());
            client.setWorkers(workers);
            if (callback == null) {
                client.setCallback(context.getBean(Callback.class));
                logger.debug(client.getCallback().toString());
            } else
                client.setCallback(callback);

            Thread consumerClient = new Thread(client);
            consumerClient.setName(getBootstrapServer() + ":" + i + ":" + consumerClient.getId());
            consumerClient.start();
            consumers.add(consumerClient);
        }
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    @Override
    public void destroy() throws Exception {
        if (consumers != null) {
            for (Thread t : consumers) {
                t.interrupt();
            }
            consumers.clear();
        }
    }

    public boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public int getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(int autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.context = context;
    }

    public static Map<TopicPartition, Long> getOffsets() {
        // IRL use persistence of offsets to KafkaConsumser.seek() when booting the
        // application. Persistence could be database, distributed memory caching, etc
        if (offsets == null) {
            offsets = Collections.synchronizedMap(new HashMap<TopicPartition, Long>());
        }
        return offsets;
    }

    public static void setOffsets(Map<TopicPartition, Long> offsets) {
        BrokerConsumer.offsets = offsets;
    }

    public int getRebalanceMaxRetries() {
        return rebalanceMaxRetries;
    }

    public void setRebalanceMaxRetries(int rebalanceMaxRetries) {
        this.rebalanceMaxRetries = rebalanceMaxRetries;
    }

    public int getRebalanceBackoff() {
        return rebalanceBackoff;
    }

    public void setRebalanceBackoff(int rebalanceBackoff) {
        this.rebalanceBackoff = rebalanceBackoff;
    }

    public boolean getCommitBeforeProcessing() {
        return commitBeforeProcessing;
    }

    public void setCommitBeforeProcessing(boolean commitBeforeProcessing) {
        this.commitBeforeProcessing = commitBeforeProcessing;
    }

    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    public int getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    public void setReceiveBufferBytes(int receiveBufferBytes) {
        this.receiveBufferBytes = receiveBufferBytes;
    }

    public boolean isAutomaticStart() {
        return automaticStart;
    }

    public void setAutomaticStart(boolean automaticStart) {
        this.automaticStart = automaticStart;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public boolean isPauseForProcessing() {
        return pauseForProcessing;
    }

    public void setPauseForProcessing(boolean pauseForProcessing) {
        this.pauseForProcessing = pauseForProcessing;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
