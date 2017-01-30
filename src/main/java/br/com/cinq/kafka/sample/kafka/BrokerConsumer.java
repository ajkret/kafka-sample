package br.com.cinq.kafka.sample.kafka;

import java.util.Properties;

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

    ApplicationContext context;

    /** Concurrent threads reading messages */
    @Value("${broker.partitions:5}")
    private int partitions;

    /** Topic for subscribe, if applicable */
    @Value("${broker.topic:karma-sample}")
    private String topic;

    /** Kafka server */
    @Value("${broker.bootstrapServer:localhost\\:9092}")
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

    private Callback callback;

    /** List of consumers */
    Thread consumers[];

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

        // Configure consumers
    	logger.info("Connecting to {}", getBootstrapServer());
    	logger.info("Auto Commit set to {}", getEnableAutoCommit());

        Properties props = new Properties();
        props.put("bootstrap.servers", getBootstrapServer());
        props.put("group.id", getGroupId());
        props.put("enable.auto.commit", getEnableAutoCommit());
        if(getEnableAutoCommit())
        	props.put("auto.commit.interval.ms", getAutoCommitInterval());
        props.put("session.timeout.ms", getSessionTimeout());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        consumers = new Thread[getPartitions()];

        for (int i = 0; i < getPartitions(); i++) {
            // Initialize client
            BrokerConsumerClient client = new BrokerConsumerClient();

            client.setEnableAutoCommit(getEnableAutoCommit());
            client.setPartition(i);
            client.setTopic(getTopic());
            client.setProperties(props);
            if(callback==null) {
                client.setCallback(context.getBean(Callback.class));
                logger.debug(client.getCallback().toString());
            }
            else
                client.setCallback(callback);

            consumers[i] = new Thread(client);
            consumers[i].setName(getBootstrapServer() + ":" + i + ":" + Thread.currentThread().getId());
            consumers[i].start();
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
}
