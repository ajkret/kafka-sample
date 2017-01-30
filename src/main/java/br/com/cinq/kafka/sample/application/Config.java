package br.com.cinq.kafka.sample.application;


import java.util.Properties;

import javax.ws.rs.ApplicationPath;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import br.com.cinq.kafka.resource.KafkaSampleResource;
import br.com.cinq.kafka.sample.Callback;
import br.com.cinq.kafka.sample.Consumer;
import br.com.cinq.kafka.sample.Producer;
import br.com.cinq.kafka.sample.mono.QueueProducerConsumer;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.admin.RackAwareMode.Enforced$;
import kafka.common.Topic;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * Register Jersey modules
 * @author Adriano Kretschmer
 */
@Configuration
@ApplicationPath("rest")
public class Config extends ResourceConfig {
    static Logger logger = LoggerFactory.getLogger(ResourceConfig.class);

    /** Zookeeper server, to manage topics and partitions */
    @Value("${broker.zookeeper:localhost:2181}")
    private String zookeeper;

    /** Topic for subscribe, if applicable */
    @Value("${broker.topic:karma-sample}")
    private String topic;

    /** Concurrent threads reading messages */
    @Value("${broker.partitions:5}")
    private int partitions;

    /** Replication Factor */
    @Value("${broker.replication-factor:0}")
    private int replicationFactor;

    private QueueProducerConsumer testProducerConsumer;

    public Config() {
        register(KafkaSampleResource.class);
        //		packages("br.com.cinq.greet.resource");
        //		property(ServletProperties.FILTER_FORWARD_ON_404, true);
    }

    @Bean
    @Profile("unit")
    @Qualifier("sampleProducer")
    public Producer createProducerTest() {
        return getTestProducerConsumer();
    }

    @Autowired
    Callback callback;

    @Bean
    @Profile("unit")
    @Qualifier("sampleConsumer")
    public Consumer createConsumerTest() {

        return getTestProducerConsumer();
    }

    @Bean
    @Profile("!unit")
    public ZkUtils createOrUpdateTopic() {
        logger.debug("Creating topic {} with replication {} and {} partitions", topic, getReplicationFactor(), partitions);

        // Call zookeeper client to create topics WITH partitions
        ZkClient zkClient = new ZkClient(getZookeeper(),10000,8000,ZKStringSerializer$.MODULE$);
        ZkUtils utils = new ZkUtils(zkClient, new ZkConnection(getZookeeper()), false);

        // Create
        if (!AdminUtils.topicExists(utils, getTopic())) {
            // Easiest way to create a Topic programatically
            AdminUtils.createTopic(utils, getTopic(), getPartitions(), getReplicationFactor(), new Properties(),Enforced$.MODULE$);

            // "Manual" assignment of partitions and brokers, may cause problems
//            Topic.validate(getTopic());
//            scala.collection.Seq<Object> brokerList = utils.getSortedBrokerList();
//            scala.collection.Map<Object, scala.collection.Seq<Object>> partitionReplicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, getPartitions(), 1,
//                    AdminUtils.assignReplicasToBrokers$default$4(), AdminUtils.assignReplicasToBrokers$default$5());
//            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(utils, getTopic(), partitionReplicaAssignment,
//                    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(),
//                    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$5());
            logger.debug("Topic {} created", topic);
        }

        utils.close();
        return utils;
    }

    private QueueProducerConsumer getTestProducerConsumer() {
        if (testProducerConsumer == null) {
            testProducerConsumer = new QueueProducerConsumer();
            testProducerConsumer.setPartitions(5);
            testProducerConsumer.setCallback(callback);

            testProducerConsumer.start();
        }

        return testProducerConsumer;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    /**
     * Either you use the bean initialization to redirect rest calls or use @ApplicationPath
    @Bean
    public ServletRegistrationBean jerseyServlet() {
        ServletRegistrationBean registration = new ServletRegistrationBean(new ServletContainer(), "/rest/*");
        registration.addInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, Config.class.getName());
        return registration;
    }
     */

}