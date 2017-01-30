package br.com.cinq.kafka.sample.kafka;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Consumer;

/**
 * Using Duplicates makes no difference.
 * @author a.kretschmer
 *
 */
@Profile("!unit")
@Component
@Qualifier("consumerDuplicate")
public class BrokerConsumerDuplicate extends BrokerConsumer implements Consumer, DisposableBean, InitializingBean {

    /** enableAutoCommit */
    @Value("${broker.consumer.enable-duplicates:false}")
    private boolean enableDuplicates;

    /** enableAutoCommit */
    @Value("${broker.consumer.duplicates:1}")
    private int duplicates;

    // Enables another consumer,
    @Override
    public void afterPropertiesSet() throws Exception {
        if(isEnableDuplicates()) {
            for(int i=0;i<getDuplicates();i++) {
                BrokerConsumerDuplicate duplicate = new BrokerConsumerDuplicate();
                duplicate.setAutoCommitInterval(getAutoCommitInterval());
                duplicate.setBootstrapServer(getBootstrapServer());
                duplicate.setCallback(getCallback());
                duplicate.setEnableAutoCommit(getEnableAutoCommit());
                duplicate.setGroupId(getGroupId());
                duplicate.setPartitions(getPartitions());
                duplicate.setSessionTimeout(getSessionTimeout());
                duplicate.setTopic(getTopic());

                duplicate.start();
            }
        }
    }

    public boolean isEnableDuplicates() {
        return enableDuplicates;
    }

    public void setEnableDuplicates(boolean enableDuplicates) {
        this.enableDuplicates = enableDuplicates;
    }

    public int getDuplicates() {
        return duplicates;
    }

    public void setDuplicates(int duplicates) {
        this.duplicates = duplicates;
    }
}
