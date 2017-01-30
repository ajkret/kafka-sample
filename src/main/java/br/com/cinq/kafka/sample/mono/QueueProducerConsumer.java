package br.com.cinq.kafka.sample.mono;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Callback;
import br.com.cinq.kafka.sample.Consumer;
import br.com.cinq.kafka.sample.Producer;
import br.com.cinq.kafka.sample.exception.QueueException;

/**
 * Implements the loop to receive messages and call back the user operations.
 */
public class QueueProducerConsumer implements Producer, Consumer, DisposableBean {

    public static String TXID = "txid";

    private int partitions = 5;

    private Callback callback;

    /** List of consumers */
    Thread consumers[];

    private QueueWrapperForSpring queue = new QueueWrapperForSpring();

    @Override
    public void send(String message) throws QueueException {
        queue.send(message);
    }

    /**
    * Start to receive messages
    */
    public void start() {

        consumers = new Thread[getPartitions()];

        for (int i = 0; i < getPartitions(); i++) {
            QueueConsumerClient client = new QueueConsumerClient(queue);
            client.setCallback(callback);
            consumers[i] = new Thread(client);
            consumers[i].start();
        }
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public void destroy() throws Exception {
        if (consumers != null) {
            for (Thread t : consumers) {
                t.interrupt();
            }
        }
    }

    public QueueWrapperForSpring getQueue() {
        return queue;
    }

    public void setQueue(QueueWrapperForSpring queue) {
        this.queue = queue;
    }

}
