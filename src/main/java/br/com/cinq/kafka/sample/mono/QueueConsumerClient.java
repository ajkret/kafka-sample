package br.com.cinq.kafka.sample.mono;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import br.com.cinq.kafka.sample.Callback;

public class QueueConsumerClient implements Runnable {
    Logger logger = LoggerFactory.getLogger(QueueConsumerClient.class);

    private Callback callback;

    private final String TXID = "test";

    private QueueWrapperForSpring queue;

    public QueueConsumerClient(QueueWrapperForSpring queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        UUID uuid = UUID.randomUUID();
        MDC.put(TXID, uuid.toString());

        try {
            while (true) {
                String message = queue.poll();
                if (message == null)
                    continue;
                callback.receive(message);
            }
        } catch (InterruptedException e) {
        } finally {
            MDC.remove(TXID);
        }
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }
}
