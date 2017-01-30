package br.com.cinq.kafka.sample.mono;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueWrapperForSpring {

    private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    public void send(String message) {
        queue.add(message);
    }

    public String poll() throws InterruptedException {
        return queue.poll(1, TimeUnit.HOURS);
    }

}
