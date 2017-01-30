package br.com.cinq.kafka.sample;

import br.com.cinq.kafka.sample.exception.QueueException;

/**
 * Minimal interface for message Producer.
 */
public interface Producer {
    public void send(String message) throws QueueException;
}
