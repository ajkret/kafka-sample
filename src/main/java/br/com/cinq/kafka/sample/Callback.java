package br.com.cinq.kafka.sample;

/**
 * Define the interface to receive messages
 */
public interface Callback {
    public void receive(String message);
}
