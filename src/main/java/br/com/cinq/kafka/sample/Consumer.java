package br.com.cinq.kafka.sample;

/**
 * Interface for consumers
 */
public interface Consumer {
    public void start();

    public void setCallback(Callback callback);
}
