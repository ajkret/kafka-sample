package br.com.cinq.kafka.sample.exception;

public class QueueException extends Exception {
   private static final long serialVersionUID = 8874665227805706522L;

   public QueueException() {
      super();
   }

   public QueueException(String message) {
      super(message);
   }

   public QueueException(String message, Throwable t) {
      super(message, t);
   }
}
