package br.com.cinq.kafka.resource;

import java.sql.Timestamp;

import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import br.com.cinq.kafka.sample.Consumer;
import br.com.cinq.kafka.sample.Producer;
import br.com.cinq.kafka.sample.repository.MessagesRepository;

/**
 * Resource to send messages.
 * @author Adriano Kretschmer
 */
@Path("/kafka")
public class KafkaSampleResource {
    Logger logger = LoggerFactory.getLogger(KafkaSampleResource.class);

    // For now, store the information on a bean, in memory
    @Autowired
    @Qualifier("sampleProducer")
    Producer sampleProducer;

    @Autowired
    @Qualifier("sampleConsumer")
    Consumer sampleConsumer;

    @Autowired
    MessagesRepository dao;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response send(String message) {
        try {
            logger.info("Sending message {}", message);

            sampleProducer.send(message);

        } catch (Exception e) {
            logger.error("An exception occurred while sending a message", e);
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
        }

        return Response.ok().build();
    }

    /**
     * Simulates a load of messages, check if Kafka received and delivered them all. IRL Transactions should be
     * managed by a facade / Model class
     * @param repeat Number of repetitions
     * @param message A messages. Add a placeholder {count} in the payload, it will be replaced by the unit number.
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{repeat}")
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public Response sendBatch(@PathParam("repeat") int repeat, String message) {
    	long producerEnd, producerStart;
        try {
            logger.debug("Deleting all messages");
            dao.deleteAll();

            logger.info("Sending message - repeat {} {}", message, repeat);

            producerStart = System.currentTimeMillis();
            for (int i = 0; i < repeat; i++) {
                sampleProducer.send(message.replace("{count}", "{" + i + "}"));
            }
            producerEnd = System.currentTimeMillis();

        } catch (Exception e) {
            logger.error("An exception occurred while sending messages", e);
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
        }

        // Check if all messages were received
        long count = -1;
        long lastCount = 0;
        try {
            Thread.sleep(3000);
            if (dao.count() == 0) {
                return Response.serverError().entity("No messages were received after timeout").build();
            }

            while (lastCount != count) {
                count = dao.count();
                Thread.sleep(3000);
                lastCount = dao.count();
            }
        } catch (Exception e) {
            return Response.serverError().entity("An exception occurred").build();
        }

        Timestamp consumerStart = dao.findFirstCreated();
        Timestamp consumerEnd = dao.findLastCreated();

        StringBuffer sb = new StringBuffer();
        sb.append(String.format("Producer : %d ms\n", producerEnd - producerStart));
        sb.append(String.format("Consumer : %d ms\n", consumerEnd.getTime() - consumerStart.getTime()));
        sb.append(String.format("Received %d messages", count));

        if (count == repeat)
            return Response.ok().entity(sb.toString()).build();

        return Response.serverError().entity(sb.toString()).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/start")
    public Response startConsumer() {
        try {
            logger.info("Starting consumers");

            sampleConsumer.start();

        } catch (Exception e) {
            logger.error("An exception occurred while starting consumers", e);
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
        }

        return Response.ok().build();
    }

}
