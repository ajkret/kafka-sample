package br.com.cinq.kafka.resource;

import javax.transaction.Transactional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import br.com.cinq.kafka.sample.Producer;
import br.com.cinq.kafka.sample.repository.MessagesRepository;

/**
 * Greet Service
 *
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
    MessagesRepository dao;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response send(String message) {
        try {
            logger.info("Sending message {}", message);

            sampleProducer.send(message);

        } catch (Exception e) {
            logger.error("An exception occurred during Greet message update", e);
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity("exception").build();
        }

        return Response.ok().build();
    }

    /**
     * Simulates a load of messages, check if Kafka received and delivered them all
     * @param repeat Number of repetitions
     * @param message A messages. Add a placeholder {count} in the payload, it will be replaced by the unit number.
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{repeat}")
    @Transactional(Transactional.TxType.SUPPORTS)
    public Response sendBatch(@PathParam("repeat") int repeat, String message) {
        try {
            logger.debug("Deleting all messages");
            dao.deleteAll();

            logger.info("Sending message - repeat {} {}", message, repeat);

            for (int i = 0; i < repeat; i++) {
                sampleProducer.send(message.replace("{count}", "{" + i + "}"));
            }

        } catch (Exception e) {
            logger.error("An exception occurred during Greet message update", e);
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

        if (count == repeat)
            return Response.ok().entity("Received " + count + " messages").build();

        return Response.serverError().entity("Received " + count + " messages").build();
    }
}
