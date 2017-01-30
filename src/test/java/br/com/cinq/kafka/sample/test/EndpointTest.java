package br.com.cinq.kafka.sample.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import br.com.cinq.kafka.sample.application.Application;
import br.com.cinq.kafka.sample.callback.MyCallback;
import br.com.cinq.kafka.sample.repository.MessagesRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebIntegrationTest(randomPort = true)
@IntegrationTest("server.port=9000")
@ActiveProfiles("unit")
public class EndpointTest {
    Logger logger = LoggerFactory.getLogger(EndpointTest.class);

    private final String localhost = "http://localhost:";

    @Value("${local.server.port}")
    private int port;

    private RestTemplate restTemplate = new TestRestTemplate();

    @Autowired
    MessagesRepository dao;

    @Before
    public void setUp() {
        dao.deleteAll();
    }

    @Test
    public void testPostAMessage() throws InterruptedException {
        String newMessage = "A wild message appears!";

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<String>(newMessage, headers);

        ResponseEntity<Void> response = this.restTemplate.exchange(this.localhost + this.port + "/rest/kafka",
                HttpMethod.POST, entity, Void.class);

        Assert.assertEquals(HttpStatus.OK, response.getStatusCode());

        Thread.sleep(2000L);

        Assert.assertNotEquals(2, dao.count());

    }

    @Test
    public void testPostSeveralMessages() throws InterruptedException {
        String newMessage = "A pack of wild messages appears!{count}";

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<String>(newMessage, headers);

        ResponseEntity<Void> response = this.restTemplate.exchange(this.localhost + this.port + "/rest/kafka/10",
                HttpMethod.POST, entity, Void.class);

        Assert.assertEquals(HttpStatus.OK, response.getStatusCode());

        Thread.sleep(2000L);

        Assert.assertEquals(10, dao.count());

    }

}
