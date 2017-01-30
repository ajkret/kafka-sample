package br.com.cinq.kafka.repository.test;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import br.com.cinq.kafka.sample.application.Application;
import br.com.cinq.kafka.sample.entity.Message;
import br.com.cinq.kafka.sample.entity.Person;
import br.com.cinq.kafka.sample.repository.MessagesRepository;
import br.com.cinq.kafka.sample.repository.PersonRepository;

/**
 * Eye candy: implements a sample in using JpaRespositories
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebIntegrationTest(randomPort = true)
@IntegrationTest("server.port=9000")
@ActiveProfiles("unit")
public class MessageRepositoryTest {

    @Autowired
    private MessagesRepository dao;

    @Test
    public void testInsertMessages() {

        Assert.assertNotNull(dao);

        for(int i=0;i<10;i++) {
            Message entity = new Message();
            entity.setMessage("Message created at " + System.currentTimeMillis());
            dao.save(entity);
        }

        Assert.assertEquals(10, dao.count());

        dao.deleteAll();

        Assert.assertEquals(0, dao.count());
    }
}
