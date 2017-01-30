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
import br.com.cinq.kafka.sample.entity.Person;
import br.com.cinq.kafka.sample.repository.PersonRepository;

/**
 * Eye candy: implements a sample in using JpaRespositories
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebIntegrationTest(randomPort = true)
@IntegrationTest("server.port=9000")
@ActiveProfiles("unit")
public class PersonRepositoryTest {

    @Autowired
    private PersonRepository dao;

    @Test
    public void testQueryPerson() {

        Assert.assertNotNull(dao);

        List<Person> list = dao.findLikeName("Web");

        Assert.assertEquals(1, list.size());
    }
}
