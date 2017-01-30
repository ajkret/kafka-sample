package br.com.cinq.kafka.sample.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import br.com.cinq.kafka.sample.entity.Person;

/**
 * Eye candy: implements a sample in using JpaRespositories
 */
public interface PersonRepository extends JpaRepository<Person, Integer> {

    @Query("SELECT e FROM Person e where name LIKE CONCAT('%',?1,'%')")
    List<Person> findLikeName(String name);
}
