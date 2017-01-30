package br.com.cinq.kafka.sample.repository;

import java.sql.Timestamp;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import br.com.cinq.kafka.sample.entity.Message;

public interface MessagesRepository extends JpaRepository<Message, Integer> {

	@Query("SELECT MIN(e.created) from Message e")
	public Timestamp findFirstCreated();

	@Query("SELECT MAX(e.created) from Message e")
	public Timestamp findLastCreated();

}
