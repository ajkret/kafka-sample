package br.com.cinq.kafka.sample.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.cinq.kafka.sample.entity.Message;

public interface MessagesRepository extends JpaRepository<Message, Integer> {


}
