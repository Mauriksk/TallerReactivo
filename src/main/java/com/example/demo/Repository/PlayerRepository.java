package com.example.demo.Repository;

import com.example.demo.Models.PlayerDb;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PlayerRepository extends ReactiveMongoRepository<PlayerDb, String> {

}
