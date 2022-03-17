package com.example.demo.Controller;


import com.example.demo.Models.PlayerDb;
import com.example.demo.Services.PlayerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@CrossOrigin("*")
@RequestMapping("/api")
public class PlayerController {

    private PlayerService playerService;

    @Autowired
    public PlayerController(PlayerService playerService){
        this.playerService = playerService;
    }

    @GetMapping(value = "/players")
    public Flux<PlayerDb> getPlayers(){
        return playerService.getPlayers();
    }

    @GetMapping(value = "/players/filerByAge")
    public Flux<PlayerDb> getPlayersOlderThan34(){
        return playerService.getPlayersOlderThan34();
    }

    @GetMapping(value = "/players/filterByClub/{club}")
    public Flux<PlayerDb> getPlayersByClub(@PathVariable("club") String club){
        return playerService.getPlayersByClub(club);
    }

    @GetMapping(value = "/playersByCountries")
    public Flux<List<PlayerDb>> rankingPlayersByCountry(){
        return playerService.rankingPlayersByCountry();
    }

}
