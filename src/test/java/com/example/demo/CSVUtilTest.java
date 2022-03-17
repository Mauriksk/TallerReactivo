package com.example.demo;

import com.example.demo.Models.PlayerDb;
import com.example.demo.Services.PlayerService;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    private PlayerService playerService;

    @Test
    void converterData() throws IOException {
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void converterDataToFlux() throws IOException {
        Flux<Player> list = CsvUtilFile.getAllPlayers();
        Mono<Map<String, Collection<Player>>> listFilter = list
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> list
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35() throws IOException {
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test
    void reactive_filtrarJugadoresMayoresA35() throws IOException {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }


    @Test
    void filtradoPorVictorias() throws IOException {


        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();

        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(x -> x.national.equals(playerB.national)))
                ).distinct()
                .sort((k, player) -> player.winners)
                .collectMultimap(Player::getNational);

        System.out.println("Nacionalidad => [" + listFilter.block().size() + "]");
        listFilter.block().forEach((pais, players) -> {
            System.out.println("\n[Pais => " + pais + "]");
            players.forEach(player -> {
                System.out.println("Nombre => player.name  Partidos ganados => " + player.winners );
            });
        });
    }

    @Test
    void jugadoresMayoresA34years() throws IOException {

        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> flux = Flux.fromStream(list.parallelStream()).cache();

        Mono<Map<String, Collection<Player>>> listFilter = flux
                .filter(p -> p.age >= 34 && p.club.equals("SV Sandhausen"))
                .distinct()
                .collectMultimap(Player::getClub);
        System.out.println("Jugadores Superiores a 34 años");
        System.out.print("Cuadro => ");
        listFilter.block().forEach((equipo, p) -> {
            System.out.println(equipo + "]\n");
            p.forEach(player -> {
                System.out.println("Jugador => " + player.name + " Edad => " + player.age);
                assert player.club.equals("SV Sandhausen");
            });
        });
        System.out.println("--------------------------------------------");
        System.out.println(listFilter.block().size());
        assert listFilter.block().size() == 1;

    }

}
