package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import reactor.core.publisher.Flux;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CsvUtilFile {

    private CsvUtilFile(){}

    public static List<Player> getPlayers() throws IOException {

        var uri =  CsvUtilFile.class.getClassLoader().getResource("data.csv");
        List<Player> list = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(uri.getFile()))) {
            List<String[]> registers = reader.readAll();
            registers.forEach(strings -> list.add(new Player(
                    Integer.parseInt(strings[0].trim()),
                    strings[1],
                    Integer.parseInt(Optional.of(strings[2].trim()).filter(h -> !h.isBlank()).orElse("0")),
                    strings[3],
                    strings[4],
                    Integer.parseInt(strings[5].trim()),
                    Integer.parseInt(strings[6].trim()),
                    strings[7]
            )));

           return list;

        } catch (IOException | CsvException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static List<String[]> createCsvFIle() throws IOException {

        var uri =  CsvUtilFile.class.getClassLoader().getResource("data.csv");

        FileWriter csvWriter = new FileWriter("players.csv");

        csvWriter.append("id");
        csvWriter.append(",");
        csvWriter.append("name");
        csvWriter.append(",");
        csvWriter.append("age");
        csvWriter.append(",");
        csvWriter.append("icon");
        csvWriter.append(",");
        csvWriter.append("national");
        csvWriter.append(",");
        csvWriter.append("winners");
        csvWriter.append(",");
        csvWriter.append("gamers");
        csvWriter.append(",");
        csvWriter.append("ranking");
        csvWriter.append(",");
        csvWriter.append("club");
        csvWriter.append(",\n");

        try (CSVReader reader = new CSVReader(new FileReader(uri.getFile()))) {

            List<String[]> registers = reader.readAll();
            registers.stream().forEach(jugador -> {

                try {


                    csvWriter.append(jugador[0].trim());
                    csvWriter.append(",");
                    csvWriter.append(jugador[1]);
                    csvWriter.append(",");
                    csvWriter.append(Optional.of(jugador[2].trim()).filter(h -> !h.isBlank()).orElse("0"));
                    csvWriter.append(",");
                    csvWriter.append(jugador[3]);
                    csvWriter.append(",");
                    csvWriter.append(jugador[4]);
                    csvWriter.append(",");
                    csvWriter.append(jugador[5]);
                    csvWriter.append(",");
                    csvWriter.append(jugador[6]);
                    csvWriter.append(",");
                    csvWriter.append(Double.toString(Double.parseDouble(jugador[5]) / Double.parseDouble(jugador[6])));
                    csvWriter.append(",");
                    csvWriter.append(jugador[7]);
                    csvWriter.append(",\n");

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            csvWriter.flush();
            csvWriter.close();
            return registers;

        } catch (IOException | CsvException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static Flux<Player> getAllPlayers(){
        var uri =  CsvUtilFile.class.getClassLoader().getResource("data.csv");
        List<Player> list = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(uri.getFile()))) {
            List<String[]> registers = reader.readAll();
            registers.forEach(strings -> list.add(new Player(
                    Integer.parseInt(strings[0].trim()),
                    strings[1],
                    Integer.parseInt(Optional.of(strings[2].trim()).filter(h -> !h.isBlank()).orElse("0")),
                    strings[3],
                    strings[4],
                    Integer.parseInt(strings[5].trim()),
                    Integer.parseInt(strings[6].trim()),
                    strings[7]
            )));

            Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
            return listFlux;

        } catch (IOException | CsvException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
