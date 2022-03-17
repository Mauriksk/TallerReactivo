package com.example.demo.Models;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "players")
public class PlayerDb {

    @Id
    private String id;
    public String name;
    public int age;
    public String icon;
    public String national;
    public int winners;
    public int gamers;
    public double ranking;
    public String club;

    public PlayerDb(String id, String name, int age, String icon, String national, int winners, int gamers, double ranking, String club) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.icon = icon;
        this.national = national;
        this.winners = winners;
        this.gamers = gamers;
        this.ranking = ranking;
        this.club = club;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getIcon() {
        return icon;
    }

    public String getNational() {
        return national;
    }

    public int getWinners() {
        return winners;
    }

    public int getGamers() {
        return gamers;
    }

    public double getRanking() {
        return ranking;
    }

    public String getClub() {
        return club;
    }
}
