package net.mkowalski.hazelcast.util;

import net.mkowalski.hazelcast.Developer;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class InputDataLibrary {

    private InputDataLibrary() {
        // hidden
    }

    public static Queue<String> colors() {
        return colorsRepeated(1);
    }

    public static Queue<String> colorsRepeated(int times) {
        List<String> colors = Arrays.asList("blue", "green", "red", "yellow", "white", "purple", "black", "orange");
        Queue<String> colorsRepeated = new LinkedList<>();
        for (int i = 0; i < times; ++i) {
            colorsRepeated.addAll(colors);
        }
        return colorsRepeated;
    }

    public static Developer randomDeveloper() {
        SecureRandom random = new SecureRandom();
        int age = 18 + random.nextInt(50);
        boolean male = random.nextBoolean();
        int salary = 3000 + random.nextInt(15000);
        return new Developer(age, male, salary);
    }

}
