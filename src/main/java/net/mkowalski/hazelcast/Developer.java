package net.mkowalski.hazelcast;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Developer implements Serializable {

    private int age;
    private boolean male;
    private int salary;

}
