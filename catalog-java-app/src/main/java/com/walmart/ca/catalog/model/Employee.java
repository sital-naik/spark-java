package com.walmart.ca.catalog.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
@Data
@ToString
public class Employee implements Serializable {
    private int age;
    private String designation;
    private String name;
    private Address address;
}
