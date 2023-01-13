package com.walmart.ca.catalog.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Address implements Serializable {
    private String city;
    private String state;
    private String pin;
}
