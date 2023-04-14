package com.ashu.tutorial.utils;

import customerManagement.avro.Customer;

import java.util.UUID;

public class CustomerGenerator {

    public static Customer getNext() {
        return new Customer((int)System.currentTimeMillis(), "Ashutosh"+UUID.randomUUID().toString(), "jai@email.com");
    }
}
