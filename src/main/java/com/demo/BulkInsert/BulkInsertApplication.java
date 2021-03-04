package com.demo.BulkInsert;

import com.demo.BulkInsert.consumer.CSVBulkInsertion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BulkInsertApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(BulkInsertApplication.class, args);
    }

    @Autowired
    private CSVBulkInsertion csvBulkInsertion;

    @Override
    public void run(String... args) throws Exception {

        csvBulkInsertion.executeCSV("CSV-2012231227012586673.txt");

    }
}
