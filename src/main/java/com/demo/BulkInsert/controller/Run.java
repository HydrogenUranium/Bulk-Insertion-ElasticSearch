package com.demo.BulkInsert.controller;

import com.demo.BulkInsert.consumer.CSVBulkInsertion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Run {
    @Autowired
    CSVBulkInsertion csvBulkInsertion;

    @RequestMapping(value = {"/csv"})
    public ResponseEntity<String> testCSV() {
        try {
            csvBulkInsertion.executeCSV("CSV-2012231227012586673.txt");
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }
}
