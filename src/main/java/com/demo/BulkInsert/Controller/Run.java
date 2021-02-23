package com.demo.BulkInsert.Controller;

import com.demo.BulkInsert.Service.csvBulkInsertion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Run {
    @Autowired
    csvBulkInsertion csvBulkInsertion;

    @RequestMapping(value={"/csv"})
    public ResponseEntity<String> testCSV(@RequestParam String file){
        try{
            csvBulkInsertion.executeCSV(file);
            return new ResponseEntity<>(HttpStatus.OK);
        }catch(Exception e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }
}
