package com.demo.BulkInsert.consumer;

import com.demo.BulkInsert.service.ElasticService;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class CSVBulkInsertion {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIME_ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:sss";

    @Autowired
    private ElasticService elasticService;

    public void executeCSV(String fileName) {
        String path = "C:\\Users\\AmirBahar\\My Projects\\projects\\emd-workspace\\poc\\BulkInsertionDemo\\temp\\";
        String fileExecute = path + fileName;
        List<Map<String, Object>> mapList = new ArrayList<>();
        log.info("fileExecute {}", fileExecute);
        int count = 0;
        try {
            CSVReader reader = new CSVReader(new FileReader(fileExecute));
            reader.skip(1);
            String[] line;
            while ((line = reader.readNext()) != null) {
                log.info("while loop count " + count++);
                Map<String, Object> maps = new HashMap();
                maps.put("subClientId1", line[0]);
                maps.put("subClientId2", line[1]);
                maps.put("fqhn", line[2]);
                maps.put("displayName", line[3]);
                maps.put("operatingSystem", line[4]);
                maps.put("hostname", line[5]);
                maps.put("managingSDC", line[6]);
                maps.put("serverId", line[7]);
                maps.put("datatype", line[8]);
                maps.put("statusCode", line[9]);
                maps.put("statusMessage", line[10]);
                maps.put("collectionDatetime", line[11]);
                maps.put("clientId", line[12]);
                maps.put("datafileDate", line[13]);
                maps.put("recordedToss", line[14]);
                maps.put("recordedDone", line[15]);
                maps.put("mlm1", line[16]);
                maps.put("mlm2", line[17]);
                maps.put("operatingSystemVersion", line[18]);
                maps.put("collectionDate", line[19]);
                maps.put("timestamp", new SimpleDateFormat(DATETIME_ISO8601_FORMAT)
                        .format(new SimpleDateFormat(DATETIME_FORMAT).parse(line[11])));

                mapList.add(maps);
            }
            log.info("List: {}", mapList);

            elasticService.bulkRequestIndex("amir-collection-master2", mapList);

        } catch (Exception e) {
            log.info("Error processing file {}", e.getMessage());
        }
    }

}
