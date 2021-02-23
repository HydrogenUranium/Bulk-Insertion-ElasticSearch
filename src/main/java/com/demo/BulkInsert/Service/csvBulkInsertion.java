package com.demo.BulkInsert.Service;

import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Slf4j
public class csvBulkInsertion {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIME_ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:sss";

    @Autowired
    RestHighLevelClient client;
    @Autowired
    BulkRequest bulkRequest;


    public void executeCSV(String fileName){
        String path = "C:\\Users\\HaikalHafizKadar\\Desktop\\BulkInsert\\BulkInsert\\temp\\";
        String fileExecute = path + fileName;
        List<Map<String, Object>> mapList = new ArrayList<>();
        log.info("fileExecute {}", fileExecute);
        int count=0;
        try{
            CSVReader reader = new CSVReader(new FileReader(fileExecute));
            reader.skip(1);
            String[] line;
            while ((line = reader.readNext()) != null){
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

           bulkRequest.add(getIndexRequest(mapList,"haikal-collection-master"));
           // bulkProcessor.add(getIndexRequest(mapList,"emd-collection-master"));
            long timeStart = System.nanoTime();

            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("bulkRequest " + bulkRequest.getDescription());
           //log.info("print mapList " + mapList);
            if(bulkResponse.hasFailures()){
                log.info("Bulk request has failure " + bulkResponse.status());
            }else {
                for (BulkItemResponse bulk : bulkResponse.getItems()) {
                    log.info("bulk respond " + bulk.getResponse());
                    log.info("bulk respond status " + bulk.status());
                }
            }
            long endTime = System.nanoTime();
            long elapsedTime  =  endTime - timeStart;
            log.info("Execution Time " + elapsedTime  /1_000_000_000.0);
        }catch (Exception e){
            log.info("Error processing file {}",e.getMessage());
        }
    }

    public static IndexRequest getIndexRequest(List<Map<String, Object>> data, String index)throws Exception {
        IndexRequest indexRequest = null;
        indexRequest = new IndexRequest(index).type("_doc").id(UUID.randomUUID().toString()).source(data, XContentType.JSON);
        return indexRequest;
    }

}
