package com.demo.BulkInsert.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ElasticService {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    BulkRequest bulkRequest;
    @Autowired
    private RestHighLevelClient client;

    public void index(String indexName, String message) {
        index(indexName, message, "doc", XContentType.JSON);
    }

    public void indexAsString(String indexName, String type, String message) {
        index(indexName, message, type, XContentType.fromMediaType("text/plain; charset=utf-8"));
    }

    public void index(String indexName, String message, String type, XContentType contentType) {
        try {
            IndexRequest request = new IndexRequest(indexName);
            request.source(message, contentType);
            request.type(type);

            client.index(request);
        } catch (IOException e) {
            log.error("IO Exception: {}: {}", e.getMessage(), e.toString());
        }
    }

    public void upsert(String index, String message, String id) {
        upsert(index, message, id, "doc", XContentType.JSON);
    }

    public void upsert(String index, String message, String id, String type, XContentType contentType) {
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(message, contentType);

        UpdateRequest updateRequest =
                new UpdateRequest(index, type, id).retryOnConflict(10).doc(message, contentType).upsert(indexRequest);

        try {
            client.update(updateRequest);

        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }


    public int checkIndexExistence(String indexName) throws IOException {
        Response response = client.getLowLevelClient().performRequest("HEAD", "/" + indexName);
        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode;
    }

    public void bulkRequestIndex(String indexName, List<Map<String, Object>> mapList) {
        BulkRequest bulkRequest = new BulkRequest();
        int[] iarr = {0};
        if(mapList.size() > 100000){
            List<List<Map<String, Object>>> mapList2 = ListUtils.partition(mapList, 100000);
            mapList2.forEach(x ->{
                BulkRequest bulkRequestPortion = new BulkRequest();
                x.forEach(y ->{
                    IndexRequest request = new IndexRequest(indexName, "doc")
                            .source(objectMapper.convertValue(y, Map.class));
                    bulkRequestPortion.add(request);
                });
                try {
                    long startTime = System.nanoTime();
                    log.info("run client bulk " + iarr[0]++);
                    client.bulk(bulkRequestPortion, RequestOptions.DEFAULT);
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime);
                    log.info("finished client bulk " +iarr[0]++);
                    log.info("Bulk Execution Time : " + duration);
                } catch (IOException e) {
                    log.error("Bulk IO Exception: {}", e.getMessage());
                }
            });
        }else{
           mapList.forEach(x -> {
            IndexRequest request = new IndexRequest(indexName, "doc")
                    .source(objectMapper.convertValue(x, Map.class));
            bulkRequest.add(request);
        });
       try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);

        } catch (IOException e) {
            log.error("Bulk IO Exception: {}", e.getMessage());
            }
        }
    }

}
