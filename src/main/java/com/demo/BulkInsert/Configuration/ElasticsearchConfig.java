package com.demo.BulkInsert.Configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ElasticsearchConfig {
    @Value("${elasticsearch.hostname}")
    private String hostname;

    @Value("${elasticsearch.port}")
    private int port;

    @Value("${elasticsearch.username}")
    private String username;

    @Value("${elasticsearch.password}")
    private String password;

    @Value("${elasticsearch.protocol}")
    private String protocol;

    @Bean
    public RestHighLevelClient client (){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));


        return new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, protocol))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                                credentialsProvider)));

    }
    @Bean
    public BulkRequest bulkRequest (){
        return new BulkRequest();
    }

    @Bean
    public ActionListener<BulkResponse> listener (){
        ActionListener<BulkResponse> listener;
        return listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse BulkResponse) { }
            @Override
            public void onFailure(Exception e) { }
        };
    }
}
