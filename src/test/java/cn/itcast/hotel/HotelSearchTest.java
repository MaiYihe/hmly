package cn.itcast.hotel;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HotelSearchTest {
    private RestClient restClient;
    private ElasticsearchClient client;

    @BeforeEach
    void setUp() {
        this.restClient = RestClient.builder(
                HttpHost.create("http://127.0.0.1:9200")).build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        this.client = new ElasticsearchClient(transport);
    }

    @AfterEach
    void tearDown() throws IOException {
        this.restClient.close();
    }

    // 查询所有
    @Test
    void testMatchAll() throws IOException {
        SearchResponse<Object> response = client.search(s -> s
                .index("hotel") // 改为索引名
                .query(q -> q.matchAll(m -> m)),
                Object.class);

        // 打印命中总数
        log.info("Total hits = {}", response.hits().total().value());

        // 遍历文档
        for (Hit<Object> hit : response.hits().hits()) {
            log.info("Doc ID = {}", hit.id());
            log.info("Source = {}", hit.source());
            log.info("--------------------------");
        }
    }

    // 全文检索查询
    @Test
    void testMultiMatch() throws IOException {
        SearchResponse<Object> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                        .multiMatch(mm -> mm
                                .query("如家")
                                .fields("brand", "name"))),
                Object.class);

        log.info("Total hits = {}", response.hits().total().value());

        for (Hit<Object> hit : response.hits().hits()) {
            log.info("Doc ID = {}", hit.id());
            log.info("Source = {}", hit.source());
        }
    }

    // 复合查询结合精确查询
    @Test
    void testBoolMustAndFilter() throws IOException {
        SearchResponse<Object> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                        .bool(b -> b
                                .must(m -> m
                                        .term(t -> t
                                                .field("city")
                                                .value("杭州")))
                                .filter(f -> f
                                        .range(r -> r
                                                .field("price")
                                                .lte(JsonData.of(250)))))),
                Object.class);

        log.info("Total hits = {}", response.hits().total().value());

        for (Hit<Object> hit : response.hits().hits()) {
            log.info("Doc ID = {}", hit.id());
            log.info("Score = {}", hit.score());
            log.info("Source = {}", hit.source());
            log.info("------------------------");
        }
    }

}
