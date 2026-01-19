package cn.itcast.hotel;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HotelIndexTest {
    private RestClient restClient;
    private ElasticsearchClient client;

    @Test
    void testInit() {
        System.out.println(client);
    }

    @Test
    void createHotelIndex() throws IOException {
        String indexName = "hotel";

        log.debug("Checking if index [{}] exists...", indexName);

        // 先判断 ES 中该索引是否存在
        boolean exists = client.indices()
                .exists(e -> e.index(indexName))
                .value();

        if (exists) {
            log.debug("Index [{}] already exists, skip creation.", indexName);
            return;
        }

        log.debug("Index [{}] not found, creating...", indexName);

        // 判断 Json 文件是否存在
        InputStream is = getClass()
                .getClassLoader()
                .getResourceAsStream("es/hotel-index.json");

        if (is == null) {
            throw new IllegalStateException("Mapping file not found: es/hotel-index.json");
        }
        
        // 创建索引
        client.indices().create(c -> c
                .index(indexName)
                .withJson(is));

        log.info("Index [{}] created successfully.", indexName);
    }

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
}
