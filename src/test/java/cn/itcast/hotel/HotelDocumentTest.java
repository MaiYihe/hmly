package cn.itcast.hotel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest
public class HotelDocumentTest {

    private RestClient restClient;
    private ElasticsearchClient client;

    @Autowired
    private IHotelService hotelService;

    private String indexName = "hotel";

    @Test
    void testAddDocument() throws IOException{
        // 根据 id 查询酒店数据
        Hotel hotel = hotelService.getById(61075L);

        // 转化为文档类型（主要是实体类地理位置坐标不一致）
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 创建文档
        IndexResponse response = client.index(i -> i
                .index(indexName)
                .id(hotelDoc.getId().toString()) // 这条文档的 id
                .document(hotelDoc));

        // 断言：ES 是否接受了这次写入
        assertNotNull(response);
        assertEquals(Result.Created, response.result());
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
