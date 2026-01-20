package cn.itcast.hotel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
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
    void testAddDocument() throws IOException {
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

    @Test
    void testGetDocumentById() throws IOException {
        String idValue = "61075";

        // 根据 id 从 ES 中查询文档
        GetResponse<HotelDoc> response = client.get(g -> g
                .index(indexName)
                .id(idValue),
                HotelDoc.class);
        assertTrue(response.found(), "ES 中应该存在 id=" + idValue + " 的文档");

        // 拿到文档内容
        HotelDoc hotelDoc = response.source();

        assertNotNull(hotelDoc);
        log.debug("hotelDoc = {}", hotelDoc);
    }

    @Test
    void testUpdateDocument() throws IOException{
        String idValue = "61075";

        // 1. 构造局部更新内容
        Map<String, Object> updateFields = new HashMap<>();
        updateFields.put("price", 399);
        updateFields.put("score", 4.8);

        // 2. 调用 update API
        UpdateResponse<HotelDoc> response = client.update(u -> u
                .index(indexName)
                .id(idValue)
                .doc(updateFields),
                HotelDoc.class);

        // 3. 断言：更新成功
        assertNotNull(response);
        assertEquals(Result.Updated, response.result());
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
