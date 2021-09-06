package com.github.pukkaone.odata.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.ResolvableType;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;

/**
 * Tests OData server.
 */
@Import(TestElasticsearchConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ODataIntegrationTest {

  @TestConfiguration
  static class MyConfiguration {
    @Bean
    @Primary
    public JacksonTester<JsonNode> jacksonTester(ObjectMapper objectMapper) {
      return new JacksonTester<>(
          ODataIntegrationTest.class, ResolvableType.forClass(JsonNode.class), objectMapper);
    }
  }

  private static final String INDEX_NAME = "customer";

  @Autowired
  private RestHighLevelClient elasticsearchClient;

  @Autowired
  private ElasticsearchRestTemplate elasticsearchTemplate;

  @Autowired
  private TestRestTemplate testRestTemplate;

  @Autowired
  private JacksonTester<JsonNode> json;

  private boolean createIndex(String mappings) throws Exception {
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME)
        .source(mappings, XContentType.JSON);
    return elasticsearchClient.indices()
        .create(createIndexRequest, RequestOptions.DEFAULT)
        .isAcknowledged();
  }

  private void waitForGreenStatus() throws Exception {
    ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(INDEX_NAME)
        .waitForGreenStatus();
    elasticsearchClient.cluster()
        .health(clusterHealthRequest, RequestOptions.DEFAULT);
  }

  private String indexDocument(String documentId, String documentFile) throws Exception {
    String source = StreamUtils.copyToString(
        ODataIntegrationTest.class.getResourceAsStream(documentFile),
        StandardCharsets.UTF_8);

    IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
        .id(documentId)
        .source(source, XContentType.JSON);
    return elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT)
        .getId();
  }

  private void refresh() throws Exception {
    RefreshRequest refreshRequest = new RefreshRequest(INDEX_NAME);
    elasticsearchClient.indices()
        .refresh(refreshRequest, RequestOptions.DEFAULT);
  }

  @BeforeAll
  public void beforeClass() throws Exception {
    elasticsearchTemplate.indexOps(IndexCoordinates.of(INDEX_NAME))
        .delete();

    String mappings = StreamUtils.copyToString(
        ODataIntegrationTest.class.getResourceAsStream("mappings.json"),
        StandardCharsets.UTF_8);
    createIndex(mappings);
    waitForGreenStatus();

    indexDocument("entityId1", "entity1-source.json");
    indexDocument("entityId2", "entity2-source.json");
    indexDocument("entityId3", "entity3-source.json");

    refresh();
  }

  private String joinPathSegments(String... pathSegments) {
    return "/odata/" + INDEX_NAME + String.join("", pathSegments);
  }

  private void assertEquals(String expectedFile, JsonNode actual) throws Exception {
    assertThat(json.write(actual)).isStrictlyEqualToJson(expectedFile, ODataIntegrationTest.class);
  }

  @Test
  public void should_get_entity_sets() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/"), JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertEquals("entity-sets-expected.json", response.getBody());
  }

  @Test
  public void should_get_metadata() throws Exception {
    String metadata = testRestTemplate.getForObject(
        joinPathSegments("/$metadata"),
        String.class);

    String expected = StreamUtils.copyToString(
        getClass().getResourceAsStream("metadata-expected.xml"), StandardCharsets.UTF_8);
    assertThat(metadata).isXmlEqualTo(expected);
  }

  @Test
  public void should_read_entity() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer('entityId1')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertEquals("entity1-expected.json", response.getBody());
  }

  @Test
  public void should_find_all_entities() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathStringValue("$['@odata.context']")
        .isEqualTo("$metadata#customer");
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 20, 30);
  }

  @Test
  public void should_filter_integer_property_and() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=integerProperty gt 19 and integerProperty lt 21"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_integer_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=integerProperty eq 20"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_integer_property_ne() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=integerProperty ne 20"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_integer_property_or() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=integerProperty lt 19 or integerProperty gt 21"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_keyword_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=keywordProperty eq 'alpha bravo'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_keyword_property_ne() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=keywordProperty ne 'alpha bravo'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_object_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=objectProperty/objectTextProperty eq 'c'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10);
  }

  @Test
  public void should_filter_text_property_contains() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=contains(textProperty,'delta')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_text_property_not_contains() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$filter=not contains(textProperty,'delta')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_order_by_multiple_properties() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$orderby=longProperty,keywordProperty desc"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30, 20);
  }

  @Test
  public void should_skip() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$skip=1"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20, 30);
  }

  @Test
  public void should_top() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/customer?$top=2"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 20);
  }
}
