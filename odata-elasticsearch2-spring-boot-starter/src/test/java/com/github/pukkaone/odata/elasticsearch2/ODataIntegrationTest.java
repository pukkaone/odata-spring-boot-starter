package com.github.pukkaone.odata.elasticsearch2;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Requests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.ResolvableType;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;

/**
 * Tests OData server.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
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

  private static final String INDEX_NAME = "example-index";
  private static final String TYPE_NAME = "ExampleType";

  private static EmbeddedElasticsearchServer elasticsearchServer;
  private static ElasticsearchTemplate elasticsearchTemplate;

  @Value("${server.context-path}")
  private String contextPath;

  @Autowired
  private TestRestTemplate testRestTemplate;

  @Autowired
  private JacksonTester<JsonNode> json;

  private static boolean createIndex(String indexName, String mappings) {
    CreateIndexRequest createIndexRequest = Requests.createIndexRequest(indexName)
        .source(mappings);

    return elasticsearchTemplate.getClient()
        .admin()
        .indices()
        .create(createIndexRequest)
        .actionGet()
        .isAcknowledged();
  }

  private static void waitForGreenStatus(String indexName) {
    elasticsearchTemplate.getClient()
        .admin()
        .cluster()
        .prepareHealth(indexName)
        .setWaitForGreenStatus()
        .get();
  }

  private static String indexDocument(String documentId, String documentFile) throws Exception {
    String source = StreamUtils.copyToString(
        ODataIntegrationTest.class.getResourceAsStream(documentFile),
        StandardCharsets.UTF_8);

    return elasticsearchTemplate.getClient()
        .prepareIndex(INDEX_NAME, TYPE_NAME, documentId)
        .setSource(source)
        .get()
        .getId();
  }

  private static void refresh(String indexName) {
    elasticsearchTemplate.getClient()
        .admin()
        .indices()
        .prepareRefresh(indexName)
        .get();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    elasticsearchServer = new EmbeddedElasticsearchServer();
    elasticsearchTemplate = new ElasticsearchTemplate(elasticsearchServer.getClient());

    elasticsearchTemplate.deleteIndex(INDEX_NAME);

    String mappings = StreamUtils.copyToString(
        ODataIntegrationTest.class.getResourceAsStream("mappings.json"),
        StandardCharsets.UTF_8);
    createIndex(INDEX_NAME, mappings);
    waitForGreenStatus(INDEX_NAME);

    indexDocument("entityId1", "entity1-source.json");
    indexDocument("entityId2", "entity2-source.json");
    indexDocument("entityId3", "entity3-source.json");

    refresh(INDEX_NAME);
  }

  @AfterClass
  public static void afterClass() {
    elasticsearchServer.stop();
  }

  private String joinPathSegments(String... pathSegments) {
    return contextPath + "/odata/" + INDEX_NAME + String.join("", pathSegments);
  }

  private void assertEquals(String expectedFile, JsonNode actual) throws Exception {
    assertThat(json.write(actual)).isStrictlyEqualToJson(expectedFile, ODataIntegrationTest.class);
  }

  @Test
  public void should_get_entity_sets() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments(""), JsonNode.class);

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
        joinPathSegments("/ExampleType('entityId1')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertEquals("entity1-expected.json", response.getBody());
  }

  @Test
  public void should_find_all_entities() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathStringValue("$['@odata.context']")
        .isEqualTo("$metadata#ExampleType");
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 20, 30);
  }

  @Test
  public void should_filter_integer_property_and() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=integerProperty gt 19 and integerProperty lt 21"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_integer_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=integerProperty eq 20"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_integer_property_ne() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=integerProperty ne 20"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_integer_property_or() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=integerProperty lt 19 or integerProperty gt 21"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_keyword_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=keywordProperty eq 'alpha bravo'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_keyword_property_ne() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=keywordProperty ne 'alpha bravo'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_filter_object_property_eq() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=objectProperty/objectTextProperty eq 'c'"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10);
  }

  @Test
  public void should_filter_text_property_contains() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=contains(textProperty,'delta')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20);
  }

  @Test
  public void should_filter_text_property_not_contains() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$filter=not contains(textProperty,'delta')"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30);
  }

  @Test
  public void should_order_by_multiple_properties() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$orderby=longProperty,textProperty desc"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 30, 20);
  }

  @Test
  public void should_skip() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$skip=1"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(20, 30);
  }

  @Test
  public void should_top() throws Exception {
    ResponseEntity<JsonNode> response = testRestTemplate.getForEntity(
        joinPathSegments("/ExampleType?$top=2"),
        JsonNode.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(json.write(response.getBody()))
        .extractingJsonPathArrayValue("$.value[*].integerProperty")
        .containsExactly(10, 20);
  }
}
