package com.github.pukkaone.odata.elasticsearch;

import org.elasticsearch.Version;
import org.springframework.boot.test.context.TestConfiguration;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/**
 * Starts an Elasticsearch server in a local Docker container.
 */
@TestConfiguration
public class TestElasticsearchConfiguration {

  private static ElasticsearchContainer container = new ElasticsearchContainer(
      "docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);

  static {
    container.start();
    System.setProperty(
        "spring.elasticsearch.rest.uris", "http://" + container.getHttpHostAddress());
  }
}
