package com.github.pukkaone.odata.elasticsearch2;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Embedded Elasticsearch server.
 */
public class EmbeddedElasticsearchServer {

  private static final String HOME_DIRECTORY = "target/elasticsearch-test";

  private final Node node;

  public EmbeddedElasticsearchServer() {
    Settings settings = Settings.settingsBuilder()
        .put("http.enabled", "true")
        .put("path.home", HOME_DIRECTORY)
        .build();

    node = NodeBuilder.nodeBuilder()
        .local(true)
        .settings(settings)
        .node();
  }

  public Client getClient() {
    return node.client();
  }

  public void stop() {
    node.close();
  }
}
