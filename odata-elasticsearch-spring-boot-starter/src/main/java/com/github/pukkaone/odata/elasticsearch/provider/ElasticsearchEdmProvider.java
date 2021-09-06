package com.github.pukkaone.odata.elasticsearch.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.server.core.SchemaBasedEdmProvider;

/**
 * Provides entity data model from Elasticsearch mappings.
 */
public class ElasticsearchEdmProvider extends SchemaBasedEdmProvider {

  /**
   * Constructor.
   *
   * @param schema
   *     schema
   */
  public ElasticsearchEdmProvider(CsdlSchema schema) {
    addSchema(schema);
  }
}
