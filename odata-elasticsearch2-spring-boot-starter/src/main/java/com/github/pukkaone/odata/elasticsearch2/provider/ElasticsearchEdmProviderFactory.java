package com.github.pukkaone.odata.elasticsearch2.provider;

import com.github.pukkaone.odata.web.provider.CsdlEdmProviderFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;

/**
 * Provides Entity Data Model from Elasticsearch mappings.
 */
public class ElasticsearchEdmProviderFactory implements CsdlEdmProviderFactory {

  private Map<String, CsdlEdmProvider> nameToEdmProviderMap = new HashMap<>();

  /**
   * Constructor.
   *
   * @param indicesToCsdlSchemasMapper
   *     schemas mapper
   */
  public ElasticsearchEdmProviderFactory(IndicesToCsdlSchemasMapper indicesToCsdlSchemasMapper) {
    indicesToCsdlSchemasMapper.toSchemas()
        .forEach(schema ->
          nameToEdmProviderMap.put(schema.getNamespace(), new ElasticsearchEdmProvider(schema))
        );
  }

  @Override
  public Map<String, CsdlEdmProvider> getEdmProviders() {
    return nameToEdmProviderMap;
  }
}
