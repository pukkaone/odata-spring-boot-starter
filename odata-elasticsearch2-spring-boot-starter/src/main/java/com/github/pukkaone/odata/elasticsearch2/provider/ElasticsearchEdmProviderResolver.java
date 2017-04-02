package com.github.pukkaone.odata.elasticsearch2.provider;

import com.github.pukkaone.odata.web.provider.CsdlEdmProviderResolver;
import java.util.HashMap;
import java.util.Map;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;

/**
 * Provides Entity Data Model from Elasticsearch mappings.
 */
public class ElasticsearchEdmProviderResolver implements CsdlEdmProviderResolver {

  private Map<String, CsdlEdmProvider> nameToEdmProviderMap = new HashMap<>();

  /**
   * Constructor.
   *
   * @param indicesToCsdlSchemasMapper
   *     schemas mapper
   */
  public ElasticsearchEdmProviderResolver(IndicesToCsdlSchemasMapper indicesToCsdlSchemasMapper) {
    indicesToCsdlSchemasMapper.toSchemas()
        .forEach(schema ->
          nameToEdmProviderMap.put(schema.getNamespace(), new ElasticsearchEdmProvider(schema))
        );
  }

  @Override
  public CsdlEdmProvider findByServiceName(String serviceName) {
    return nameToEdmProviderMap.get(serviceName);
  }
}
