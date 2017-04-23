package com.github.pukkaone.odata.elasticsearch2.provider;

import com.github.pukkaone.odata.web.provider.CsdlEdmProviderResolver;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

/**
 * Provides Entity Data Model from Elasticsearch mappings.
 */
@RequiredArgsConstructor
public class ElasticsearchEdmProviderResolver implements CsdlEdmProviderResolver {

  private final ElasticsearchTemplate elasticsearchTemplate;

  private Map<String, CsdlEdmProvider> nameToEdmProviderMap = new HashMap<>();

  private IndexMetaData getIndexMetaData(String indexName) {
    AliasOrIndex aliasOrIndex = elasticsearchTemplate.getClient()
        .admin()
        .cluster()
        .state(Requests.clusterStateRequest())
        .actionGet()
        .getState()
        .getMetaData()
        .getAliasAndIndexLookup()
        .get(indexName);
    if (aliasOrIndex == null) {
      return null;
    }

    return aliasOrIndex.getIndices().get(0);
  }

  private CsdlEdmProvider createEdmProvider(String indexName) {
    IndexMetaData indexMetaData = getIndexMetaData(indexName);
    if (indexMetaData == null) {
      return null;
    }

    CsdlSchema schema = new IndexToCsdlSchemaMapper().toSchema(indexName, indexMetaData);
    return new ElasticsearchEdmProvider(schema);
  }

  @Override
  public CsdlEdmProvider findByServiceName(String serviceName) {
    return nameToEdmProviderMap.computeIfAbsent(serviceName, this::createEdmProvider);
  }
}
