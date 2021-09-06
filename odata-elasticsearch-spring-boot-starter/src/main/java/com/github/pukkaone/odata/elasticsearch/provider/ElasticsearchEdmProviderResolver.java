package com.github.pukkaone.odata.elasticsearch.provider;

import com.github.pukkaone.odata.web.provider.CsdlEdmProviderResolver;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.springframework.stereotype.Component;

/**
 * Provides Entity Data Model from Elasticsearch mappings.
 */
@Component
@RequiredArgsConstructor
public class ElasticsearchEdmProviderResolver implements CsdlEdmProviderResolver {

  private final RestHighLevelClient elasticsearchClient;

  private ConcurrentHashMap<String, CsdlEdmProvider> nameToEdmProviderMap =
      new ConcurrentHashMap<>();

  private Stream<Map.Entry<String, Set<AliasMetadata>>> streamIndicesByAlias(String aliasName) {
    Map<String, Set<AliasMetadata>> indexToAliasesMap;
    try {
      indexToAliasesMap = elasticsearchClient.indices()
          .getAlias(new GetAliasesRequest(aliasName), RequestOptions.DEFAULT)
          .getAliases();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot get alias " + aliasName, e);
    }

    return indexToAliasesMap.entrySet()
        .stream()
        .filter(entry ->
            entry.getValue()
                .stream()
                .anyMatch(aliasMetaData -> aliasMetaData.getAlias().equals(aliasName)));
  }

  private Optional<String> findIndexByAlias(String aliasName) {
    return streamIndicesByAlias(aliasName)
        .findFirst()
        .map(Map.Entry::getKey);
  }

  private Index describeIndex(String aliasOrIndexName) {
    String indexName = findIndexByAlias(aliasOrIndexName).orElse(aliasOrIndexName);
    try {
      MappingMetadata mappingMetadata = elasticsearchClient.indices()
          .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
          .mappings()
          .get(indexName);
      return new Index(indexName, mappingMetadata);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot get mapping, index " + indexName, e);
    }
  }

  private CsdlEdmProvider createEdmProvider(String indexName) {
    Index index = describeIndex(indexName);
    CsdlSchema schema = new IndexToCsdlSchemaMapper().toSchema(index);
    return new ElasticsearchEdmProvider(schema);
  }

  @Override
  public CsdlEdmProvider findByServiceName(String serviceName) {
    return nameToEdmProviderMap.computeIfAbsent(serviceName, this::createEdmProvider);
  }
}
