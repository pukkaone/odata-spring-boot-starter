package com.github.pukkaone.odata.elasticsearch2.provider;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

/**
 * Converts Elasticsearch indices to CSDL Schemas.
 */
public class IndicesToCsdlSchemasMapper {

  private final ElasticsearchTemplate elasticsearchTemplate;
  private boolean allIndices;
  private Set<String> includeIndexNames = new HashSet<>();

  /**
   * Constructor.
   *
   * @param elasticsearchTemplate
   *     Elasticsearch template
   * @param indexNames
   *     names of Elasticsearch indices to convert
   */
  public IndicesToCsdlSchemasMapper(
      ElasticsearchTemplate elasticsearchTemplate,
      List<String> indexNames) {

    this.elasticsearchTemplate = elasticsearchTemplate;

    if (indexNames.isEmpty() || (indexNames.size() == 1 && indexNames.get(0).equals(""))) {
      allIndices = true;
    } else {
      allIndices = false;
      includeIndexNames.addAll(indexNames);
    }
  }

  private boolean isIncluded(String indexName) {
    return allIndices || includeIndexNames.contains(indexName);
  }

  private Stream<Map.Entry<String, IndexMetaData>> getIndexStream() {
    return elasticsearchTemplate.getClient()
        .admin()
        .cluster()
        .state(Requests.clusterStateRequest())
        .actionGet()
        .getState()
        .getMetaData()
        .getAliasAndIndexLookup()
        .entrySet()
        .stream()
        .filter(entry -> isIncluded(entry.getKey()))
        .map(entry -> new AbstractMap.SimpleImmutableEntry<>(
            entry.getKey(), entry.getValue().getIndices().get(0)));
  }

  /**
   * Converts to schemas.
   *
   * @return schemas
   */
  public Stream<CsdlSchema> toSchemas() {
    return getIndexStream()
        .map(entry -> new IndexToCsdlSchemaMapper()
            .toSchema(entry.getKey(), entry.getValue()));
  }
}
