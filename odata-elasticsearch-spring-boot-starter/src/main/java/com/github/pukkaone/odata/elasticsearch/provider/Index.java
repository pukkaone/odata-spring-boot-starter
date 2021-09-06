package com.github.pukkaone.odata.elasticsearch.provider;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.elasticsearch.cluster.metadata.MappingMetadata;

/**
 * Elasticsearch index description.
 */
@AllArgsConstructor
@Data
public class Index {

  private String name;
  private MappingMetadata mapping;
}
