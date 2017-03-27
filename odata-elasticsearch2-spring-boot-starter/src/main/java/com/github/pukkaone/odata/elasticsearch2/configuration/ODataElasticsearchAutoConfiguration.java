package com.github.pukkaone.odata.elasticsearch2.configuration;

import com.github.pukkaone.odata.elasticsearch2.provider.ElasticsearchEdmProviderFactory;
import com.github.pukkaone.odata.elasticsearch2.provider.IndicesToCsdlSchemasMapper;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

/**
 * Auto-configures OData provider implemented by Elasticsearch backend.
 */
@ConditionalOnClass(ElasticsearchTemplate.class)
@Configuration
public class ODataElasticsearchAutoConfiguration {

  @Bean
  public IndicesToCsdlSchemasMapper indicesToCsdlSchemasMapper(
      ElasticsearchTemplate elasticsearchTemplate,
      @Value("${odata.elasticsearch.indices:}") List<String> indices) {

    return new IndicesToCsdlSchemasMapper(elasticsearchTemplate, indices);
  }

  @Bean
  public ElasticsearchEdmProviderFactory elasticsearchEdmProviderFactory(
      IndicesToCsdlSchemasMapper indicesToCsdlSchemasMapper) {

    return new ElasticsearchEdmProviderFactory(indicesToCsdlSchemasMapper);
  }
}
