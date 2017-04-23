package com.github.pukkaone.odata.elasticsearch2.configuration;

import com.github.pukkaone.odata.elasticsearch2.processor.ElasticsearchEntityCollectionProcessor;
import com.github.pukkaone.odata.elasticsearch2.processor.ElasticsearchEntityProcessor;
import com.github.pukkaone.odata.elasticsearch2.processor.EntityRepository;
import com.github.pukkaone.odata.elasticsearch2.provider.ElasticsearchEdmProviderResolver;
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
  public ElasticsearchEdmProviderResolver elasticsearchEdmProviderFactory(
      ElasticsearchTemplate elasticsearchTemplate) {

    return new ElasticsearchEdmProviderResolver(elasticsearchTemplate);
  }

  @Bean
  public EntityRepository entityRepository(ElasticsearchTemplate elasticsearchTemplate) {
    return new EntityRepository(elasticsearchTemplate);
  }

  @Bean
  public ElasticsearchEntityCollectionProcessor elasticsearchEntityCollectionProcessor(
      EntityRepository entityRepository) {

    return new ElasticsearchEntityCollectionProcessor(entityRepository);
  }

  @Bean
  public ElasticsearchEntityProcessor elasticsearchEntityProcessor(
      EntityRepository entityRepository) {

    return new ElasticsearchEntityProcessor(entityRepository);
  }
}
