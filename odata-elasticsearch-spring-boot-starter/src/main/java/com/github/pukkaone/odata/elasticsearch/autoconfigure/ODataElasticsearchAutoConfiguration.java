package com.github.pukkaone.odata.elasticsearch.autoconfigure;

import com.github.pukkaone.odata.elasticsearch.processor.ElasticsearchEntityCollectionProcessor;
import com.github.pukkaone.odata.elasticsearch.processor.ElasticsearchEntityProcessor;
import com.github.pukkaone.odata.elasticsearch.processor.EntityRepository;
import com.github.pukkaone.odata.elasticsearch.provider.ElasticsearchEdmProviderResolver;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Auto-configures OData provider implemented by Elasticsearch backend.
 */
@AutoConfigureAfter(ElasticsearchRestClientAutoConfiguration.class)
@ConditionalOnClass(RestHighLevelClient.class)
@Configuration
@Import({
    ElasticsearchEdmProviderResolver.class,
    ElasticsearchEntityCollectionProcessor.class,
    ElasticsearchEntityProcessor.class,
    EntityRepository.class })
public class ODataElasticsearchAutoConfiguration {
}
