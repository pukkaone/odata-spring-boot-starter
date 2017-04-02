package com.github.pukkaone.odata.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pukkaone.odata.web.processor.DebugErrorProcessor;
import com.github.pukkaone.odata.web.provider.CsdlEdmProviderResolver;
import java.util.List;
import org.apache.olingo.server.api.processor.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configures OData servlet.
 */
@ConditionalOnWebApplication
@Configuration
public class ODataWebAutoConfiguration {

  @Bean
  public DebugErrorProcessor debugErrorProcessor(ObjectMapper objectMapper) {
    return new DebugErrorProcessor(objectMapper);
  }

  @Bean
  public ServletRegistrationBean odataServletRegistration(
      @Value("${odata.web.service-parent-path:/odata}") String serviceParentPath,
      List<CsdlEdmProviderResolver> edmProviderResolvers,
      List<Processor> processors) {

    String parentPath = (serviceParentPath.endsWith("/"))
        ? serviceParentPath : serviceParentPath + '/';

    ODataServlet servlet = new ODataServlet(edmProviderResolvers, processors);
    return new ServletRegistrationBean(servlet, parentPath + '*');
  }
}
