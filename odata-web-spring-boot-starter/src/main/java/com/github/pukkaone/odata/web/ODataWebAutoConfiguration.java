package com.github.pukkaone.odata.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pukkaone.odata.web.processor.DebugErrorProcessor;
import com.github.pukkaone.odata.web.provider.CsdlEdmProviderFactory;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import org.apache.olingo.server.api.processor.Processor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

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

  @Component
  public static class ODataInitializer implements ServletContextInitializer {

    @Value("${odata.web.service-parent-path:/odata}")
    private String serviceParentPath;

    @Autowired
    private CsdlEdmProviderFactory edmProviderFactory;

    @Autowired
    private List<Processor> processors;

    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
      final String parentPath = (serviceParentPath.endsWith("/"))
          ? serviceParentPath : serviceParentPath + '/';

      edmProviderFactory.getEdmProviders()
          .forEach((serviceName, edmProvider) -> {
            String servletName = "odata-" + serviceName;
            ODataServlet servlet = new ODataServlet(edmProvider, processors);
            servletContext.addServlet(servletName, servlet)
                .addMapping(parentPath + serviceName + "/*");
          });
    }
  }
}
