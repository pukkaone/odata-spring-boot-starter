package com.github.pukkaone.odata.web;

import com.github.pukkaone.odata.web.provider.CsdlEdmProviderResolver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.Processor;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

/**
 * Implements OData service root endpoint.
 */
public class ODataServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private List<CsdlEdmProviderResolver> edmProviderResolvers;
  private List<Processor> processors;

  /**
   * Constructor.
   *
   * @param edmProviderResolvers
   *     Entity Data Model provider resolvers
   * @param processors
   *     processors
   */
  public ODataServlet(
      List<CsdlEdmProviderResolver> edmProviderResolvers, List<Processor> processors) {

    this.edmProviderResolvers = new ArrayList<>(edmProviderResolvers);
    this.edmProviderResolvers.sort(AnnotationAwareOrderComparator.INSTANCE);

    this.processors = processors;
  }

  private String extractServiceName(HttpServletRequest request) {
    String[] pathSegments = request.getPathInfo().split("/");
    return (pathSegments.length >= 2) ? pathSegments[1] : null;
  }

  private CsdlEdmProvider toEdmProvider(String serviceName) {
    for (CsdlEdmProviderResolver resolver : edmProviderResolvers) {
      CsdlEdmProvider edmProvider = resolver.findByServiceName(serviceName);
      if (edmProvider != null) {
        return edmProvider;
      }
    }

    return null;
  }

  @Override
  protected void service(
      HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {

    String serviceName = extractServiceName(request);
    if (serviceName == null) {
      response.sendError(
          HttpServletResponse.SC_BAD_REQUEST, "Service name required in URI path");
      return;
    }

    CsdlEdmProvider edmProvider = toEdmProvider(serviceName);
    if (edmProvider == null) {
      response.sendError(
          HttpServletResponse.SC_NOT_FOUND, "Unknown service name " + serviceName);
      return;
    }

    OData odata = OData.newInstance();
    ServiceMetadata metadata = odata.createServiceMetadata(edmProvider, Collections.emptyList());
    ODataHttpHandler handler = odata.createHandler(metadata);
    for (Processor processor : processors) {
      handler.register(processor);
    }

    HttpServletRequestWrapper requestWrapper = new HttpServletRequestWrapper(request) {
      @Override
      public String getServletPath() {
        return request.getServletPath() + '/' + serviceName;
      }
    };
    handler.process(requestWrapper, response);
  }
}
