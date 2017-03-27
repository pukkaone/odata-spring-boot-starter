package com.github.pukkaone.odata.web;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.Processor;

/**
 * Implements OData service root endpoint.
 */
@RequiredArgsConstructor
public class ODataServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final CsdlEdmProvider edmProvider;
  private final List<Processor> processors;

  @Override
  protected void service(
      HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {

    OData odata = OData.newInstance();
    ServiceMetadata metadata = odata.createServiceMetadata(edmProvider, Collections.emptyList());
    ODataHttpHandler handler = odata.createHandler(metadata);
    for (Processor processor : processors) {
      handler.register(processor);
    }

    handler.process(request, response);
  }
}
