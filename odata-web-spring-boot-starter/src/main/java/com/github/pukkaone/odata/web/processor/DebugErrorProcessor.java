package com.github.pukkaone.odata.web.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.ErrorProcessor;

/**
 * Logs error while processing error.
 */
@RequiredArgsConstructor
@Slf4j
public class DebugErrorProcessor implements ErrorProcessor {

  private final ObjectMapper objectMapper;

  @Override
  public void init(OData odata, ServiceMetadata serviceMetadata) {
  }

  @Override
  public void processError(
      ODataRequest request,
      ODataResponse response,
      ODataServerError serverError,
      ContentType requestedContentType) {

    log.error("processError, requestUri " + request.getRawRequestUri(), serverError.getException());

    ErrorDocument errorDocument = new ErrorDocument(serverError);
    String content;
    try {
      content = objectMapper.writeValueAsString(errorDocument);
    } catch (JsonProcessingException e) {
      content =
          "{" +
            "\"error\":{" +
              "\"code\":null," +
              "\"message\":\"Serialize to JSON failed during error processing\"" +
            "}" +
          "}";
    }

    response.setContent(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    response.setStatusCode(serverError.getStatusCode());
    response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.APPLICATION_JSON.toContentTypeString());
  }
}
