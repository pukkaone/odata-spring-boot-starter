package com.github.pukkaone.odata.elasticsearch2.processor;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;

/**
 * Processes single instance of an Entity Type by accessing an Elasticsearch document.
 */
@RequiredArgsConstructor
public class ElasticsearchEntityProcessor implements EntityProcessor {

  private final EntityRepository entityRepository;
  private OData odata;
  private ServiceMetadata serviceMetadata;

  @Override
  public void init(OData odata, ServiceMetadata serviceMetadata) {
    this.odata = odata;
    this.serviceMetadata = serviceMetadata;
  }

  @Override
  public void readEntity(
      ODataRequest request,
      ODataResponse response,
      UriInfo uriInfo,
      ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {

    // First path segment is Entity Set.
    List<UriResource> resourceParts = uriInfo.getUriResourceParts();
    UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourceParts.get(0);
    EdmEntitySet entitySet = uriResourceEntitySet.getEntitySet();
    List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();

    // Retrieve entity from backend.
    Entity entity = entityRepository.read(entitySet, keyPredicates);

    // Serialize to response format.
    ContextURL contextUrl = ContextURL.with()
        .entitySet(entitySet)
        .suffix(ContextURL.Suffix.ENTITY)
        .build();
    EntitySerializerOptions options = EntitySerializerOptions.with()
        .contextURL(contextUrl)
        .build();
    ODataSerializer serializer = odata.createSerializer(responseFormat);
    SerializerResult serializerResult = serializer.entity(
        serviceMetadata, entitySet.getEntityType(), entity, options);

    // Set response attributes.
    response.setContent(serializerResult.getContent());
    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
  }

  @Override
  public void createEntity(
      ODataRequest request,
      ODataResponse response,
      UriInfo uriInfo,
      ContentType requestFormat,
      ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {

    throw new NotImplementedException("createEntity not implemented");
  }

  @Override
  public void updateEntity(
      ODataRequest request,
      ODataResponse response,
      UriInfo uriInfo,
      ContentType requestFormat,
      ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {

    throw new NotImplementedException("updateEntity not implemented");
  }

  @Override
  public void deleteEntity(
      ODataRequest request,
      ODataResponse response,
      UriInfo uriInfo) throws ODataApplicationException, ODataLibraryException {

    throw new NotImplementedException("deleteEntity not implemented");
  }
}
