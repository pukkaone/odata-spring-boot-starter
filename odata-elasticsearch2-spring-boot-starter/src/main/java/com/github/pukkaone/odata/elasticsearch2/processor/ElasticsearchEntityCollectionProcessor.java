package com.github.pukkaone.odata.elasticsearch2.processor;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.EntityCollection;
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
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;

/**
 * Reads multiple instances of an Entity Type by accessing an Elasticsearch index.
 */
@RequiredArgsConstructor
public class ElasticsearchEntityCollectionProcessor implements EntityCollectionProcessor {

  private final EntityRepository entityRepository;
  private OData odata;
  private ServiceMetadata serviceMetadata;

  @Override
  public void init(OData odata, ServiceMetadata serviceMetadata) {
    this.odata = odata;
    this.serviceMetadata = serviceMetadata;
  }

  @Override
  public void readEntityCollection(
      ODataRequest request,
      ODataResponse response,
      UriInfo uriInfo,
      ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {

    // First path segment is Entity Set.
    List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    EdmEntitySet entitySet = uriResourceEntitySet.getEntitySet();

    // Retrieve entities from backend.
    EntityCollection entityCollection = entityRepository.list(entitySet, uriInfo);

    // Serialize to response format.
    ContextURL contextUrl = ContextURL.with()
        .entitySet(entitySet)
        .build();
    EntityCollectionSerializerOptions options = EntityCollectionSerializerOptions.with()
        .id(request.getRawBaseUri() + "/" + entitySet.getName())
        .contextURL(contextUrl)
        .build();
    ODataSerializer serializer = odata.createSerializer(responseFormat);
    SerializerResult serializerResult = serializer.entityCollection(
        serviceMetadata, entitySet.getEntityType(), entityCollection, options);

    // Set response attributes.
    response.setContent(serializerResult.getContent());
    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
  }
}
