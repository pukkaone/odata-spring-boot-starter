package com.github.pukkaone.odata.elasticsearch.processor;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyPoint;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Component;

/**
 * Reads entity from Elasticsearch.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EntityRepository {

  private final RestHighLevelClient elasticsearchClient;

  private String toIndexName(EdmEntitySet entitySet) {
    return entitySet.getName();
  }

  private static ComplexValue toComplexValue(
      EdmComplexType complexType, Map<String, Object> source) {

    if (source == null) {
      return null;
    }

    ComplexValue complexValue = new ComplexValue();
    addProperties(complexType, source, complexValue.getValue());
    return complexValue;
  }

  @SuppressWarnings("unchecked")
  private static List<ComplexValue> toCollection(EdmComplexType complexType, Object source) {
    if (source == null) {
      return null;
    }

    if (source instanceof List) {
      return ((List<Map<String, Object>>) source).stream()
          .map(element -> toComplexValue(complexType, element))
          .collect(Collectors.toList());
    }

    return Collections.singletonList(toComplexValue(complexType, (Map<String, Object>) source));
  }

  private static byte[] toByteArray(String sourceValue) {
    try {
      return EdmBinary.getInstance().valueOfString(
          sourceValue,
          true,
          null,
          null,
          null,
          null,
          byte[].class);
    } catch (EdmPrimitiveTypeException e) {
      throw new IllegalStateException("Cannot convert to EdmBinary", e);
    }
  }

  private static Date toDate(String sourceValue) {
    try {
      return EdmDateTimeOffset.getInstance().valueOfString(
          sourceValue,
          true,
          null,
          null,
          null,
          null,
          Date.class);
    } catch (EdmPrimitiveTypeException dateTimeOffsetException) {
      try {
        return EdmDate.getInstance().valueOfString(
            sourceValue,
            true,
            null,
            null,
            null,
            null,
            Date.class);
      } catch (EdmPrimitiveTypeException e) {
        throw new IllegalStateException("Cannot convert to EdmDate: " + sourceValue, e);
      }
    }
  }

  private static Point toPoint(Map<String, Double> sourcePoint) {
    if (sourcePoint == null) {
      return null;
    }

    Point point = new Point(Geospatial.Dimension.GEOGRAPHY, null);
    point.setY(sourcePoint.get("lat"));
    point.setX(sourcePoint.get("lon"));
    return point;
  }

  @SuppressWarnings("unchecked")
  private static Property toProperty(
      String propertyName, Object sourceValue, EdmProperty description) {

    ValueType valueType;
    Object value;
    EdmTypeKind kind = description.getType().getKind();
    switch (kind) {
      case COMPLEX:
        if (description.isCollection()) {
          valueType = ValueType.COLLECTION_COMPLEX;
          value = toCollection(
              (EdmComplexType) description.getType(), sourceValue);
        } else if (sourceValue instanceof List) {
          valueType = ValueType.COMPLEX;
          List<Map<String, Object>> list = (List<Map<String, Object>>) sourceValue;
          if (list.isEmpty()) {
            value = null;
          } else {
            log.warn("Discarded all elements from list {} except first", propertyName);
            value = toComplexValue(
                (EdmComplexType) description.getType(), list.get(0));
          }
        } else {
          valueType = ValueType.COMPLEX;
          value = toComplexValue(
              (EdmComplexType) description.getType(), (Map<String, Object>) sourceValue);
        }
        break;
      case PRIMITIVE:
        valueType = ValueType.PRIMITIVE;
        if (description.getType() instanceof EdmBinary) {
          value = toByteArray((String) sourceValue);
        } else if (description.getType() instanceof EdmDateTimeOffset) {
          value = toDate((String) sourceValue);
        } else if (description.getType() instanceof EdmGeographyPoint) {
          value = toPoint((Map<String, Double>) sourceValue);
        } else {
          value = sourceValue;
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert from EdmTypeKind " + kind);
    }

    return new Property(null, propertyName, valueType, value);
  }

  private static void addProperties(
      EdmStructuredType structuredType, Map<String, Object> source, List<Property> properties) {

    for (String propertyName : structuredType.getPropertyNames()) {
      Object sourceValue = source.get(propertyName);
      EdmElement description = structuredType.getProperty(propertyName);
      if (description instanceof EdmProperty) {
        Property property = toProperty(propertyName, sourceValue, (EdmProperty) description);
        properties.add(property);
      } else {
        log.debug("Skipping property name {}, description {}", propertyName, description);
      }
    }
  }

  private static Entity toEntity(
      EdmEntitySet entitySet, String entityId, Map<String, Object> source) {

    Entity entity = new Entity();
    entity.setId(URI.create(entitySet.getName() + "('" + entityId + "')"));

    Property property = new Property(null, "_id", ValueType.PRIMITIVE, entityId);
    entity.addProperty(property);

    addProperties(entitySet.getEntityType(), source, entity.getProperties());
    return entity;
  }

  /**
   * Reads single instance of an Entity Type.
   *
   * @param entitySet
   *     Entity Set to read from
   * @param keyPredicates
   *     contains entity primary key
   * @return entity
   */
  public Entity read(EdmEntitySet entitySet, List<UriParameter> keyPredicates) {
    String indexName = toIndexName(entitySet);
    String entityId = LiteralUtils.unquote(keyPredicates.get(0).getText());

    GetRequest getRequest = Requests.getRequest(indexName)
        .id(entityId);
    Map<String, Object> source = null;
    try {
      source = elasticsearchClient.get(getRequest, RequestOptions.DEFAULT)
          .getSource();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Cannot get, index %s, id %s", indexName, entityId),
          e);
    }

    return toEntity(entitySet, entityId, source);
  }

  private static QueryBuilder toQueryBuilder(FilterOption filterOption)
      throws ODataApplicationException {

    if (filterOption == null) {
      return QueryBuilders.matchAllQuery();
    }

    try {
      return (QueryBuilder) filterOption.getExpression()
          .accept(new ElasticsearchExpressionVisitor());
    } catch (ExpressionVisitException e) {
      throw new ODataApplicationException(
          "accept failed", HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), null, e);
    }
  }

  private static void configurePaging(UriInfo uriInfo, SearchSourceBuilder searchSource) {
    int skip = (uriInfo.getSkipOption() == null) ? 0 : uriInfo.getSkipOption().getValue();
    searchSource.from(skip);

    int top = (uriInfo.getTopOption() == null) ? 20 : uriInfo.getTopOption().getValue();
    searchSource.size(top);
  }

  private static void configureSorting(UriInfo uriInfo, SearchSourceBuilder searchSource) {
    if (uriInfo.getOrderByOption() == null) {
      return;
    }

    for (OrderByItem orderByItem : uriInfo.getOrderByOption().getOrders()) {
      searchSource.sort(
          MemberMapper.toFieldName((Member) orderByItem.getExpression()),
          orderByItem.isDescending() ? SortOrder.DESC : SortOrder.ASC);
    }
  }

  /**
   * Reads multiple instances of an Entity Type.
   *
   * @param entitySet
   *     Entity Set to read from
   * @param uriInfo
   *     contains filter, order by and paging arguments
   * @return entities
   * @throws ODataApplicationException
   *     if error occurred handling filter option
   */
  public EntityCollection list(EdmEntitySet entitySet, UriInfo uriInfo)
      throws ODataApplicationException {

    String indexName = toIndexName(entitySet);

    SearchSourceBuilder searchSource = new SearchSourceBuilder()
        .query(toQueryBuilder(uriInfo.getFilterOption()));
    configurePaging(uriInfo, searchSource);
    configureSorting(uriInfo, searchSource);

    SearchRequest searchRequest = Requests.searchRequest(indexName)
        .source(searchSource);

    SearchResponse response;
    try {
      response = elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot search, index " + indexName, e);
    }

    EntityCollection entityCollection = new EntityCollection();
    List<Entity> entities = entityCollection.getEntities();
    response.getHits().forEach(hit -> {
      Entity entity = toEntity(entitySet, hit.getId(), hit.getSourceAsMap());
      entities.add(entity);
    });

    return entityCollection;
  }
}
