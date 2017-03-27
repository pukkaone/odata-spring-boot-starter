package com.github.pukkaone.odata.elasticsearch2.provider;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

/**
 * Converts Elasticsearch index to CSDL Schema.
 */
@Slf4j
public class IndexToCsdlSchemaMapper {

  private static final String ID_PROPERTY_NAME = "_id";

  private CsdlSchema schema;
  private Map<FullQualifiedName, CsdlEntityType> nameToEntityTypeMap = new LinkedHashMap<>();
  private Map<FullQualifiedName, CsdlComplexType> nameToComplexTypeMap = new LinkedHashMap<>();

  private static final Map<String, EdmPrimitiveTypeKind> TO_PRIMITIVE_TYPE_MAP =
      ImmutableMap.<String, EdmPrimitiveTypeKind>builder()
          .put(BinaryFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Binary)
          .put(BooleanFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Boolean)
          .put(ByteFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Byte)
          .put(DateFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.DateTimeOffset)
          .put(DoubleFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Double)
          .put(FloatFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Single)
          .put(GeoPointFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.GeographyPoint)
          .put(GeoShapeFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.GeographyPolygon)
          .put(IntegerFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Int32)
          .put(LongFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Int64)
          .put(ShortFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Int16)
          .put(StringFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.String)
          .build();

  private FullQualifiedName toPrimitiveType(String elasticsearchType) {
    EdmPrimitiveTypeKind type = TO_PRIMITIVE_TYPE_MAP.get(elasticsearchType);
    if (type == null) {
      log.warn("Elasticsearch type {} translated to OData String type", elasticsearchType);
      type = EdmPrimitiveTypeKind.String;
    }

    return type.getFullQualifiedName();
  }

  private FullQualifiedName toComplexType(
      String propertyName, Map<String, Object> nameToAttributeMap) {

    List<CsdlProperty> properties = toProperties(nameToAttributeMap);

    CsdlComplexType complexType = new CsdlComplexType()
        .setName(propertyName)
        .setProperties(properties);

    FullQualifiedName complexTypeFqn =
        new FullQualifiedName(schema.getNamespace(), complexType.getName());
    nameToComplexTypeMap.put(complexTypeFqn, complexType);
    return complexTypeFqn;
  }

  private FullQualifiedName toPropertyType(
      String propertyName, Map<String, Object> nameToAttributeMap) {

    String elasticsearchType = (String) nameToAttributeMap.getOrDefault(
        "type", "object");
    if ("nested".equals(elasticsearchType) || "object".equals(elasticsearchType)) {
      return toComplexType(propertyName, nameToAttributeMap);
    }

    return toPrimitiveType(elasticsearchType);
  }

  @SuppressWarnings("unchecked")
  private CsdlProperty toProperty(String propertyName, Object propertySchema) {
    Map<String, Object> nameToAttributeMap = (Map<String, Object>) propertySchema;

    String elasticsearchType = (String) nameToAttributeMap.get("type");
    boolean isCollection = "nested".equals(elasticsearchType);

    FullQualifiedName type = toPropertyType(propertyName, nameToAttributeMap);
    return new CsdlProperty()
        .setCollection(isCollection)
        .setName(propertyName)
        .setType(type);
  }

  @SuppressWarnings("unchecked")
  private List<CsdlProperty> toProperties(Map<String, Object> sourceSchema) {
    Map<String, Object> sourceProperties = (Map<String, Object>) sourceSchema.get("properties");

    List<CsdlProperty> properties = new ArrayList<>();
    sourceProperties.forEach((propertyName, propertySchema) ->
      properties.add(toProperty(propertyName, propertySchema))
    );

    return properties;
  }

  private List<CsdlProperty> toProperties(MappingMetaData mappingMetaData) {
    try {
      return toProperties(mappingMetaData.getSourceAsMap());
    } catch (IOException e) {
      throw new IllegalStateException("getSourceAsMap failed", e);
    }
  }

  private CsdlEntityType toEntityType(MappingMetaData mappingMetaData) {
    List<CsdlProperty> properties = toProperties(mappingMetaData);

    properties.add(
        new CsdlProperty()
            .setName(ID_PROPERTY_NAME)
            .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
            .setNullable(false));

    return new CsdlEntityType()
        .setName(mappingMetaData.type())
        .setKey(Collections.singletonList(new CsdlPropertyRef().setName(ID_PROPERTY_NAME)))
        .setProperties(properties);
  }

  private CsdlEntitySet toEntitySet(MappingMetaData mappingMetaData) {
    CsdlEntityType entityType = toEntityType(mappingMetaData);

    FullQualifiedName entityTypeFqn = new FullQualifiedName(
        schema.getNamespace(), entityType.getName());
    nameToEntityTypeMap.put(entityTypeFqn, entityType);

    return new CsdlEntitySet()
        .setName(entityType.getName())
        .setType(entityTypeFqn);
  }

  private List<CsdlEntitySet> toEntitySets(IndexMetaData indexMetaData) {
    return Streams.stream(indexMetaData.getMappings())
        .map(cursor -> toEntitySet(cursor.value))
        .collect(Collectors.toList());
  }

  private CsdlEntityContainer toEntityContainer(String indexName, IndexMetaData indexMetaData) {
    return new CsdlEntityContainer()
        .setName(indexName)
        .setEntitySets(toEntitySets(indexMetaData));
  }

  /**
   * Converts Elasticsearch index to CSDL Schema.
   *
   * @param indexName
   *     Elasticsearch index name
   * @param indexMetaData
   *     Elasticsearch index metadata
   * @return schema
   */
  public CsdlSchema toSchema(String indexName, IndexMetaData indexMetaData) {

    schema = new CsdlSchema()
        .setNamespace(indexName);
    return schema.setEntityContainer(toEntityContainer(indexName, indexMetaData))
        .setEntityTypes(new ArrayList<>(nameToEntityTypeMap.values()))
        .setComplexTypes(new ArrayList<>(nameToComplexTypeMap.values()));
  }
}
