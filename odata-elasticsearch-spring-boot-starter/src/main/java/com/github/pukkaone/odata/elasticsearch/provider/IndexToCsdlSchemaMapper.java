package com.github.pukkaone.odata.elasticsearch.provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;

/**
 * Converts Elasticsearch index to CSDL Schema.
 */
@Slf4j
public class IndexToCsdlSchemaMapper {

  private static final String ID_PROPERTY_NAME = "_id";
  private static final String NESTED = "nested";
  private static final String OBJECT = "object";
  private static final Map<String, EdmPrimitiveTypeKind> TO_PRIMITIVE_TYPE_MAP = new HashMap<>();

  static {
    TO_PRIMITIVE_TYPE_MAP.put(BinaryFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Binary);
    TO_PRIMITIVE_TYPE_MAP.put(BooleanFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.Boolean);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.BYTE.typeName(), EdmPrimitiveTypeKind.Byte);
    TO_PRIMITIVE_TYPE_MAP.put(DateFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.DateTimeOffset);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.DOUBLE.typeName(), EdmPrimitiveTypeKind.Double);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.FLOAT.typeName(), EdmPrimitiveTypeKind.Single);
    TO_PRIMITIVE_TYPE_MAP.put(
        GeoPointFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.GeographyPoint);
    TO_PRIMITIVE_TYPE_MAP.put(
        GeoShapeFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.GeographyPolygon);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.INTEGER.typeName(), EdmPrimitiveTypeKind.Int32);
    TO_PRIMITIVE_TYPE_MAP.put(KeywordFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.String);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.LONG.typeName(), EdmPrimitiveTypeKind.Int64);
    TO_PRIMITIVE_TYPE_MAP.put(
        NumberFieldMapper.NumberType.SHORT.typeName(), EdmPrimitiveTypeKind.Int16);
    TO_PRIMITIVE_TYPE_MAP.put(TextFieldMapper.CONTENT_TYPE, EdmPrimitiveTypeKind.String);
  }

  private CsdlSchema schema;
  private Map<FullQualifiedName, CsdlEntityType> nameToEntityTypeMap = new LinkedHashMap<>();
  private Map<FullQualifiedName, CsdlComplexType> nameToComplexTypeMap = new LinkedHashMap<>();

  private FullQualifiedName toPrimitiveType(String elasticsearchType) {
    EdmPrimitiveTypeKind type = TO_PRIMITIVE_TYPE_MAP.get(elasticsearchType);
    return (type == null) ? null : type.getFullQualifiedName();
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
        "type", OBJECT);
    if (NESTED.equals(elasticsearchType) || OBJECT.equals(elasticsearchType)) {
      return toComplexType(propertyName, nameToAttributeMap);
    }

    return toPrimitiveType(elasticsearchType);
  }

  @SuppressWarnings("unchecked")
  private Optional<CsdlProperty> toProperty(String propertyName, Object propertySchema) {
    Map<String, Object> nameToAttributeMap = (Map<String, Object>) propertySchema;

    String elasticsearchType = (String) nameToAttributeMap.get("type");
    boolean isCollection = NESTED.equals(elasticsearchType);

    FullQualifiedName type = toPropertyType(propertyName, nameToAttributeMap);
    if (type == null) {
      log.warn(
          "Ignoring Elasticsearch field {} having unsupported type {}",
          propertyName,
          elasticsearchType);
      return Optional.empty();
    }

    return Optional.of(new CsdlProperty()
        .setCollection(isCollection)
        .setName(propertyName)
        .setType(type));
  }

  @SuppressWarnings("unchecked")
  private List<CsdlProperty> toProperties(Map<String, Object> sourceSchema) {
    List<CsdlProperty> properties = new ArrayList<>();

    Map<String, Object> sourceProperties = (Map<String, Object>) sourceSchema.get("properties");
    if (sourceProperties != null) {
      sourceProperties = new TreeMap<>(sourceProperties);
      sourceProperties.forEach((propertyName, propertySchema) ->
          toProperty(propertyName, propertySchema)
              .ifPresent(properties::add)
      );
    }

    return properties;
  }

  private List<CsdlProperty> toProperties(Index index) {
    return toProperties(index.getMapping().sourceAsMap());
  }

  private CsdlEntityType toEntityType(Index index) {
    List<CsdlProperty> properties = toProperties(index);

    properties.add(
        new CsdlProperty()
            .setName(ID_PROPERTY_NAME)
            .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
            .setNullable(false));

    return new CsdlEntityType()
        .setName(index.getName())
        .setKey(Collections.singletonList(new CsdlPropertyRef().setName(ID_PROPERTY_NAME)))
        .setProperties(properties);
  }

  private CsdlEntitySet toEntitySet(Index index) {
    CsdlEntityType entityType = toEntityType(index);

    FullQualifiedName entityTypeFqn = new FullQualifiedName(
        schema.getNamespace(), entityType.getName());
    nameToEntityTypeMap.put(entityTypeFqn, entityType);

    return new CsdlEntitySet()
        .setName(entityType.getName())
        .setType(entityTypeFqn);
  }

  private List<CsdlEntitySet> toEntitySets(Index index) {
    return Collections.singletonList(toEntitySet(index));
  }

  private CsdlEntityContainer toEntityContainer(Index index) {
    return new CsdlEntityContainer()
        .setName(index.getName() + "Container")
        .setEntitySets(toEntitySets(index));
  }

  /**
   * Converts Elasticsearch index to CSDL Schema.
   *
   * @param index
   *     index description
   * @return schema
   */
  public CsdlSchema toSchema(Index index) {
    schema = new CsdlSchema()
        .setNamespace(index.getName());
    return schema.setEntityContainer(toEntityContainer(index))
        .setEntityTypes(new ArrayList<>(nameToEntityTypeMap.values()))
        .setComplexTypes(new ArrayList<>(nameToComplexTypeMap.values()));
  }
}
