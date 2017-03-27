package com.github.pukkaone.odata.elasticsearch2.processor;

import java.util.stream.Collectors;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;

/**
 * Converts member data.
 */
public final class MemberMapper {

  // Private constructor disallows creating instances of this class.
  private MemberMapper() {
  }

  /**
   * Converts to Elasticsearch field name.
   *
   * @param member
   *     to convert from
   * @return Elasticsearch field name
   */
  public static String toFieldName(Member member) {
    return member.getResourcePath()
        .getUriResourceParts()
        .stream()
        .map(uriResource -> ((UriResourceProperty) uriResource).getProperty().getName())
        .collect(Collectors.joining("."));
  }
}
