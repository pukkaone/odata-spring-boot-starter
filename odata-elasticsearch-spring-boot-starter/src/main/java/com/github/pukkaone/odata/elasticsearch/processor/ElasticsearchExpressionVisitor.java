package com.github.pukkaone.odata.elasticsearch.processor;

import java.util.List;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Converts filter expression to Elasticsearch query.
 */
public class ElasticsearchExpressionVisitor implements ExpressionVisitor<Object> {

  private static QueryBuilder and(QueryBuilder left, QueryBuilder right) {
    return QueryBuilders.boolQuery()
        .must(left)
        .must(right);
  }

  private static QueryBuilder eq(String fieldName, Object value) {
    if (value == null) {
      // Field must not exist.
      return QueryBuilders.boolQuery()
          .mustNot(QueryBuilders.existsQuery(fieldName));
    }

    return QueryBuilders.termQuery(fieldName, value);
  }

  private static QueryBuilder ge(String fieldName, Object value) {
    return QueryBuilders.rangeQuery(fieldName)
        .gte(value);
  }

  private static QueryBuilder gt(String fieldName, Object value) {
    return QueryBuilders.rangeQuery(fieldName)
        .gt(value);
  }

  private static QueryBuilder le(String fieldName, Object value) {
    return QueryBuilders.rangeQuery(fieldName)
        .lte(value);
  }

  private static QueryBuilder lt(String fieldName, Object value) {
    return QueryBuilders.rangeQuery(fieldName)
        .lt(value);
  }

  private static QueryBuilder ne(String fieldName, Object value) {
    if (value == null) {
      // Field must exist.
      return QueryBuilders.existsQuery(fieldName);
    }

    return QueryBuilders.boolQuery()
        .mustNot(QueryBuilders.termQuery(fieldName, value));
  }

  private static QueryBuilder not(QueryBuilder query) {
    return QueryBuilders.boolQuery()
        .mustNot(query);
  }

  private static QueryBuilder or(QueryBuilder left, QueryBuilder right) {
    return QueryBuilders.boolQuery()
        .should(left)
        .should(right)
        .minimumShouldMatch(1);
  }

  @Override
  public Object visitBinaryOperator(
      BinaryOperatorKind operator,
      Object left,
      Object right) throws ExpressionVisitException, ODataApplicationException {

    switch (operator) {
      case AND:
        return and((QueryBuilder) left, (QueryBuilder) right);
      case EQ:
        return eq((String) left, right);
      case GE:
        return ge((String) left, right);
      case GT:
        return gt((String) left, right);
      case LE:
        return le((String) left, right);
      case LT:
        return lt((String) left, right);
      case NE:
        return ne((String) left, right);
      case OR:
        return or((QueryBuilder) left, (QueryBuilder) right);
      default:
        throw new NotImplementedException("Binary operator " + operator + " not implemented");
    }
  }

  @Override
  public Object visitUnaryOperator(
      UnaryOperatorKind operator,
      Object operand) throws ExpressionVisitException, ODataApplicationException {

    switch (operator) {
      case NOT:
        return not((QueryBuilder) operand);
      default:
        throw new NotImplementedException("Unary operator " + operator + " not implemented");
    }
  }

  private static QueryBuilder contains(String fieldName, String value) {
    return QueryBuilders.matchQuery(fieldName, value);
  }

  private static QueryBuilder startsWith(String fieldName, String value) {
    return QueryBuilders.prefixQuery(fieldName, value);
  }

  @Override
  public Object visitMethodCall(
      MethodKind method,
      List<Object> parameters) throws ExpressionVisitException, ODataApplicationException {

    switch (method) {
      case CONTAINS:
        return contains((String) parameters.get(0), (String) parameters.get(1));
      case STARTSWITH:
        return startsWith((String) parameters.get(0), (String) parameters.get(1));
      default:
        throw new NotImplementedException("Method " + method + " not implemented");
    }
  }

  @Override
  public Object visitLambdaExpression(
      String lambdaFunction,
      String lambdaVariable,
      Expression expression) throws ExpressionVisitException, ODataApplicationException {

    throw new NotImplementedException("visitLambdaExpression not implemented");
  }

  @Override
  public Object visitLiteral(Literal literal)
      throws ExpressionVisitException, ODataApplicationException {

    String value = literal.getText();
    if (literal.getType() instanceof EdmString) {
      value = LiteralUtils.unquote(value);
    }

    return value;
  }

  @Override
  public Object visitMember(Member member)
      throws ExpressionVisitException, ODataApplicationException {

    return MemberMapper.toFieldName(member);
  }

  @Override
  public Object visitAlias(String aliasName)
      throws ExpressionVisitException, ODataApplicationException {

    throw new NotImplementedException("visitAlias not implemented");
  }

  @Override
  public Object visitTypeLiteral(EdmType type)
      throws ExpressionVisitException, ODataApplicationException {

    throw new NotImplementedException("visitTypeLiteral not implemented");
  }

  @Override
  public Object visitLambdaReference(String variableName)
      throws ExpressionVisitException, ODataApplicationException {

    throw new NotImplementedException("visitLambdaReference not implemented");
  }

  @Override
  public Object visitEnum(
      EdmEnumType type,
      List<String> enumValues) throws ExpressionVisitException, ODataApplicationException {

    throw new NotImplementedException("visitEnum not implemented");
  }
}
