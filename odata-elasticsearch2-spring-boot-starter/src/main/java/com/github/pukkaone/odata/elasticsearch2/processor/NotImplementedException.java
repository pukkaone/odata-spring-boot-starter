package com.github.pukkaone.odata.elasticsearch2.processor;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;

/**
 * Thrown on attempt to execute unimplemented operation.
 */
public class NotImplementedException extends ODataApplicationException {

  /**
   * Constructor.
   *
   * @param message
   *     message
   */
  public NotImplementedException(String message) {
    super(message, HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), null);
  }
}
