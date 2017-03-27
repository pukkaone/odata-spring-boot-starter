package com.github.pukkaone.odata.web.processor;

import lombok.Data;
import org.apache.olingo.commons.api.ex.ODataError;

/**
 * Response body when error occurred.
 */
@Data
public class ErrorDocument {

  private ODataError error;

  /**
   * Constructor.
   *
   * @param error
   *     error information
   */
  public ErrorDocument(ODataError error) {
    this.error = new ODataError()
        .setCode(error.getCode())
        .setMessage(error.getMessage())
        .setTarget(error.getTarget())
        .setDetails(error.getDetails())
        .setInnerError(error.getInnerError());
  }
}
